use crate::{Confirm, ConfirmPayload, ErrorKind, Result, Return};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::CancelOk;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::CloseOk as ChannelCloseOk;
use amq_protocol::protocol::confirm::AMQPMethod as AmqpConfirm;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use amq_protocol::protocol::exchange::AMQPMethod as AmqpExchange;
use amq_protocol::protocol::queue::AMQPMethod as AmqpQueue;
use amq_protocol::protocol::{AMQPClass, AMQPHardError};
use crossbeam_channel::{Sender, TrySendError};
use failure::ResultExt;
use log::{debug, error, trace, warn};
use std::collections::hash_map::Entry;

use super::content_collector::CollectorResult;
use super::{
    Channel0Slot, ChannelMessage, ChannelSlot, ConnectionBlockedNotification, ConsumerMessage,
    Inner,
};

// Clippy warns about ConnectionState::Steady being much larger than the other variants, but we
// expect ConnectionState to be in the Steady case almost all the time.
#[allow(clippy::large_enum_variant)]
pub(super) enum ConnectionState {
    Steady(Channel0Slot),
    ServerClosing(ConnectionClose),
    ClientException,
    ClientClosed,
}

fn slot_remove(inner: &mut Inner, channel_id: u16) -> Result<ChannelSlot> {
    Ok(inner
        .chan_slots
        .remove(channel_id)
        .ok_or_else(|| ErrorKind::ReceivedFrameWithBogusChannelId(channel_id))?)
}

fn slot_get(inner: &mut Inner, channel_id: u16) -> Result<&ChannelSlot> {
    Ok(inner
        .chan_slots
        .get(channel_id)
        .ok_or_else(|| ErrorKind::ReceivedFrameWithBogusChannelId(channel_id))?)
}

fn slot_get_mut(inner: &mut Inner, channel_id: u16) -> Result<&mut ChannelSlot> {
    Ok(inner
        .chan_slots
        .get_mut(channel_id)
        .ok_or_else(|| ErrorKind::ReceivedFrameWithBogusChannelId(channel_id))?)
}

fn send<T: Send + Sync + 'static>(tx: &Sender<T>, item: T) -> Result<()> {
    // See comment in ChannelSlot::new() about the bound size of the control
    // channel. If we're sending to a consumer channel, they are not bounded
    // and will not return Full.
    match tx.try_send(item) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            error!("internal error - bounded channel is unexpectedly full");
            Err(ErrorKind::FrameUnexpected)?
        }
        Err(TrySendError::Disconnected(_)) => {
            error!("internal error - channel client dropped without being disconnected");
            Err(ErrorKind::EventLoopClientDropped)?
        }
    }
}

// When we set up a return listener, it's just a crossbeam channel. If it gets dropped,
// we don't want to error; just start discarding returned messages.
fn try_send_return(slot: &mut ChannelSlot, return_: Return) {
    let return_ = if let Some(tx) = &slot.return_handler {
        match tx.try_send(return_) {
            Ok(()) => return,
            Err(TrySendError::Full(return_)) | Err(TrySendError::Disconnected(return_)) => {
                slot.return_handler = None;
                return_
            }
        }
    } else {
        return_
    };
    warn!("discarding returned data {:?}", return_);
}

// When we set up a pub confirm listener, it's just a crossbeam channel. If it gets dropped,
// we don't want to error; just start discarding acks/nacks
fn try_send_confirm(slot: &mut ChannelSlot, confirm: Confirm) {
    let confirm = if let Some(tx) = &slot.pub_confirm_handler {
        match tx.try_send(confirm) {
            Ok(()) => return,
            Err(TrySendError::Full(confirm)) | Err(TrySendError::Disconnected(confirm)) => {
                slot.pub_confirm_handler = None;
                confirm
            }
        }
    } else {
        confirm
    };
    warn!("discarding returned data {:?}", confirm);
}

// When we set up a blocked connection listener, it's just a crossbeam channel. If it gets
// dropped, we don't want to error; just start discarding blocked notifications.
fn try_send_blocked(slot: &mut Channel0Slot, note: ConnectionBlockedNotification) {
    if let Some(tx) = &slot.blocked_tx {
        match tx.try_send(note) {
            Ok(()) => (),
            Err(_) => {
                slot.blocked_tx = None;
            }
        }
    }
}

impl ConnectionState {
    fn client_exception(
        &mut self,
        inner: &mut Inner,
        reply_code: AMQPHardError,
        reply_text: String,
    ) -> Result<()> {
        error!("{} - closing connection", reply_text);
        let close = ConnectionClose {
            reply_code: reply_code.get_id(),
            reply_text,
            class_id: 0,
            method_id: 0,
        };
        inner.push_method(0, AmqpConnection::Close(close));
        inner.seal_writes();
        *self = ConnectionState::ClientException;
        Ok(())
    }

    pub(super) fn process(&mut self, inner: &mut Inner, frame: AMQPFrame) -> Result<()> {
        // bail out if we shouldn't be getting frames
        let ch0_slot = match self {
            ConnectionState::Steady(ch0_slot) => ch0_slot,
            ConnectionState::ClientException => return Ok(()),
            ConnectionState::ServerClosing(_) | ConnectionState::ClientClosed => {
                Err(ErrorKind::FrameUnexpected)?
            }
        };

        match frame {
            // Server-sent heartbeat
            AMQPFrame::Heartbeat(0) => {
                // nothing to do here; IoLoop already updated heartbeat timer when it
                // received data on the socket
                debug!("received heartbeat");
            }
            // We never expect to see a protocl header (we send it to begin the connection)
            // or a heartbeat on a non-0 channel.
            AMQPFrame::ProtocolHeader | AMQPFrame::Heartbeat(_) => Err(ErrorKind::FrameUnexpected)?,
            // Server-initiated connection close.
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::Close(close))) => {
                inner.push_method(0, AmqpConnection::CloseOk(ConnectionCloseOk {}));
                inner.seal_writes();
                let err =
                    ErrorKind::ServerClosedConnection(close.reply_code, close.reply_text.clone());
                *self = ConnectionState::ServerClosing(close);

                for (_, mut slot) in inner.chan_slots.drain() {
                    send(&slot.tx, Err(err.clone().into()))?;
                    for (_, tx) in slot.consumers.drain() {
                        send(&tx, ConsumerMessage::ServerClosedConnection(err.clone()))?;
                    }
                }
            }
            // Server ack for client-initiated connection close.
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::CloseOk(close_ok))) => {
                ch0_slot
                    .common
                    .tx
                    .send(Ok(ChannelMessage::Method(AMQPClass::Connection(
                        AmqpConnection::CloseOk(close_ok),
                    ))))
                    .context(ErrorKind::EventLoopClientDropped)?;
                *self = ConnectionState::ClientClosed;

                for (_, mut slot) in inner.chan_slots.drain() {
                    send(&slot.tx, Err(ErrorKind::ClientClosedConnection.into()))?;
                    for (_, tx) in slot.consumers.drain() {
                        send(&tx, ConsumerMessage::ClientClosedConnection)?;
                    }
                }
            }
            // Server is blocking publishes due to an alarm on its side (e.g., low mem)
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::Blocked(blocked))) => {
                warn!("server has blocked connection; reason = {}", blocked.reason);
                let note = ConnectionBlockedNotification::Blocked(blocked.reason);
                try_send_blocked(ch0_slot, note);
            }
            // Server has unblocked publishes
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::Unblocked(_))) => {
                warn!("server has unblocked connection");
                let note = ConnectionBlockedNotification::Unblocked;
                try_send_blocked(ch0_slot, note);
            }
            // Reject all other expected channel 0 methods
            AMQPFrame::Method(0, other) => {
                let text = format!("do not know how to handle channel 0 method {:?}", other);
                self.client_exception(inner, AMQPHardError::NOTIMPLEMENTED, text)?;
            }
            // Reject content frames on channel 0.
            AMQPFrame::Header(0, _, _) | AMQPFrame::Body(0, _) => {
                let text = format!("received illegal channel 0 frame {:?}", frame);
                self.client_exception(inner, AMQPHardError::NOTALLOWED, text)?;
            }
            // Server-initiated channel close.
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::Close(close))) => {
                warn!("server closing channel {}: {:?}", n, close);
                let mut slot = slot_remove(inner, n)?;
                let err = ErrorKind::ServerClosedChannel(n, close.reply_code, close.reply_text);
                send(&slot.tx, Err(err.clone().into()))?;
                for (_, tx) in slot.consumers.drain() {
                    send(&tx, ConsumerMessage::ServerClosedChannel(err.clone()))?;
                }
                inner.push_method(n, AmqpChannel::CloseOk(ChannelCloseOk {}));
            }
            // Server ack for client-initiated channel close.
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::CloseOk(close_ok))) => {
                // Closing is inherently racy; if we and the server both send a Close at
                // the same time, we might see the server Close and then get a CloseOk, but
                // we will have removed the slot when we got the close. It is therefore not
                // an error to get a CloseOk for a nonexistent slot, since the server is
                // confirming that a channel is gone (and we don't have it anymore anyway).
                if let Ok(mut slot) = slot_remove(inner, n) {
                    send(
                        &slot.tx,
                        Ok(ChannelMessage::Method(AMQPClass::Channel(
                            AmqpChannel::CloseOk(close_ok),
                        ))),
                    )?;
                    for (_, tx) in slot.consumers.drain() {
                        send(&tx, ConsumerMessage::ClientClosedChannel)?;
                    }
                }
            }
            // Server ack for consume request.
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::ConsumeOk(consume_ok))) => {
                let consumer_tag = consume_ok.consumer_tag;
                let slot = slot_get_mut(inner, n)?;
                match slot.consumers.entry(consumer_tag.clone()) {
                    Entry::Occupied(_) => Err(ErrorKind::DuplicateConsumerTag(n, consumer_tag))?,
                    Entry::Vacant(entry) => {
                        let (tx, rx) = crossbeam_channel::unbounded();
                        entry.insert(tx);
                        send(&slot.tx, Ok(ChannelMessage::ConsumeOk(consumer_tag, rx)))?;
                    }
                }
            }
            // Server-initiated consumer cancel.
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Cancel(cancel))) => {
                let consumer_tag = cancel.consumer_tag;
                let slot = slot_get_mut(inner, n)?;
                if let Some(tx) = slot.consumers.remove(&consumer_tag) {
                    send(&tx, ConsumerMessage::ServerCancelled)?;
                }
                if !cancel.nowait {
                    inner.push_method(n, AmqpBasic::CancelOk(CancelOk { consumer_tag }));
                }
            }
            // Server ack for client-initiated consumer cancel.
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::CancelOk(cancel_ok))) => {
                let slot = slot_get_mut(inner, n)?;
                let consumer = slot.consumers.remove(&cancel_ok.consumer_tag);
                send(
                    &slot.tx,
                    Ok(ChannelMessage::Method(AMQPClass::Basic(
                        AmqpBasic::CancelOk(cancel_ok),
                    ))),
                )?;
                if let Some(tx) = consumer {
                    send(&tx, ConsumerMessage::ClientCancelled)?;
                }
            }
            // Server beginning delivery of content to a consumer.
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Deliver(deliver))) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_deliver(deliver)?;
            }
            // Server beginning return of undeliverable content.
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Return(return_))) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_return(return_)?;
            }
            // Server ack for get (message incoming).
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::GetOk(get_ok))) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_get(get_ok)?;
            }
            // Server ack for get (no message).
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::GetEmpty(_))) => {
                let slot = slot_get(inner, n)?;
                send(&slot.tx, Ok(ChannelMessage::GetOk(Box::new(None))))?;
            }
            // Server ack for publish (publisher confirmation)
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Ack(ack))) => {
                let slot = slot_get_mut(inner, n)?;
                let confirm = ConfirmPayload {
                    delivery_tag: ack.delivery_tag,
                    multiple: ack.multiple,
                };
                try_send_confirm(slot, Confirm::Ack(confirm));
            }
            // Server nack for publish (publisher confirmation)
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Nack(nack))) => {
                let slot = slot_get_mut(inner, n)?;
                let confirm = ConfirmPayload {
                    delivery_tag: nack.delivery_tag,
                    multiple: nack.multiple,
                };
                try_send_confirm(slot, Confirm::Nack(confirm));
            }
            // Generic ack messages we send back to the caller.
            AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::QosOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::RecoverOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Channel(AmqpChannel::OpenOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Confirm(AmqpConfirm::SelectOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::DeclareOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::DeleteOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::BindOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::UnbindOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::DeclareOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::DeleteOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::BindOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::PurgeOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::UnbindOk(_))) => {
                let slot = slot_get(inner, n)?;
                trace!(
                    "trying to send method to client for channel {}: {:?}",
                    n,
                    method
                );
                send(&slot.tx, Ok(ChannelMessage::Method(method)))?;
            }
            // Methods we do not handle
            AMQPFrame::Method(n, method @ AMQPClass::Access(_))
            | AMQPFrame::Method(n, method @ AMQPClass::Channel(AmqpChannel::Flow(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Channel(AmqpChannel::FlowOk(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Tx(_)) => {
                let text = format!(
                    "do not know how to handle channel {} method {:?}",
                    n, method
                );
                self.client_exception(inner, AMQPHardError::NOTIMPLEMENTED, text)?;
            }
            // Methods that are illegal coming from the server
            AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::Qos(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::Consume(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::Get(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::Publish(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::Recover(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::RecoverAsync(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Basic(AmqpBasic::Reject(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Channel(AmqpChannel::Open(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Confirm(AmqpConfirm::Select(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Connection(_))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::Declare(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::Delete(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::Bind(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Exchange(AmqpExchange::Unbind(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::Declare(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::Delete(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::Bind(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::Purge(_)))
            | AMQPFrame::Method(n, method @ AMQPClass::Queue(AmqpQueue::Unbind(_))) => {
                let text = format!("illegal channel {} method {:?}", n, method);
                self.client_exception(inner, AMQPHardError::NOTALLOWED, text)?;
            }
            // Server sending content header as part of a deliver.
            AMQPFrame::Header(n, _, header) => {
                let slot = slot_get_mut(inner, n)?;
                if let Some(collected) = slot.collector.collect_header(*header)? {
                    match collected {
                        CollectorResult::Delivery((consumer_tag, delivery)) => {
                            let tx = slot
                                .consumers
                                .get(&consumer_tag)
                                .ok_or_else(|| ErrorKind::UnknownConsumerTag(n, consumer_tag))?;
                            send(tx, ConsumerMessage::Delivery(delivery))?;
                        }
                        CollectorResult::Return(return_) => {
                            try_send_return(slot, return_);
                        }
                        CollectorResult::Get(get) => {
                            send(&slot.tx, Ok(ChannelMessage::GetOk(Box::new(Some(get)))))?;
                        }
                    }
                }
            }
            // Server sending content body as part of a deliver.
            AMQPFrame::Body(n, body) => {
                let slot = slot_get_mut(inner, n)?;
                if let Some(collected) = slot.collector.collect_body(body)? {
                    match collected {
                        CollectorResult::Delivery((consumer_tag, delivery)) => {
                            let tx = slot
                                .consumers
                                .get(&consumer_tag)
                                .ok_or_else(|| ErrorKind::UnknownConsumerTag(n, consumer_tag))?;
                            send(tx, ConsumerMessage::Delivery(delivery))?;
                        }
                        CollectorResult::Return(return_) => {
                            try_send_return(slot, return_);
                        }
                        CollectorResult::Get(get) => {
                            send(&slot.tx, Ok(ChannelMessage::GetOk(Box::new(Some(get)))))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
