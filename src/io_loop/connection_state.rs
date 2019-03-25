use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::CancelOk;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::CloseOk as ChannelCloseOk;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use amq_protocol::protocol::{AMQPClass, AMQPHardError};
use crossbeam_channel::Sender;
use failure::ResultExt;
use log::{debug, error, trace, warn};
use std::collections::hash_map::Entry;

use super::{
    Channel0Slot, ChannelMessage, ChannelSlot, ConnectionBlockedNotification, ConsumerMessage,
    Inner,
};

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
        .ok_or(ErrorKind::ReceivedFrameWithBogusChannelId(channel_id))?)
}

fn slot_get(inner: &mut Inner, channel_id: u16) -> Result<&ChannelSlot> {
    Ok(inner
        .chan_slots
        .get(channel_id)
        .ok_or(ErrorKind::ReceivedFrameWithBogusChannelId(channel_id))?)
}

fn slot_get_mut(inner: &mut Inner, channel_id: u16) -> Result<&mut ChannelSlot> {
    Ok(inner
        .chan_slots
        .get_mut(channel_id)
        .ok_or(ErrorKind::ReceivedFrameWithBogusChannelId(channel_id))?)
}

fn send<T: Send + Sync + 'static>(tx: &Sender<T>, item: T) -> Result<()> {
    // TODO Is it okay to error out here (and eventually bubble up and kill the i/o thread)
    // if a client is gone? If they all drop properly this shouldn't happen, but it seems
    // dicey...
    Ok(tx.send(item).context(ErrorKind::EventLoopClientDropped)?)
}

impl ConnectionState {
    pub(super) fn process(&mut self, inner: &mut Inner, frame: AMQPFrame) -> Result<()> {
        // bail out if we shouldn't be getting frames
        let ch0_slot = match self {
            ConnectionState::Steady(ch0_slot) => ch0_slot,
            ConnectionState::ServerClosing(_)
            | ConnectionState::ClientClosed
            | ConnectionState::ClientException => Err(ErrorKind::FrameUnexpected)?,
        };

        Ok(match frame {
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
                inner.push_method(0, AmqpConnection::CloseOk(ConnectionCloseOk {}))?;
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
                ch0_slot.common.tx
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
                send(&ch0_slot.blocked_tx, ConnectionBlockedNotification::Blocked(blocked.reason))?;
            }
            // Server has unblocked publishes
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::Unblocked(_))) => {
                warn!("server has unblocked connection");
                send(&ch0_slot.blocked_tx, ConnectionBlockedNotification::Unblocked)?;
            }
            // TODO handle other expected channel 0 methods
            AMQPFrame::Method(0, other) => {
                let text = format!("do not know how to handle channel 0 method {:?}", other);
                error!("{} - closing connection", text);
                let close = ConnectionClose {
                    reply_code: AMQPHardError::NOTIMPLEMENTED.get_id(),
                    reply_text: text,
                    class_id: 0,
                    method_id: 0,
                };
                inner.push_method(0, AmqpConnection::Close(close))?;
                inner.seal_writes();
                *self = ConnectionState::ClientException;
            }
            // Reject content frames on channel 0.
            AMQPFrame::Header(0, _, _) | AMQPFrame::Body(0, _) => {
                let text = format!("received illegal channel 0 frame {:?}", frame);
                error!("{} - closing connection", text);
                let close = ConnectionClose {
                    reply_code: AMQPHardError::NOTALLOWED.get_id(),
                    reply_text: text,
                    class_id: 0,
                    method_id: 0,
                };
                inner.push_method(0, AmqpConnection::Close(close))?;
                inner.seal_writes();
                *self = ConnectionState::ClientException;
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
                inner.push_method(n, AmqpChannel::CloseOk(ChannelCloseOk {}))?;
            }
            // Server ack for client-initiated channel close.
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::CloseOk(close_ok))) => {
                let mut slot = slot_remove(inner, n)?;
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
                    send(&tx, ConsumerMessage::Cancelled)?;
                }
                if !cancel.nowait {
                    inner.push_method(
                        n,
                        AmqpBasic::CancelOk(CancelOk {
                            consumer_tag: consumer_tag,
                        }),
                    )?;
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
                    send(&tx, ConsumerMessage::Cancelled)?;
                }
            }
            // Server beginning delivery of content to a consumer.
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Deliver(deliver))) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_deliver(deliver)?;
            }
            // TODO break this out into other methods so we know which ones we expect
            AMQPFrame::Method(n, method) => {
                let slot = slot_get(inner, n)?;
                trace!(
                    "trying to send method to client for channel {}: {:?}",
                    n,
                    method
                );
                send(&slot.tx, Ok(ChannelMessage::Method(method)))?;
            }
            // Server sending content header as part of a deliver.
            AMQPFrame::Header(n, _, header) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_header(*header)?;
            }
            // Server sending content body as part of a deliver.
            AMQPFrame::Body(n, body) => {
                let slot = slot_get_mut(inner, n)?;
                if let Some((consumer_tag, delivery)) = slot.collector.collect_body(body)? {
                    let tx = slot
                        .consumers
                        .get(&consumer_tag)
                        .ok_or(ErrorKind::UnknownConsumerTag(n, consumer_tag))?;
                    send(tx, ConsumerMessage::Delivery(delivery))?;
                }
            }
            //_ => panic!("TODO"),
        })
    }
}
