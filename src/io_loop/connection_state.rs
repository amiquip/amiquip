use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::CloseOk as ChannelCloseOk;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use amq_protocol::protocol::{AMQPClass, AMQPHardError};
use crossbeam_channel::Sender;
use failure::ResultExt;
use log::{error, trace, warn};
use std::collections::hash_map::Entry;

use super::{ChannelMessage, ChannelSlot, Inner};

#[derive(Debug)]
pub(super) enum ConnectionState {
    Steady,
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
        match self {
            ConnectionState::Steady => (),
            ConnectionState::ServerClosing(_)
            | ConnectionState::ClientClosed
            | ConnectionState::ClientException => Err(ErrorKind::FrameUnexpected)?,
        }

        Ok(match frame {
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::Close(close))) => {
                inner.push_method(0, AmqpConnection::CloseOk(ConnectionCloseOk {}))?;
                inner.seal_writes();
                *self = ConnectionState::ServerClosing(close);
            }
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::CloseOk(close_ok))) => {
                inner
                    .chan_slots
                    .get(0)
                    .unwrap() // channel 0 slot always exist if we got to Steady state
                    .tx
                    .send(ChannelMessage::Method(AMQPClass::Connection(
                        AmqpConnection::CloseOk(close_ok),
                    )))
                    .context(ErrorKind::EventLoopClientDropped)?;
                *self = ConnectionState::ClientClosed;
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
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::Close(close))) => {
                let slot = slot_remove(inner, n)?;
                warn!("server closing channel {}: {:?}", n, close);
                inner.push_method(n, AmqpChannel::CloseOk(ChannelCloseOk {}))?;
                send(
                    &slot.tx,
                    ChannelMessage::ServerClosing(ErrorKind::ServerClosedChannel(
                        n,
                        close.reply_code,
                        close.reply_text,
                    )),
                )?;
            }
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::CloseOk(close_ok))) => {
                let slot = slot_remove(inner, n)?;
                send(
                    &slot.tx,
                    ChannelMessage::Method(AMQPClass::Channel(AmqpChannel::CloseOk(close_ok))),
                )?;
            }
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::ConsumeOk(consume_ok))) => {
                let consumer_tag = consume_ok.consumer_tag;
                let slot = slot_get_mut(inner, n)?;
                match slot.consumers.entry(consumer_tag.clone()) {
                    Entry::Occupied(_) => Err(ErrorKind::DuplicateConsumerTag(n, consumer_tag))?,
                    Entry::Vacant(entry) => {
                        let (tx, rx) = crossbeam_channel::unbounded();
                        entry.insert(tx);
                        send(&slot.tx, ChannelMessage::ConsumeOk(consumer_tag, rx))?;
                    }
                }
            }
            AMQPFrame::Method(n, AMQPClass::Basic(AmqpBasic::Deliver(deliver))) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_deliver(deliver)?;
            }
            AMQPFrame::Method(n, method) => {
                let slot = slot_get(inner, n)?;
                trace!(
                    "trying to send method to client for channel {}: {:?}",
                    n,
                    method
                );
                send(&slot.tx, ChannelMessage::Method(method))?;
            }
            AMQPFrame::Header(n, _, header) => {
                let slot = slot_get_mut(inner, n)?;
                slot.collector.collect_header(*header)?;
            }
            AMQPFrame::Body(n, body) => {
                let slot = slot_get_mut(inner, n)?;
                if let Some((consumer_tag, delivery)) = slot.collector.collect_body(body)? {
                    let tx = slot
                        .consumers
                        .get(&consumer_tag)
                        .ok_or(ErrorKind::UnknownConsumerTag(n, consumer_tag))?;
                    send(tx, delivery)?;
                }
            }
            _ => panic!("TODO"),
        })
    }
}
