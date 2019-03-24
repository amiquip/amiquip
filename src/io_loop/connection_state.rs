use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::CloseOk as ChannelCloseOk;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use amq_protocol::protocol::{AMQPClass, AMQPHardError};
use failure::ResultExt;
use log::{error, trace, warn};

use super::{ChannelMessage, Inner};

#[derive(Debug)]
pub(super) enum ConnectionState {
    Steady,
    ServerClosing(ConnectionClose),
    ClientException,
    ClientClosed,
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
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::Close(close))) => {
                let slot = inner
                    .chan_slots
                    .remove(n)
                    .ok_or(ErrorKind::ReceivedFrameWithBogusChannelId(n))?;
                warn!("server closing channel {}: {:?}", n, close);
                inner.push_method(n, AmqpChannel::CloseOk(ChannelCloseOk {}))?;
                slot.tx
                    .send(ChannelMessage::ServerClosing(
                        ErrorKind::ServerClosedChannel(n, close.reply_code, close.reply_text),
                    ))
                    .context(ErrorKind::EventLoopClientDropped)?;
            }
            AMQPFrame::Method(n, AMQPClass::Channel(AmqpChannel::CloseOk(close_ok))) => {
                let slot = inner
                    .chan_slots
                    .remove(n)
                    .ok_or(ErrorKind::ReceivedFrameWithBogusChannelId(n))?;
                slot.tx
                    .send(ChannelMessage::Method(AMQPClass::Channel(
                        AmqpChannel::CloseOk(close_ok),
                    )))
                    .context(ErrorKind::EventLoopClientDropped)?;
            }
            AMQPFrame::Method(n, method) => {
                let slot = inner
                    .chan_slots
                    .get(n)
                    .ok_or(ErrorKind::ReceivedFrameWithBogusChannelId(n))?;
                trace!(
                    "trying to send method to client for channel {}: {:?}",
                    n,
                    method
                );
                slot.tx
                    .send(ChannelMessage::Method(method))
                    .context(ErrorKind::EventLoopClientDropped)?;
            }
            _ => panic!("TODO"),
        })
    }
}

/*
use super::Inner;
use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::serialize::TryFromAmqpFrame;
use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use crossbeam_channel::Sender;
use log::debug;

#[derive(Debug)]
pub(super) enum ConnectionState {
    //Handshake(Sender<IoLoopHandle>, HandshakeState<Auth>),
    Steady,
    ServerClosing(ConnectionClose),
    //ClientClosing(ConnectionClose),
}
*/

/*
impl<Auth: Sasl> ConnectionState<Auth> {
    fn process(self, inner: &mut Inner, frame: AMQPFrame) -> Result<Self> {
        Ok(match self {
            ConnectionState::Handshake(sender, state) => match state.process(inner, frame)? {
                HandshakeState::Done(_tune_ok) => {
                    // TODO send handle on sender
                    ConnectionState::Steady
                }
                HandshakeState::ServerClosing(close) => ConnectionState::ServerClosing(close),
                other => ConnectionState::Handshake(sender, other),
            },
            ConnectionState::ServerClosing(close) => {
                warn!("received unexpected frame after server close - {:?}", frame);
                ConnectionState::ServerClosing(close)
            }
            //ConnectionState::ClientClosing(_close) => unimplemented!(),
            ConnectionState::Steady => unimplemented!(),
        })
    }
}
*/
