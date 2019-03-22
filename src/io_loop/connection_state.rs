use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use amq_protocol::protocol::{AMQPClass, AMQPHardError};
use failure::ResultExt;
use log::error;

use super::Inner;

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
            AMQPFrame::Method(0, AMQPClass::Connection(AmqpConnection::CloseOk(_))) => {
                inner
                    .chan_slots
                    .get(0)
                    .unwrap() // channel 0 slot always exist if we got to Steady state
                    .tx
                    .send(AMQPFrame::Method(
                        0,
                        AMQPClass::Connection(AmqpConnection::CloseOk(ConnectionCloseOk {})),
                    ))
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
