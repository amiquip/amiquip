use super::Inner;
use crate::auth::Sasl;
use crate::serialize::TryFromAmqpFrame;
use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::OpenOk as ConnectionOpenOk;
use amq_protocol::protocol::connection::{Secure, Start, Tune, TuneOk};
use crossbeam_channel::Sender;
use log::{warn, debug};

pub(crate) struct IoLoopHandle {}

#[derive(Debug)]
pub(super) enum HandshakeState {
    Start,
    Secure,
    Tune,
    Open(TuneOk),
    ServerClosing(ConnectionClose),
    Done(TuneOk),
}

impl HandshakeState {
    fn process<Auth: Sasl>(
        self,
        inner: &mut Inner<Auth>,
        frame: AMQPFrame,
    ) -> Result<HandshakeState> {
        Ok(match self {
            HandshakeState::Start => {
                let start = Start::try_from(0, frame)?;
                debug!("received handshake {:?}", start);

                let start_ok = inner.options.make_start_ok(start)?;
                debug!("sending handshake {:?}", start_ok);
                //inner.push_method(0, AmqpConnection::StartOk(start_ok))?;

                HandshakeState::Secure
            }
            HandshakeState::Secure => {
                // We currently only support PLAIN and EXTERNAL, neither of which
                // need a secure/secure-ok
                if let Ok(secure) = Secure::try_from(0, frame.clone()) {
                    debug!("received unsupported handshake {:?}", secure);
                    return Err(ErrorKind::SaslSecureNotSupported)?;
                }
                return HandshakeState::Tune.process(inner, frame);
            }
            HandshakeState::Tune => {
                let tune = Tune::try_from(0, frame)?;
                debug!("received handshake {:?}", tune);

                let tune_ok = inner.options.make_tune_ok(tune)?;
                //inner.start_heartbeats(tune_ok.heartbeat);

                debug!("sending handshake {:?}", tune_ok);
                //inner.push_method(0, AmqpConnection::TuneOk(tune_ok.clone()))?;

                let open = inner.options.make_open();
                debug!("sending handshake {:?}", open);
                //inner.push_method(0, AmqpConnection::Open(open))?;

                HandshakeState::Open(tune_ok)
            }
            HandshakeState::Open(tune_ok) => {
                // If we sent bad tune params, server might send us a Close.
                if let Ok(close) = ConnectionClose::try_from(0, frame.clone()) {
                    //inner.set_server_close_req(close);
                    return Ok(HandshakeState::ServerClosing(close));
                }

                let open_ok = ConnectionOpenOk::try_from(0, frame)?;
                debug!("received handshake {:?}", open_ok);

                HandshakeState::Done(tune_ok)
            }
            HandshakeState::ServerClosing(_) | HandshakeState::Done(_) => {
                unreachable!("handshake is done - HandshakeState::process() should not be called");
            }
        })
    }
}

#[derive(Debug)]
pub(super) enum ConnectionState {
    Handshake(Sender<IoLoopHandle>, HandshakeState),
    Steady,
    ServerClosing(ConnectionClose),
    ClientClosing(ConnectionClose),
}

impl ConnectionState {
    fn process<Auth: Sasl>(
        self,
        inner: &mut Inner<Auth>,
        frame: AMQPFrame,
    ) -> Result<ConnectionState> {
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
            ConnectionState::ClientClosing(_close) => unimplemented!(),
            ConnectionState::Steady => unimplemented!(),
        })
    }
}
