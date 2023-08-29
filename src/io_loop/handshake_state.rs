use super::Inner;
use crate::connection_options::ConnectionOptions;
use crate::errors::*;
use crate::serialize::TryFromAmqpFrame;
use crate::{FieldTable, Sasl};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::{Close, CloseOk, OpenOk, Secure, Start, Tune, TuneOk};
use log::{debug, error};

#[derive(Debug)]
pub(super) enum HandshakeState<Auth: Sasl> {
    Start(ConnectionOptions<Auth>),
    Secure(ConnectionOptions<Auth>, FieldTable),
    Tune(ConnectionOptions<Auth>, FieldTable),
    Open(TuneOk, FieldTable),
    ServerClosing(Close),
    Done(TuneOk, FieldTable),
}

impl<Auth: Sasl> HandshakeState<Auth> {
    pub(super) fn process(&mut self, inner: &mut Inner, frame: AMQPFrame) -> Result<()> {
        // unlikely but not impossible to receive a heartbeat during handshake
        if let AMQPFrame::Heartbeat(0) = frame {
            debug!("received heartbeat");
            return Ok(());
        }

        match self {
            HandshakeState::Start(options) => {
                let start = Start::try_from_frame(0, frame)?;
                debug!("received handshake {:?}", start);

                let (start_ok, server_properties) = options.make_start_ok(start)?;
                debug!("sending handshake {:?}", start_ok);
                inner.push_method(0, AmqpConnection::StartOk(start_ok));

                *self = HandshakeState::Secure(options.clone(), server_properties);
            }
            HandshakeState::Secure(options, server_properties) => {
                // We currently only support PLAIN and EXTERNAL, neither of which
                // need a secure/secure-ok
                if let Ok(secure) = Secure::try_from_frame(0, frame.clone()) {
                    error!("received unsupported handshake {:?}", secure);
                    return SaslSecureNotSupportedSnafu.fail();
                }
                *self = HandshakeState::Tune(options.clone(), server_properties.clone());
                return self.process(inner, frame);
            }
            HandshakeState::Tune(options, server_properties) => {
                let tune = Tune::try_from_frame(0, frame)?;
                debug!("received handshake {:?}", tune);

                let tune_ok = options.make_tune_ok(tune)?;
                inner.start_heartbeats(tune_ok.heartbeat);

                debug!("sending handshake {:?}", tune_ok);
                inner.push_method(0, AmqpConnection::TuneOk(tune_ok.clone()));

                let open = options.make_open();
                debug!("sending handshake {:?}", open);
                inner.push_method(0, AmqpConnection::Open(open));

                *self = HandshakeState::Open(tune_ok, server_properties.clone());
            }
            HandshakeState::Open(tune_ok, server_properties) => {
                // If we sent bad tune params, server might send us a Close.
                if let Ok(close) = Close::try_from_frame(0, frame.clone()) {
                    inner.push_method(0, AmqpConnection::CloseOk(CloseOk {}));
                    inner.seal_writes();
                    *self = HandshakeState::ServerClosing(close);
                    return Ok(());
                }

                let open_ok = OpenOk::try_from_frame(0, frame)?;
                debug!("received handshake {:?}", open_ok);

                *self = HandshakeState::Done(tune_ok.clone(), server_properties.clone());
            }
            HandshakeState::ServerClosing(_) | HandshakeState::Done(_, _) => {
                return FrameUnexpectedSnafu.fail();
            }
        }
        Ok(())
    }
}
