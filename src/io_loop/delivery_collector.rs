use crate::{Delivery, ErrorKind, Result};
use amq_protocol::frame::AMQPContentHeader;
use amq_protocol::protocol::basic::Deliver;

pub(super) struct DeliveryCollector {
    state: Option<State>,
}

impl DeliveryCollector {
    pub(super) fn new() -> DeliveryCollector {
        DeliveryCollector { state: None }
    }

    pub(super) fn collect_deliver(&mut self, deliver: Deliver) -> Result<()> {
        match self.state.take() {
            None => {
                self.state = Some(State::Deliver(deliver));
                Ok(())
            }
            Some(_) => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    pub(super) fn collect_header(
        &mut self,
        header: AMQPContentHeader,
    ) -> Result<Option<(String, Delivery)>> {
        match self.state.take() {
            Some(State::Deliver(deliver)) => {
                if header.body_size == 0 {
                    self.state = None;
                    Ok(Some(Delivery::new(deliver, Vec::new(), header.properties)))
                } else {
                    let buf = Vec::with_capacity(header.body_size as usize);
                    self.state = Some(State::Body(deliver, header, buf));
                    Ok(None)
                }
            }
            None | Some(State::Body(_, _, _)) => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    pub(super) fn collect_body(&mut self, mut body: Vec<u8>) -> Result<Option<(String, Delivery)>> {
        match self.state.take() {
            Some(State::Body(deliver, header, mut buf)) => {
                let body_size = header.body_size as usize;
                buf.append(&mut body);
                if buf.len() == body_size {
                    self.state = None;
                    Ok(Some(Delivery::new(deliver, buf, header.properties)))
                } else if buf.len() < body_size {
                    self.state = Some(State::Body(deliver, header, buf));
                    Ok(None)
                } else {
                    Err(ErrorKind::FrameUnexpected)?
                }
            }
            None | Some(State::Deliver(_)) => Err(ErrorKind::FrameUnexpected)?,
        }
    }
}

enum State {
    Deliver(Deliver),
    Body(Deliver, AMQPContentHeader, Vec<u8>),
}
