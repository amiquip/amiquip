use crate::{Delivery, ErrorKind};
use crossbeam_channel::Receiver;

pub struct Consumer {
    consumer_tag: String,
    rx: Receiver<ConsumerMessage>,
}

#[derive(Clone, Debug)]
pub enum ConsumerMessage {
    Delivery(Delivery),
    Cancelled,
    ClientClosedChannel,
    ServerClosedChannel(ErrorKind),
    ClientClosedConnection,
    ServerClosedConnection(ErrorKind),
}

impl Consumer {
    pub(crate) fn new(consumer_tag: String, rx: Receiver<ConsumerMessage>) -> Consumer {
        Consumer { consumer_tag, rx }
    }

    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    pub fn receiver(&self) -> &Receiver<ConsumerMessage> {
        &self.rx
    }
}
