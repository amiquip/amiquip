use crate::Delivery;
use crossbeam_channel::Receiver;

pub struct Consumer {
    consumer_tag: String,
    rx: Receiver<Delivery>,
}

impl Consumer {
    pub(crate) fn new(consumer_tag: String, rx: Receiver<Delivery>) -> Consumer {
        Consumer { consumer_tag, rx }
    }

    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    pub fn receiver(&self) -> &Receiver<Delivery> {
        &self.rx
    }
}
