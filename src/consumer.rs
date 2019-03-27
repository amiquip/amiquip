use crate::{Channel, Delivery, ErrorKind, Result};
use crossbeam_channel::Receiver;

pub struct Consumer<'a> {
    channel: &'a Channel,
    consumer_tag: String,
    rx: Receiver<ConsumerMessage>,
    cancelled: bool,
}

impl Drop for Consumer<'_> {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
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

impl Consumer<'_> {
    pub(crate) fn new(
        channel: &Channel,
        consumer_tag: String,
        rx: Receiver<ConsumerMessage>,
    ) -> Consumer {
        Consumer {
            channel,
            consumer_tag,
            rx,
            cancelled: false,
        }
    }

    #[inline]
    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    #[inline]
    pub fn receiver(&self) -> &Receiver<ConsumerMessage> {
        &self.rx
    }

    pub fn cancel(&mut self) -> Result<()> {
        if self.cancelled {
            return Ok(());
        }
        self.cancelled = true;
        self.channel.basic_cancel(&self)
    }

    #[inline]
    pub fn ack(&self, delivery: &Delivery, multiple: bool) -> Result<()> {
        self.channel.basic_ack(delivery, multiple)
    }
}
