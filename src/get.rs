use crate::{Channel, Delivery, Result};

#[derive(Clone, Debug)]
pub struct Get {
    pub delivery: Delivery,
    pub message_count: u32,
}

impl Get {
    #[inline]
    pub fn ack(self, channel: &Channel, multiple: bool) -> Result<()> {
        self.delivery.ack(channel, multiple)
    }

    #[inline]
    pub fn nack(self, channel: &Channel, multiple: bool, requeue: bool) -> Result<()> {
        self.delivery.nack(channel, multiple, requeue)
    }

    #[inline]
    pub fn reject(self, channel: &Channel, requeue: bool) -> Result<()> {
        self.delivery.reject(channel, requeue)
    }
}
