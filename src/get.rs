use crate::{Channel, Delivery, Result};

/// A message delivered in response to a [`get`](struct.Queue.html#method.get) request.
#[derive(Clone, Debug)]
pub struct Get {
    /// The message.
    pub delivery: Delivery,

    /// The number of messages present in the queue at the time the get was serviced.
    pub message_count: u32,
}

impl Get {
    /// Calls [`Delivery::ack`](struct.Delivery.html#method.ack) on `self.delivery`.
    #[inline]
    pub fn ack(self, channel: &Channel, multiple: bool) -> Result<()> {
        self.delivery.ack(channel, multiple)
    }

    /// Calls [`Delivery::nack`](struct.Delivery.html#method.nack) on `self.delivery`.
    #[inline]
    pub fn nack(self, channel: &Channel, multiple: bool, requeue: bool) -> Result<()> {
        self.delivery.nack(channel, multiple, requeue)
    }

    /// Calls [`Delivery::reject`](struct.Delivery.html#method.reject) on `self.delivery`.
    #[inline]
    pub fn reject(self, channel: &Channel, requeue: bool) -> Result<()> {
        self.delivery.reject(channel, requeue)
    }
}
