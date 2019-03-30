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
    pub fn ack(self, channel: &Channel) -> Result<()> {
        self.delivery.ack(channel)
    }

    /// Calls [`Delivery::ack_multiple`](struct.Delivery.html#method.ack_multiple) on
    /// `self.delivery`.
    #[inline]
    pub fn ack_multiple(self, channel: &Channel) -> Result<()> {
        self.delivery.ack_multiple(channel)
    }

    /// Calls [`Delivery::nack`](struct.Delivery.html#method.nack) on `self.delivery`.
    #[inline]
    pub fn nack(self, channel: &Channel, requeue: bool) -> Result<()> {
        self.delivery.nack(channel, requeue)
    }

    /// Calls [`Delivery::nack_multiple`](struct.Delivery.html#method.nack_multiple) on
    /// `self.delivery`.
    #[inline]
    pub fn nack_multiple(self, channel: &Channel, requeue: bool) -> Result<()> {
        self.delivery.nack_multiple(channel, requeue)
    }

    /// Calls [`Delivery::reject`](struct.Delivery.html#method.reject) on `self.delivery`.
    #[inline]
    pub fn reject(self, channel: &Channel, requeue: bool) -> Result<()> {
        self.delivery.reject(channel, requeue)
    }
}
