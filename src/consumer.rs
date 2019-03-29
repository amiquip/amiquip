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

/// Messages delivered to consumers.
#[derive(Clone, Debug)]
pub enum ConsumerMessage {
    /// A delivered message.
    Delivery(Delivery),

    /// The channel was cancelled by the client; e.g., by calling
    /// [`Consumer::cancel`](struct.Consumer.html#method.cancel).
    ClientCancelled,

    /// The channel has been cancelled by the server; e.g., because the queue the consumer is
    /// attached to was deleted.
    ServerCancelled,

    /// The client has closed the channel where this consumer was created.
    ClientClosedChannel,

    /// The server has closed the channel where this consumer was created.
    ServerClosedChannel(ErrorKind),

    /// The client has closed the connection where this consumer was created.
    ClientClosedConnection,

    /// The server has closed the connection where this consumer was created.
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

    #[inline]
    pub fn nack(&self, delivery: &Delivery, multiple: bool, requeue: bool) -> Result<()> {
        self.channel.basic_nack(delivery, multiple, requeue)
    }

    #[inline]
    pub fn reject(&self, delivery: &Delivery, requeue: bool) -> Result<()> {
        self.channel.basic_reject(delivery, requeue)
    }
}
