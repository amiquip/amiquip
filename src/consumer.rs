use crate::errors::*;
use crate::{Channel, Delivery, FieldTable};
use crossbeam_channel::Receiver;
use std::cell::Cell;

/// Options passed to the server when starting a consumer.
///
/// The [`default`](#impl-Default) implementation sets all boolean fields to false and has an empty
/// set of arguments.
///
/// # Example
///
/// The [`arguments`](#structfield.arguments) field can be used to set a
/// [consumer priority](https://www.rabbitmq.com/consumer-priority.html):
///
/// ```rust
/// # use amiquip::{AmqpValue, ConsumerOptions, FieldTable};
/// let mut arguments = FieldTable::new();
/// arguments.insert("x-priority".to_string(), AmqpValue::ShortInt(10));
/// let options = ConsumerOptions {
///     arguments,
///     ..ConsumerOptions::default()
/// };
/// ```
#[derive(Clone, Debug, Default)]
pub struct ConsumerOptions {
    /// If true, the server will not send this consumer messages that were published by the
    /// consumer's connection.
    pub no_local: bool,

    /// If true, the server assumes all delivered messages are acknowledged, and the client should
    /// not acknowledge messages. If using this option, be aware of [unbounded memory
    /// growth](struct.Channel.html#unbounded-memory-usage) concerns.
    pub no_ack: bool,

    /// If true, requires that this consumer is the only one attached to the queue. If other
    /// consumers are active, the server will close the channel.
    pub exclusive: bool,

    /// Extra arguments; these are optional in general, but may be needed for some plugins or
    /// server-specific features.
    pub arguments: FieldTable,
}

/// Messages delivered to consumers.
// Clippy warns about ConsumerMessage::Delivery being much larger than the other variants, but we
// expect almost all instances of ConsumerMessage to be Deliveries.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
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
    ServerClosedChannel(Error),

    /// The client has closed the connection where this consumer was created.
    ClientClosedConnection,

    /// The server has closed the connection where this consumer was created.
    ServerClosedConnection(Error),
}

/// A message consumer associated with an AMQP queue.
///
/// # Example
///
/// ```rust
/// use amiquip::{Consumer, ConsumerMessage, Result};
/// # use amiquip::Delivery;
///
/// # fn handle_delivery(_: Delivery) {}
/// // Receive (at least) n messages on the consumer, then cancel it.
/// fn consume_n_messages(consumer: Consumer, n: usize) -> Result<()> {
///     for (i, message) in consumer.receiver().iter().enumerate() {
///         match message {
///             ConsumerMessage::Delivery(delivery) => handle_delivery(delivery),
///             ConsumerMessage::ServerClosedChannel(err)
///             | ConsumerMessage::ServerClosedConnection(err) => return Err(err)?,
///             ConsumerMessage::ClientCancelled
///             | ConsumerMessage::ServerCancelled
///             | ConsumerMessage::ClientClosedChannel
///             | ConsumerMessage::ClientClosedConnection => break,
///         }
///         if i >= n {
///             consumer.cancel()?;
///         }
///     }
///     Ok(())
/// }
/// ```
pub struct Consumer<'a> {
    channel: &'a Channel,
    consumer_tag: String,
    rx: Receiver<ConsumerMessage>,
    cancelled: Cell<bool>,
}

impl Drop for Consumer<'_> {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
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
            cancelled: Cell::new(false),
        }
    }

    /// The server-assigned consumer tag.
    #[inline]
    pub fn consumer_tag(&self) -> &str {
        &self.consumer_tag
    }

    /// The `crossbeam_channel::Receiver` on which messages will be delivered. Once a consumer
    /// message of any variant other than
    /// [`ConsumerMessage`](enum.ConsumerMessage.html#variant.Delivery) has been received, no more
    /// messages will be sent and the sending side of the channel (held by the connection's I/O
    /// thread) will be dropped.
    ///
    /// # Note on Cloning
    ///
    /// Crossbeam channels implement `Clone`. Be careful cloning this receiver, as the sending side
    /// (held by the connection's I/O thread) will be dropped when `self` is cancelled, which will
    /// happen when [`cancel`](#method.cancel) is called or `self` is dropped.
    #[inline]
    pub fn receiver(&self) -> &Receiver<ConsumerMessage> {
        &self.rx
    }

    /// Cancel this consumer.
    ///
    /// When the cancellation is acknowledged by the server, the channel returned by
    /// [`receiver`](#method.receiver) will receive a
    /// [`ConsumerMessage::ClientCancelled`](enum.ConsumerMessage.html#variant.ClientCancelled)
    /// message. This method does not consume `self` because this method is inherently racy; the
    /// server may be sending us additional messages as we are attempting to cancel.
    ///
    /// Calling this method a second or later time will always return `Ok`; if you care about
    /// cancellation errors, you must capture the `Err` value on the first call.
    pub fn cancel(&self) -> Result<()> {
        if self.cancelled.get() {
            return Ok(());
        }
        self.cancelled.set(true);
        self.channel.basic_cancel(&self)
    }

    /// Calls [`Delivery::ack`](struct.Delivery.html#method.ack) on `delivery` using the channel
    /// that contains this consumer. See the note on that method about taking care not to ack
    /// deliveries across channels.
    #[inline]
    pub fn ack(&self, delivery: Delivery) -> Result<()> {
        delivery.ack(self.channel)
    }

    /// Calls [`Delivery::ack_multiple`](struct.Delivery.html#method.ack_multiple) on `delivery`
    /// using the channel that contains this consumer. See the note on that method about taking
    /// care not to ack deliveries across channels.
    #[inline]
    pub fn ack_multiple(&self, delivery: Delivery) -> Result<()> {
        delivery.ack_multiple(self.channel)
    }

    /// Calls [`Delivery::nack`](struct.Delivery.html#method.nack) on `delivery` using the channel
    /// that contains this consumer. See the note on that method about taking care not to nack
    /// deliveries across channels.
    #[inline]
    pub fn nack(&self, delivery: Delivery, requeue: bool) -> Result<()> {
        delivery.nack(self.channel, requeue)
    }

    /// Calls [`Delivery::nack_multiple`](struct.Delivery.html#method.nack_multiple) on `delivery`
    /// using the channel that contains this consumer. See the note on that method about taking
    /// care not to nack deliveries across channels.
    #[inline]
    pub fn nack_multiple(&self, delivery: Delivery, requeue: bool) -> Result<()> {
        delivery.nack_multiple(self.channel, requeue)
    }

    /// Calls [`Delivery::reject`](struct.Delivery.html#method.reject) on `delivery` using the
    /// channel that contains this consumer. See the note on that method about taking care not to
    /// reject deliveries across channels.
    #[inline]
    pub fn reject(&self, delivery: Delivery, requeue: bool) -> Result<()> {
        self.channel.basic_reject(delivery, requeue)
    }
}
