use crate::{Channel, Consumer, ConsumerOptions, Exchange, FieldTable, Get, Result};
use amq_protocol::protocol::queue::{Declare, Delete};

/// Options passed to the server when declaring a queue.
///
/// The [`default`](#impl-Default) implementation sets all boolean fields to false and has an empty
/// set of arguments.
///
/// # Example
///
/// The [`arguments`](#structfield.arguments) field can be used to declare a
/// [quorum queue](https://www.rabbitmq.com/quorum-queues.html):
///
/// ```rust
/// # use amiquip::{AmqpValue, QueueDeclareOptions, FieldTable};
/// let mut arguments = FieldTable::default();
/// arguments.insert(
///     "x-queue-type".into(),
///     AmqpValue::LongString("quorum".into()),
/// );
/// let options = QueueDeclareOptions {
///     arguments,
///     ..QueueDeclareOptions::default()
/// };
/// ```
#[derive(Clone, Debug, Default)]
pub struct QueueDeclareOptions {
    /// If true, declares queue as durable (survives server restarts); if false, declares queue as
    /// transient (will be deleted on a server restart).
    pub durable: bool,

    /// If true, declares queue as exclusive: the queue may only be accessed by the current
    /// connection, and it will be deleted when the current connection is closed.
    pub exclusive: bool,

    /// If true, declares queue as auto-delete: the server will delete it once the last consumer is
    /// disconnected (either by cancellation or by its channel being closed).
    ///
    /// NOTE: If a queue is declared as auto-delete but never has a consumer, it will not be
    /// deleted.
    pub auto_delete: bool,

    /// Extra arguments; these are optional in general, but may be needed for some plugins or
    /// server-specific features.
    pub arguments: FieldTable,
}

impl QueueDeclareOptions {
    pub(crate) fn into_declare(self, queue: String, passive: bool, nowait: bool) -> Declare {
        Declare {
            queue: queue.into(),
            passive,
            durable: self.durable,
            exclusive: self.exclusive,
            auto_delete: self.auto_delete,
            nowait,
            arguments: self.arguments,
        }
    }
}

/// Options passed to the server when deleting a queue.
///
/// The [`default`](#impl-Default) implementation sets all boolean fields to false.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueueDeleteOptions {
    /// If true, the server will only delete the queue if it has no consumers. If true and the
    /// queue _does_ have consumers, the server will close the current channel with an error.
    pub if_unused: bool,

    /// If true, the server will only delete the queue if it has no messages.
    pub if_empty: bool,
}

impl QueueDeleteOptions {
    pub(crate) fn into_delete(self, queue: String, nowait: bool) -> Delete {
        Delete {
            queue: queue.into(),
            if_unused: self.if_unused,
            if_empty: self.if_empty,
            nowait,
        }
    }
}

/// Handle for a declared AMQP queue.
pub struct Queue<'a> {
    channel: &'a Channel,
    name: String,
    message_count: Option<u32>,
    consumer_count: Option<u32>,
}

impl<'a> Queue<'a> {
    pub(crate) fn new(
        channel: &Channel,
        name: String,
        message_count: Option<u32>,
        consumer_count: Option<u32>,
    ) -> Queue {
        Queue {
            channel,
            name,
            message_count,
            consumer_count,
        }
    }

    /// Name of the declared queue. Normally this is the name specified when you declare the queue;
    /// however, if you declare a queue with an empty name, the server will assign an autogenerated
    /// queue name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Count of messages in the queue at the time when the queue was declared.
    ///
    /// This will be `Some(count)` if the queue was declared via
    /// [`Channel::queue_declare`](struct.Channel.html#method.queue_declare) or
    /// [`Channel::queue_declare_passive`](struct.Channel.html#method.queue_declare_passive), and
    /// will be `None` if the queue was declared via
    /// [`Channel::queue_declare_nowait`](struct.Channel.html#method.queue_declare_nowait).
    #[inline]
    pub fn declared_message_count(&self) -> Option<u32> {
        self.message_count
    }

    /// Count of consumers on the queue queue at the time when the queue was declared.
    ///
    /// This will be `Some(count)` if the queue was declared via
    /// [`Channel::queue_declare`](struct.Channel.html#method.queue_declare) or
    /// [`Channel::queue_declare_passive`](struct.Channel.html#method.queue_declare_passive), and
    /// will be `None` if the queue was declared via
    /// [`Channel::queue_declare_nowait`](struct.Channel.html#method.queue_declare_nowait).
    #[inline]
    pub fn declared_consumer_count(&self) -> Option<u32> {
        self.consumer_count
    }

    /// Synchronously get a single message from the queue.
    ///
    /// On success, returns `Some(message)` if there was a message in the queue or `None` if there
    /// were no messages in the queue. If `no_ack` is false, you are responsible for acknowledging
    /// the returned message, typically via [`Get::ack`](struct.Get.html#method.ack).
    ///
    /// Prefer using [`consume`](#method.consume) to allow the server to push messages to you on
    /// demand instead of polling with `get`.
    #[inline]
    pub fn get(&self, no_ack: bool) -> Result<Option<Get>> {
        self.channel.basic_get(self.name.clone(), no_ack)
    }

    /// Synchronously start a consumer on this queue.
    #[inline]
    pub fn consume(&self, options: ConsumerOptions) -> Result<Consumer<'a>> {
        self.channel.basic_consume(self.name.clone(), options)
    }

    /// Synchronously bind this queue to an exchange with the given routing key. `arguments` are
    /// typically optional, and are plugin / server dependent.
    #[inline]
    pub fn bind<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_bind(self.name(), exchange.name(), routing_key, arguments)
    }

    /// Asynchronously bind this queue to an exchange with the given routing key. `arguments` are
    /// typically optional, and are plugin / server dependent.
    #[inline]
    pub fn bind_nowait<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_bind_nowait(self.name(), exchange.name(), routing_key, arguments)
    }

    /// Synchronously unbind this queue from an exchange with the given routing key. `arguments`
    /// are typically optional, and are plugin / server dependent.
    #[inline]
    pub fn unbind<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_unbind(self.name(), exchange.name(), routing_key, arguments)
    }

    /// Synchronously purge all messages from this queue. On success, returns the number of
    /// messages that were purged.
    #[inline]
    pub fn purge(&self) -> Result<u32> {
        self.channel.queue_purge(self.name())
    }

    /// Asynchronously purge all messages from this queue.
    #[inline]
    pub fn purge_nowait(&self) -> Result<()> {
        self.channel.queue_purge_nowait(self.name())
    }

    /// Synchronously delete this queue. On success, returns the number of messages that were in
    /// the queue when it was deleted.
    #[inline]
    pub fn delete(self, options: QueueDeleteOptions) -> Result<u32> {
        self.channel.queue_delete(self.name(), options)
    }

    /// Asynchronously delete this queue.
    #[inline]
    pub fn delete_nowait(self, options: QueueDeleteOptions) -> Result<()> {
        self.channel.queue_delete_nowait(self.name(), options)
    }
}
