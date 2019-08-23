use crate::io_loop::ChannelHandle;
use crate::serialize::{IntoAmqpClass, TryFromAmqpClass};
use crate::{
    Confirm, Consumer, ConsumerOptions, Delivery, Exchange, ExchangeDeclareOptions, ExchangeType,
    Get, Publish, Queue, QueueDeclareOptions, QueueDeleteOptions, Result, Return,
};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::Get as AmqpGet;
use amq_protocol::protocol::basic::Publish as AmqpPublish;
use amq_protocol::protocol::basic::{
    Ack, Cancel, CancelOk, Consume, Nack, Qos, QosOk, Recover, RecoverOk, Reject,
};
use amq_protocol::protocol::confirm::AMQPMethod as AmqpConfirm;
use amq_protocol::protocol::confirm::Select as ConfirmSelect;
use amq_protocol::protocol::confirm::SelectOk as ConfirmSelectOk;
use amq_protocol::protocol::exchange::AMQPMethod as AmqpExchange;
use amq_protocol::protocol::exchange::Bind as ExchangeBind;
use amq_protocol::protocol::exchange::BindOk as ExchangeBindOk;
use amq_protocol::protocol::exchange::DeclareOk as ExchangeDeclareOk;
use amq_protocol::protocol::exchange::Delete as ExchangeDelete;
use amq_protocol::protocol::exchange::DeleteOk as ExchangeDeleteOk;
use amq_protocol::protocol::exchange::Unbind as ExchangeUnbind;
use amq_protocol::protocol::exchange::UnbindOk as ExchangeUnbindOk;
use amq_protocol::protocol::queue::AMQPMethod as AmqpQueue;
use amq_protocol::protocol::queue::Bind as QueueBind;
use amq_protocol::protocol::queue::BindOk as QueueBindOk;
use amq_protocol::protocol::queue::DeclareOk as QueueDeclareOk;
use amq_protocol::protocol::queue::DeleteOk as QueueDeleteOk;
use amq_protocol::protocol::queue::Purge as QueuePurge;
use amq_protocol::protocol::queue::PurgeOk as QueuePurgeOk;
use amq_protocol::protocol::queue::Unbind as QueueUnbind;
use amq_protocol::protocol::queue::UnbindOk as QueueUnbindOk;
use amq_protocol::types::FieldTable;
use crossbeam_channel::Receiver;
use std::cell::RefCell;
use std::fmt::Debug;

/// Handle for an AMQP channel.
///
/// # Interaction with I/O Thread
///
/// A `Channel` is a wrapper around in-memory channels that communicate with its
/// [connection](struct.Connection.html)'s I/O thread. Messages to the I/O thread go through
/// bounded channels; see the discussion on [connection tuning](struct.Connection.html#tuning) for
/// more details.
///
/// ## Unbounded Memory Usage
///
/// Messages coming from the I/O thread use in-memory channels that are unbounded to prevent a slow
/// or misbehaving channel from blocking the I/O thread. This means it is possible for memory usage
/// to also grow in an unbounded way. There are two ways an unbounded in-memory channel gets
/// created:
///
/// * Creating a consumer; the channel for delivering messages is unbounded.
/// * Attaching a [returned message listener](#method.listen_for_returns); the channel for
/// delivering returned messages is unbounded.
///
/// To control the memory usage of consumers, avoid the use of `no_ack` consumers. If the consumer
/// is set up to acknowledge messages, the server will not send messages until previous messages
/// have been acknowledged, and you can use [`qos`](#method.qos) to control how many outstanding
/// unacknowledged messages are allowed. `no_ack` consumers do provide higher performance, but
/// amiquip does not have a mechanism for avoiding unbounded growth on the consumer channel if the
/// consumer is not processing messages fast enough to keep up with deliveries from the server.
///
/// There is no built-in mechanism to limit memory growth on a channel's returned message listener.
/// If the returned message listener cannot keep up with the rate of returned messages, consider
/// dropping the listener (which will force the I/O thread to discard returned messages instead of
/// buffering them into a channel) and reattaching a new listener once you have caught up.
///
/// ## Connection Errors
///
/// If the connection that opened this channel closes, operations on this channel will fail, and
/// may or may not return meaningful error messages. See the discussion on
/// [`Connection::close`](struct.Connection.html#method.close) for a strategy to deal with this.
pub struct Channel {
    inner: RefCell<ChannelHandle>,
    closed: bool,
}

impl Drop for Channel {
    fn drop(&mut self) {
        let _ = self.close_impl();
    }
}

impl Channel {
    pub(crate) fn new(handle: ChannelHandle) -> Channel {
        Channel {
            inner: RefCell::new(handle),
            closed: false,
        }
    }

    /// Synchronously close this channel. This method blocks until the server confirms that the
    /// channel has been closed (or an error occurs).
    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        // this can only happen if we're called from drop (since close() takes self),
        // in which case the return value doesn't matter.
        if self.closed {
            return Ok(());
        }

        // go ahead and set closed to trigger the above codepath if we're about to
        // be dropped.
        self.closed = true;
        self.inner.borrow_mut().close()
    }

    /// Return integral ID of this channel. No two open channels on the same connection may have
    /// the same channel ID, but channel IDs can be reused if a channel is opened then closed; its
    /// ID becomes available for use by a new channel.
    pub fn channel_id(&self) -> u16 {
        self.inner.borrow().channel_id()
    }

    fn call<M: IntoAmqpClass + Debug, T: TryFromAmqpClass>(&self, method: M) -> Result<T> {
        self.inner.borrow_mut().call(method)
    }

    fn call_nowait<M: IntoAmqpClass + Debug>(&self, method: M) -> Result<()> {
        self.inner.borrow_mut().call_nowait(method)
    }

    /// Specify the prefetching window.
    ///
    /// If `prefetch_size` is greater than 0, instructs the server to go ahead and send messages up
    /// to `prefetch_size` in bytes even before previous deliveries have been acknowledged. If
    /// `prefetch_count` is greater than 0, instructs the server to go ahead and send up to
    /// `prefetch_count` messages even before previous deliveries have been acknowledged. If either
    /// field is 0, that field is ignored. If both are 0, prefetching is disabled. If both are
    /// nonzero, messages will only be sent before previous deliveries are acknowledged if that
    /// send would satisfy both prefetch limits. If a consumer is started with `no_ack` set to
    /// true, prefetch limits are ignored and messages are sent as quickly as possible.
    ///
    /// According to the AMQP spec, setting `global` to true means to apply these prefetch settings
    /// to all channels in the entire connection, and `global` false means the settings apply only
    /// to this channel. RabbitMQ does not interpret `global` the same way; for it, `global: true`
    /// means the settings apply to all consumers on this channel, and `global: false` means the
    /// settings apply only to consumers created on this channel after this call to `qos`, not
    /// affecting previously-created consumers.
    pub fn qos(&self, prefetch_size: u32, prefetch_count: u16, global: bool) -> Result<()> {
        self.call::<_, QosOk>(AmqpBasic::Qos(Qos {
            prefetch_size,
            prefetch_count,
            global,
        }))
        .map(|_qos_ok| ())
    }

    /// Ask the server to redeliver all unacknowledged messages on this channel. If `requeue` is
    /// false, the server will attempt to redeliver to the original recipient. If it is true, it
    /// will attempt to requeue the message, potentially delivering it to a different recipient.
    pub fn recover(&self, requeue: bool) -> Result<()> {
        self.call::<_, RecoverOk>(AmqpBasic::Recover(Recover { requeue }))
            .map(|_recover_ok| ())
    }

    /// Publish a message to `exchange`. If the exchange does not exist, the server will close this
    /// channel. Consider using one of the [`exchange_declare`](#method.exchange_declare) methods
    /// and then [`Exchange::publish`](struct.Exchange.html#method.publish) to avoid this.
    pub fn basic_publish<S: Into<String>>(&self, exchange: S, publish: Publish) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner.call_nowait(AmqpBasic::Publish(AmqpPublish {
            ticket: 0,
            exchange: exchange.into(),
            routing_key: publish.routing_key,
            mandatory: publish.mandatory,
            immediate: publish.immediate,
        }))?;
        inner.send_content(
            publish.body,
            AmqpPublish::get_class_id(),
            &publish.properties,
        )
    }

    /// Open a crossbeam channel to receive publisher confirmations from the server.
    ///
    /// You should call this method before either calling
    /// [`enable_publisher_confirms`](#method.enable_publisher_confirms) or before publishing any
    /// messages, or you risk missing some confirmations.
    ///
    /// The [`Confirm`](enum.Confirm.html) messages sent to this receiver are the raw confirmation
    /// messages from the server; they may be out of order or be confirms for multiple messages. If
    /// you want to process perfectly sequential confirmation messages, consider using
    /// [`ConfirmSmoother`](struct.ConfirmSmoother.html).
    ///
    /// There can be only one return listener per channel. If you call this method a second (or
    /// more) time, the I/O thread will drop the sending side of previously returned channels.
    ///
    /// Dropping the `Receiver` returned by this method is harmless. If the I/O loop receives a
    /// confirmation and there is no listener registered or the previously-registered listener has
    /// been dropped, it will discard the confirmation
    pub fn listen_for_publisher_confirms(&self) -> Result<Receiver<Confirm>> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.inner.borrow_mut().set_pub_confirm_handler(Some(tx))?;
        Ok(rx)
    }

    /// Synchronously enable [publisher confirms](https://www.rabbitmq.com/confirms.html) on this
    /// channel. Confirmations will be delivered to the channel registered via
    /// [`listen_for_publisher_confirms`](#method.listen_for_publisher_confirms).
    pub fn enable_publisher_confirms(&self) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner
            .call::<_, ConfirmSelectOk>(AmqpConfirm::Select(ConfirmSelect { nowait: false }))
            .map(|_select_ok| ())
    }

    /// Asynchronously enable [publisher confirms](https://www.rabbitmq.com/confirms.html) on this
    /// channel. Confirmations will be delivered to the channel registered via
    /// [`listen_for_publisher_confirms`](#method.listen_for_publisher_confirms).
    pub fn enable_publisher_confirms_nowait(&self) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner.call_nowait(AmqpConfirm::Select(ConfirmSelect { nowait: true }))
    }

    /// Open a crossbeam channel to receive returned messages from the server (i.e., messages
    /// [published](#method.basic_publish) as `mandatory` or `immediate` that could not be
    /// delivered).
    ///
    /// There can be only one return listener per channel. If you call this method a second (or
    /// more) time, the I/O thread will drop the sending side of previously returned channels.
    ///
    /// Dropping the `Receiver` returned by this method is harmless. If the I/O loop receives a
    /// returned message and there is no listener registered or the previously-registered listener
    /// has been dropped, it will discard the message.
    pub fn listen_for_returns(&self) -> Result<Receiver<Return>> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.inner.borrow_mut().set_return_handler(Some(tx))?;
        Ok(rx)
    }

    /// Synchronously declare a queue named `queue` with the given options.
    ///
    /// If `queue` is `""` (the empty string), the server will assign an automatically generated
    /// queue name; use [`Queue::name`](struct.Queue.html#method.name) to get access to that name.
    ///
    /// If the server cannot declare the queue (e.g., if the queue already exists with options that
    /// conflict with `options`), it will close this channel.
    pub fn queue_declare<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeclareOptions,
    ) -> Result<Queue> {
        let declare = AmqpQueue::Declare(options.into_declare(queue.into(), false, false));
        let ok = self.call::<_, QueueDeclareOk>(declare)?;
        Ok(Queue::new(
            self,
            ok.queue,
            Some(ok.message_count),
            Some(ok.consumer_count),
        ))
    }

    /// Asynchronously declare a queue named `queue` with the given options.
    ///
    /// If the server cannot declare the queue (e.g., if the queue already exists with options that
    /// conflict with `options`), it will close this channel.
    ///
    /// # Panics
    ///
    /// This method will panic if `queue` is `""` (the empty string), as we would not receive a
    /// reply from the server telling us what the autogenerated name is.
    pub fn queue_declare_nowait<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeclareOptions,
    ) -> Result<Queue> {
        let queue = queue.into();
        assert!(
            queue != "",
            "cannot asynchronously declare auto-named queues"
        );
        let declare = AmqpQueue::Declare(options.into_declare(queue.clone(), false, true));
        self.call_nowait(declare)?;
        Ok(Queue::new(self, queue, None, None))
    }

    /// Passively declare that a queue exists. This asks the server to confirm that a queue named
    /// `queue` already exists; it will close the channel if it does not.
    pub fn queue_declare_passive<S: Into<String>>(&self, queue: S) -> Result<Queue> {
        // per spec, if passive is set all other fields are ignored except nowait (which
        // must be false to be meaningful)
        let options = QueueDeclareOptions {
            durable: false,
            exclusive: false,
            auto_delete: false,
            arguments: FieldTable::new(),
        };
        let declare = AmqpQueue::Declare(options.into_declare(queue.into(), true, false));
        let ok = self.call::<_, QueueDeclareOk>(declare)?;
        Ok(Queue::new(
            self,
            ok.queue,
            Some(ok.message_count),
            Some(ok.consumer_count),
        ))
    }

    /// Synchronously get a single message from `queue`. If the queue does not exist, the server
    /// will close this channel. Consider using one of the [`queue_declare`](#method.queue_declare)
    /// methods and then [`Queue::get`](struct.Queue.html#method.get) to avoid this.
    ///
    /// On success, returns `Some(message)` if there was a message in the queue or `None` if there
    /// were no messages in the queue. If `no_ack` is false, you are responsible for acknowledging
    /// the returned message, typically via [`Get::ack`](struct.Get.html#method.ack).
    ///
    /// Prefer using [`basic_consume`](#method.basic_consume) to allow the server to push messages
    /// to you on demand instead of polling with `get`.
    pub fn basic_get<S: Into<String>>(&self, queue: S, no_ack: bool) -> Result<Option<Get>> {
        self.inner.borrow_mut().get(AmqpGet {
            ticket: 0,
            queue: queue.into(),
            no_ack,
        })
    }

    /// Synchronously set up a consumer on `queue`. If the queue does not exist, the server will
    /// close this channel. Consider using one of the [`queue_declare`](#method.queue_declare)
    /// methods and then [`Queue::consume`](struct.Queue.html#method.consume) to avoid this.
    pub fn basic_consume<S: Into<String>>(
        &self,
        queue: S,
        options: ConsumerOptions,
    ) -> Result<Consumer> {
        // NOTE: We currently don't support nowait consumers for two reasons:
        // 1. We always let the server pick the consumption tag, so without
        //    the consume-ok we don't have a tag to cancel.
        // 2. The I/O loop allocates the channel to send deliveries when it
        //    receives consume-ok.
        let (tag, rx) = self.inner.borrow_mut().consume(Consume {
            ticket: 0,
            queue: queue.into(),
            consumer_tag: String::new(),
            no_local: options.no_local,
            no_ack: options.no_ack,
            exclusive: options.exclusive,
            nowait: false,
            arguments: options.arguments,
        })?;
        Ok(Consumer::new(self, tag, rx))
    }

    /// Syncronously bind `queue` to `exchange` with the given routing key and arguments.
    ///
    /// If either the queue or the exchange do not exist, the server will close this channel.
    /// Consider using the [`queue_declare`](#method.queue_declare) and
    /// [`exchange_declare`](#method.exchange_declare) methods and then using
    /// [`Queue::bind`](struct.Queue.html#method.bind) to avoid this.
    pub fn queue_bind<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        queue: S0,
        exchange: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let bind = AmqpQueue::Bind(QueueBind {
            ticket: 0,
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments,
        });
        self.call::<_, QueueBindOk>(bind).map(|_ok| ())
    }

    /// Asyncronously bind `queue` to `exchange` with the given routing key and arguments.
    ///
    /// If either the queue or the exchange do not exist, the server will close this channel.
    /// Consider using the [`queue_declare`](#method.queue_declare) and
    /// [`exchange_declare`](#method.exchange_declare) methods and then using
    /// [`Queue::bind_nowait`](struct.Queue.html#method.bind_nowait) to avoid this.
    pub fn queue_bind_nowait<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        queue: S0,
        exchange: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let bind = AmqpQueue::Bind(QueueBind {
            ticket: 0,
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            nowait: true,
            arguments,
        });
        self.call_nowait(bind)
    }

    /// Syncronously unbind `queue` from `exchange` with the given routing key and arguments.
    ///
    /// If either the queue or the exchange do not exist, the server will close this channel.
    /// Consider using the [`queue_declare`](#method.queue_declare) and
    /// [`exchange_declare`](#method.exchange_declare) methods and then using
    /// [`Queue::unbind`](struct.Queue.html#method.unbind) to avoid this.
    pub fn queue_unbind<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        queue: S0,
        exchange: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let unbind = AmqpQueue::Unbind(QueueUnbind {
            ticket: 0,
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments,
        });
        self.call::<_, QueueUnbindOk>(unbind).map(|_| ())
    }

    /// Synchronously purge all messages from `queue`. On success, returns the number of messages
    /// purged.
    ///
    /// If the queue does not exist, the server will close this channel. Consider using one of the
    /// [`queue_declare`](#method.queue_declare) methods and then
    /// [`Queue::purge`](struct.Queue.html#method.purge) to avoid this.
    pub fn queue_purge<S: Into<String>>(&self, queue: S) -> Result<u32> {
        let purge = AmqpQueue::Purge(QueuePurge {
            ticket: 0,
            queue: queue.into(),
            nowait: false,
        });
        self.call::<_, QueuePurgeOk>(purge)
            .map(|ok| ok.message_count)
    }

    /// Asynchronously purge all messages from `queue`.
    ///
    /// If the queue does not exist, the server will close this channel. Consider using one of the
    /// [`queue_declare`](#method.queue_declare) methods and then
    /// [`Queue::purge_nowait`](struct.Queue.html#method.purge_nowait) to avoid this.
    pub fn queue_purge_nowait<S: Into<String>>(&self, queue: S) -> Result<()> {
        let purge = AmqpQueue::Purge(QueuePurge {
            ticket: 0,
            queue: queue.into(),
            nowait: true,
        });
        self.call_nowait(purge)
    }

    /// Synchronously delete `queue`. On success, returns the number of messages that were in the
    /// queue when it was deleted.
    ///
    /// If the queue does not exist, the server will close this channel. Consider using one of the
    /// [`queue_declare`](#method.queue_declare) methods and then
    /// [`Queue::delete`](struct.Queue.html#method.delete) to avoid this.
    pub fn queue_delete<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeleteOptions,
    ) -> Result<u32> {
        let delete = AmqpQueue::Delete(options.into_delete(queue.into(), false));
        self.call::<_, QueueDeleteOk>(delete)
            .map(|ok| ok.message_count)
    }

    /// Synchronously delete `queue`.
    ///
    /// If the queue does not exist, the server will close this channel. Consider using one of the
    /// [`queue_declare`](#method.queue_declare) methods and then
    /// [`Queue::delete_nowait`](struct.Queue.html#method.delete_nowait) to avoid this.
    pub fn queue_delete_nowait<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeleteOptions,
    ) -> Result<()> {
        let delete = AmqpQueue::Delete(options.into_delete(queue.into(), true));
        self.call_nowait(delete)
    }

    /// Synchronously declare an exchange named `exchange` with the given type and options.
    ///
    /// If the server cannot declare the exchange (e.g., if the exchange already exists with a
    /// different type or options that conflict with `options`), it will close this channel.
    pub fn exchange_declare<S: Into<String>>(
        &self,
        type_: ExchangeType,
        exchange: S,
        options: ExchangeDeclareOptions,
    ) -> Result<Exchange> {
        let exchange = exchange.into();
        let declare =
            AmqpExchange::Declare(options.into_declare(type_, exchange.clone(), false, false));
        self.call::<_, ExchangeDeclareOk>(declare)
            .map(|_ok| Exchange::new(self, exchange))
    }

    /// Asynchronously declare an exchange named `exchange` with the given type and options.
    ///
    /// If the server cannot declare the exchange (e.g., if the exchange already exists with a
    /// different type or options that conflict with `options`), it will close this channel.
    pub fn exchange_declare_nowait<S: Into<String>>(
        &self,
        type_: ExchangeType,
        exchange: S,
        options: ExchangeDeclareOptions,
    ) -> Result<Exchange> {
        let exchange = exchange.into();
        let declare =
            AmqpExchange::Declare(options.into_declare(type_, exchange.clone(), false, true));
        self.call_nowait(declare)
            .map(|()| Exchange::new(self, exchange))
    }

    /// Passively declare that a exchange exists. This asks the server to confirm that a exchange
    /// named `exchange` already exists; it will close the channel if it does not.
    pub fn exchange_declare_passive<S: Into<String>>(&self, exchange: S) -> Result<Exchange> {
        let exchange = exchange.into();
        // per spec, if passive is set all other fields are ignored except nowait (which
        // must be false to be meaningful)
        let type_ = ExchangeType::Direct;
        let options = ExchangeDeclareOptions {
            durable: false,
            auto_delete: false,
            internal: false,
            arguments: FieldTable::new(),
        };
        let declare =
            AmqpExchange::Declare(options.into_declare(type_, exchange.clone(), true, false));
        self.call::<_, ExchangeDeclareOk>(declare)
            .map(|_ok| Exchange::new(self, exchange))
    }

    /// Synchronously bind an exchange to an exchange with the given routing key and arguments.
    ///
    /// If either the source or destination exchanges do not exist, the server will close this
    /// channel. Consider using [`exchange_declare`](#method.exchange_declare) and then
    /// [`Exchange::bind_to_source`](struct.Exchange.html#method.bind_to_source) (or one of its
    /// variants) to avoid this.
    ///
    /// Exchange-to-exchange binding is a RabbitMQ extension. You can examine the connection's
    /// [server properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn exchange_bind<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        destination: S0,
        source: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let bind = AmqpExchange::Bind(ExchangeBind {
            ticket: 0,
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments,
        });
        self.call::<_, ExchangeBindOk>(bind).map(|_bind_ok| ())
    }

    /// Asynchronously bind an exchange to an exchange with the given routing key and arguments.
    ///
    /// If either the source or destination exchanges do not exist, the server will close this
    /// channel. Consider using [`exchange_declare`](#method.exchange_declare) and then
    /// [`Exchange::bind_to_source_nowait`](struct.Exchange.html#method.bind_to_source_nowait) (or
    /// one of its variants) to avoid this.
    ///
    /// Exchange-to-exchange binding is a RabbitMQ extension. You can examine the connection's
    /// [server properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn exchange_bind_nowait<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        destination: S0,
        source: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let bind = AmqpExchange::Bind(ExchangeBind {
            ticket: 0,
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: true,
            arguments,
        });
        self.call_nowait(bind)
    }

    /// Synchronously unbind an exchange from an exchange with the given routing key and arguments.
    ///
    /// If either the source or destination exchanges do not exist, the server will close this
    /// channel. Consider using [`exchange_declare`](#method.exchange_declare) and then
    /// [`Exchange::unbind_from_source`](struct.Exchange.html#method.unbind_from_source) (or one of
    /// its variants) to avoid this.
    ///
    /// Exchange-to-exchange binding is a RabbitMQ extension. You can examine the connection's
    /// [server properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn exchange_unbind<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        destination: S0,
        source: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let unbind = AmqpExchange::Unbind(ExchangeUnbind {
            ticket: 0,
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments,
        });
        self.call::<_, ExchangeUnbindOk>(unbind)
            .map(|_unbind_ok| ())
    }

    /// Asynchronously unbind an exchange from an exchange with the given routing key and
    /// arguments.
    ///
    /// If either the source or destination exchanges do not exist, the server will close this
    /// channel. Consider using [`exchange_declare`](#method.exchange_declare) and then
    /// [`Exchange::unbind_from_source_nowait`](struct.Exchange.html#method.unbind_from_source_nowait)
    /// (or one of its variants) to avoid this.
    ///
    /// Exchange-to-exchange binding is a RabbitMQ extension. You can examine the connection's
    /// [server properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn exchange_unbind_nowait<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        destination: S0,
        source: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let unbind = AmqpExchange::Unbind(ExchangeUnbind {
            ticket: 0,
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: true,
            arguments,
        });
        self.call_nowait(unbind)
    }

    /// Synchronously delete an exchange.
    ///
    /// If `if_unused` is true, the exchange will only be deleted if it has no queue bindings.
    ///
    /// If the server cannot delete the exchange (either because it does not exist or because
    /// `if_unused` was true and it has queue bindings), it will close this channel.
    pub fn exchange_delete<S: Into<String>>(&self, exchange: S, if_unused: bool) -> Result<()> {
        let delete = AmqpExchange::Delete(ExchangeDelete {
            ticket: 0,
            exchange: exchange.into(),
            if_unused,
            nowait: false,
        });
        self.call::<_, ExchangeDeleteOk>(delete).map(|_ok| ())
    }

    /// Asynchronously delete an exchange.
    ///
    /// If `if_unused` is true, the exchange will only be deleted if it has no queue bindings.
    ///
    /// If the server cannot delete the exchange (either because it does not exist or because
    /// `if_unused` was true and it has queue bindings), it will close this channel.
    pub fn exchange_delete_nowait<S: Into<String>>(
        &self,
        exchange: S,
        if_unused: bool,
    ) -> Result<()> {
        let delete = AmqpExchange::Delete(ExchangeDelete {
            ticket: 0,
            exchange: exchange.into(),
            if_unused,
            nowait: true,
        });
        self.call_nowait(delete)
    }

    /// Asynchronously acknowledge all messages consumers on this channel have received that have
    /// not yet been acknowledged.
    pub fn ack_all(&self) -> Result<()> {
        self.call_nowait(AmqpBasic::Ack(Ack {
            delivery_tag: 0,
            multiple: true,
        }))
    }

    pub(crate) fn basic_ack(&self, delivery: Delivery, multiple: bool) -> Result<()> {
        self.call_nowait(AmqpBasic::Ack(Ack {
            delivery_tag: delivery.delivery_tag(),
            multiple,
        }))
    }

    /// Asynchronously reject all messages consumers on this channel have received that have
    /// not yet been acknowledged. If `requeue` is true, instructs the server to attempt to requeue
    /// all such messages.
    pub fn nack_all(&self, requeue: bool) -> Result<()> {
        self.call_nowait(AmqpBasic::Nack(Nack {
            delivery_tag: 0,
            multiple: true,
            requeue,
        }))
    }

    pub(crate) fn basic_nack(
        &self,
        delivery: Delivery,
        multiple: bool,
        requeue: bool,
    ) -> Result<()> {
        self.call_nowait(AmqpBasic::Nack(Nack {
            delivery_tag: delivery.delivery_tag(),
            multiple,
            requeue,
        }))
    }

    pub(crate) fn basic_reject(&self, delivery: Delivery, requeue: bool) -> Result<()> {
        self.call_nowait(AmqpBasic::Reject(Reject {
            delivery_tag: delivery.delivery_tag(),
            requeue,
        }))
    }

    pub(crate) fn basic_cancel(&self, consumer: &Consumer) -> Result<()> {
        // NOTE: We currently don't support nowait cancel for related reasons
        // to not supproting nowait consume - we want the cancel-ok to clean
        // up channels in the I/O loop.
        self.call::<_, CancelOk>(AmqpBasic::Cancel(Cancel {
            consumer_tag: consumer.consumer_tag().to_string(),
            nowait: false,
        }))
        .map(|_ok| ())
    }
}
