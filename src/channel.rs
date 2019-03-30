use crate::io_loop::ChannelHandle;
use crate::serialize::{IntoAmqpClass, TryFromAmqpClass};
use crate::{
    Consumer, Delivery, Exchange, ExchangeDeclareOptions, ExchangeType, Get, Queue,
    QueueDeclareOptions, QueueDeleteOptions, Result, Return,
};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::Get as AmqpGet;
use amq_protocol::protocol::basic::{
    AMQPProperties, Ack, Cancel, CancelOk, Consume, Nack, Publish, Qos, QosOk, Recover, RecoverOk,
    Reject,
};
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

    pub fn channel_id(&self) -> u16 {
        self.inner.borrow().channel_id()
    }

    fn call<M: IntoAmqpClass + Debug, T: TryFromAmqpClass>(&self, method: M) -> Result<T> {
        self.inner.borrow_mut().call(method)
    }

    fn call_nowait<M: IntoAmqpClass + Debug>(&self, method: M) -> Result<()> {
        self.inner.borrow_mut().call_nowait(method)
    }

    pub fn qos(&self, prefetch_size: u32, prefetch_count: u16, global: bool) -> Result<()> {
        self.call::<_, QosOk>(AmqpBasic::Qos(Qos {
            prefetch_size,
            prefetch_count,
            global,
        }))
        .map(|_qos_ok| ())
    }

    pub fn recover(&self, requeue: bool) -> Result<()> {
        self.call::<_, RecoverOk>(AmqpBasic::Recover(Recover { requeue }))
            .map(|_recover_ok| ())
    }

    pub fn basic_publish<T: AsRef<[u8]>, S0: Into<String>, S1: Into<String>>(
        &self,
        content: T,
        exchange: S0,
        routing_key: S1,
        mandatory: bool,
        immediate: bool,
        properties: &AMQPProperties,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner.call_nowait(AmqpBasic::Publish(Publish {
            ticket: 0,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            mandatory,
            immediate,
        }))?;
        inner.send_content(content.as_ref(), Publish::get_class_id(), properties)
    }

    pub fn basic_get<S: Into<String>>(&self, queue: S, no_ack: bool) -> Result<Option<Get>> {
        self.inner.borrow_mut().get(AmqpGet {
            ticket: 0,
            queue: queue.into(),
            no_ack,
        })
    }

    pub fn basic_consume<S: Into<String>>(
        &self,
        queue: S,
        no_local: bool,
        no_ack: bool,
        exclusive: bool,
        arguments: FieldTable,
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
            no_local,
            no_ack,
            exclusive,
            nowait: false,
            arguments,
        })?;
        Ok(Consumer::new(self, tag, rx))
    }

    pub fn listen_for_returns(&self) -> Result<Receiver<Return>> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.inner.borrow_mut().set_return_handler(Some(tx))?;
        Ok(rx)
    }

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

    pub fn queue_declare_nowait<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeclareOptions,
    ) -> Result<Queue> {
        let queue = queue.into();
        let declare = AmqpQueue::Declare(options.into_declare(queue.clone(), false, true));
        self.call_nowait(declare)?;
        Ok(Queue::new(self, queue, None, None))
    }

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

    pub fn queue_purge<S: Into<String>>(&self, queue: S) -> Result<u32> {
        let purge = AmqpQueue::Purge(QueuePurge {
            ticket: 0,
            queue: queue.into(),
            nowait: false,
        });
        self.call::<_, QueuePurgeOk>(purge)
            .map(|ok| ok.message_count)
    }

    pub fn queue_purge_nowait<S: Into<String>>(&self, queue: S) -> Result<()> {
        let purge = AmqpQueue::Purge(QueuePurge {
            ticket: 0,
            queue: queue.into(),
            nowait: true,
        });
        self.call_nowait(purge)
    }

    pub fn queue_delete<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeleteOptions,
    ) -> Result<u32> {
        let delete = AmqpQueue::Delete(options.into_delete(queue.into(), false));
        self.call::<_, QueueDeleteOk>(delete)
            .map(|ok| ok.message_count)
    }

    pub fn queue_delete_nowait<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeleteOptions,
    ) -> Result<()> {
        let delete = AmqpQueue::Delete(options.into_delete(queue.into(), true));
        self.call_nowait(delete)
    }

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

    pub fn exchange_declare_nowait<S: Into<String>>(
        &self,
        type_: ExchangeType,
        exchange: S,
        options: ExchangeDeclareOptions,
    ) -> Result<Exchange> {
        let exchange = exchange.into();
        let declare =
            AmqpExchange::Declare(options.into_declare(type_, exchange.clone(), false, true));
        self.call::<_, ExchangeDeclareOk>(declare)
            .map(|_ok| Exchange::new(self, exchange))
    }

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
        self.call_nowait(declare)
            .map(|()| Exchange::new(self, exchange))
    }

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

    pub fn exchange_delete<S: Into<String>>(&self, exchange: S, if_unused: bool) -> Result<()> {
        let delete = AmqpExchange::Delete(ExchangeDelete {
            ticket: 0,
            exchange: exchange.into(),
            if_unused,
            nowait: false,
        });
        self.call::<_, ExchangeDeleteOk>(delete).map(|_ok| ())
    }

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

    pub fn nack_all(&self, requeue: bool) -> Result<()> {
        self.call_nowait(AmqpBasic::Nack(Nack {
            delivery_tag: 0,
            multiple: true,
            requeue,
        }))
    }

    pub(crate) fn basic_nack(&self, delivery: Delivery, multiple: bool, requeue: bool) -> Result<()> {
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
