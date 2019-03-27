use crate::io_loop::ChannelHandle;
use crate::{
    Consumer, Delivery, ErrorKind, Exchange, ExchangeDeclareOptions, Queue, QueueDeclareOptions,
    Result,
};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{
    AMQPProperties, Ack, Cancel, CancelOk, Consume, Publish, Qos, QosOk,
};
use amq_protocol::protocol::exchange::AMQPMethod as AmqpExchange;
use amq_protocol::protocol::exchange::Declare as ExchangeDeclare;
use amq_protocol::protocol::exchange::DeclareOk as ExchangeDeclareOk;
use amq_protocol::protocol::queue::AMQPMethod as AmqpQueue;
use amq_protocol::protocol::queue::Bind as QueueBind;
use amq_protocol::protocol::queue::BindOk as QueueBindOk;
use amq_protocol::protocol::queue::Unbind as QueueUnbind;
use amq_protocol::protocol::queue::UnbindOk as QueueUnbindOk;
use amq_protocol::protocol::queue::Declare as QueueDeclare;
use amq_protocol::protocol::queue::DeclareOk as QueueDeclareOk;
use amq_protocol::types::FieldTable;
use log::{debug, trace};
use std::cell::RefCell;

pub struct Channel {
    inner: RefCell<Inner>,
}

impl Channel {
    pub(crate) fn new(handle: ChannelHandle) -> Channel {
        let inner = RefCell::new(Inner::new(handle));
        Channel { inner }
    }

    pub fn close(self) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        inner.close()
    }

    pub fn basic_qos(&self, prefetch_size: u32, prefetch_count: u16, global: bool) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;

        handle
            .call::<_, QosOk>(AmqpBasic::Qos(Qos {
                prefetch_size,
                prefetch_count,
                global,
            }))
            .map(|_qos_ok| ())
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
        let handle = inner.get_handle_mut()?;

        handle.call_nowait(AmqpBasic::Publish(Publish {
            ticket: 0,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            mandatory,
            immediate,
        }))?;
        handle.send_content(content.as_ref(), Publish::get_class_id(), properties)
    }

    pub fn basic_consume<S: Into<String>>(
        &self,
        queue: S,
        no_local: bool,
        no_ack: bool,
        exclusive: bool,
        arguments: FieldTable,
    ) -> Result<Consumer> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;

        // NOTE: We currently don't support nowait consumers for two reasons:
        // 1. We always let the server pick the consumption tag, so without
        //    the consume-ok we don't have a tag to cancel.
        // 2. The I/O loop allocates the channel to send deliveries when it
        //    receives consume-ok.
        let (tag, rx) = handle.consume(Consume {
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

    pub fn queue_declare<S: Into<String>>(
        &self,
        queue: S,
        options: QueueDeclareOptions,
    ) -> Result<Queue> {
        self.queue_declare_common(queue, false, options)
    }

    pub fn queue_declare_passive<S: Into<String>>(&self, queue: S) -> Result<Queue> {
        // per spec, if passive is set all other fields are ignored except nowait (which
        // must be false to be meaningful)
        let options = QueueDeclareOptions {
            nowait: false,
            ..QueueDeclareOptions::default()
        };
        self.queue_declare_common(queue, true, options)
    }

    fn queue_declare_common<S: Into<String>>(
        &self,
        queue: S,
        passive: bool,
        options: QueueDeclareOptions,
    ) -> Result<Queue> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;
        let name = queue.into();

        let declare = AmqpQueue::Declare(QueueDeclare {
            ticket: 0,
            queue: name.clone(),
            passive,
            durable: options.durable,
            exclusive: options.exclusive,
            auto_delete: options.auto_delete,
            nowait: options.nowait,
            arguments: options.arguments,
        });

        debug!("declaring queue: {:?}", declare);
        if options.nowait {
            handle.call_nowait(declare)?;
        } else {
            let _ = handle.call::<_, QueueDeclareOk>(declare)?;
        }

        Ok(Queue::new(self, name))
    }

    pub fn queue_bind<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        queue: S0,
        exchange: S1,
        routing_key: S2,
        nowait: bool,
        arguments: FieldTable,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;

        let bind = AmqpQueue::Bind(QueueBind {
            ticket: 0,
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            nowait,
            arguments,
        });

        debug!("binding queue to exchange: {:?}", bind);
        if nowait {
            handle.call_nowait(bind)
        } else {
            handle.call::<_, QueueBindOk>(bind).map(|_| ())
        }
    }

    pub fn queue_unbind<S0: Into<String>, S1: Into<String>, S2: Into<String>>(
        &self,
        queue: S0,
        exchange: S1,
        routing_key: S2,
        arguments: FieldTable,
    ) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;

        let unbind = AmqpQueue::Unbind(QueueUnbind {
            ticket: 0,
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments,
        });

        debug!("unbinding queue from exchange: {:?}", unbind);
        handle.call::<_, QueueUnbindOk>(unbind).map(|_| ())
    }

    pub fn exchange_declare<S: Into<String>>(
        &self,
        exchange: S,
        options: ExchangeDeclareOptions,
    ) -> Result<Exchange> {
        self.exchange_declare_common(exchange, false, options)
    }

    pub fn exchange_declare_passive<S: Into<String>>(&self, exchange: S) -> Result<Exchange> {
        // per spec, if passive is set all other fields are ignored except nowait (which
        // must be false to be meaningful)
        let options = ExchangeDeclareOptions {
            nowait: false,
            ..ExchangeDeclareOptions::default()
        };
        self.exchange_declare_common(exchange, true, options)
    }

    fn exchange_declare_common<S: Into<String>>(
        &self,
        exchange: S,
        passive: bool,
        options: ExchangeDeclareOptions,
    ) -> Result<Exchange> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;
        let name = exchange.into();

        let declare = AmqpExchange::Declare(ExchangeDeclare {
            ticket: 0,
            exchange: name.clone(),
            passive,
            type_: options.type_.as_ref().to_string(),
            durable: options.durable,
            auto_delete: options.auto_delete,
            internal: options.internal,
            nowait: options.nowait,
            arguments: options.arguments,
        });

        debug!("declaring exchange: {:?}", declare);
        if options.nowait {
            handle.call_nowait(declare)?;
        } else {
            let _ = handle.call::<_, ExchangeDeclareOk>(declare)?;
        }

        Ok(Exchange::new(self, name))
    }

    pub fn basic_ack(&self, delivery: &Delivery, multiple: bool) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;

        handle.call_nowait(AmqpBasic::Ack(Ack {
            delivery_tag: delivery.delivery_tag(),
            multiple,
        }))
    }

    pub fn basic_cancel(&self, consumer: &Consumer) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        let handle = inner.get_handle_mut()?;

        debug!("cancelling consumer {}", consumer.consumer_tag());
        let cancel_ok = handle.call::<_, CancelOk>(AmqpBasic::Cancel(Cancel {
            consumer_tag: consumer.consumer_tag().to_string(),
            nowait: false,
        }))?;
        trace!("got cancel-ok {:?}", cancel_ok);
        Ok(())
    }
}

struct Inner {
    handle: ChannelHandle,
    closed: bool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Inner {
    fn new(handle: ChannelHandle) -> Inner {
        Inner {
            handle,
            closed: false,
        }
    }

    fn get_handle_mut(&mut self) -> Result<&mut ChannelHandle> {
        if self.closed {
            Err(ErrorKind::ClientClosedChannel)?
        } else {
            Ok(&mut self.handle)
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let result = self.handle.close();
        // Even if the close call fails, treat ourselves as closed. There's little
        // hope to recovering from a failed close call, and this prevents us from
        // attempting to close again when dropped.
        self.closed = true;
        result
    }
}
