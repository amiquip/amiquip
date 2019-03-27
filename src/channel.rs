use crate::io_loop::ChannelHandle;
use crate::{Consumer, Delivery, ErrorKind, Result};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{
    AMQPProperties, Ack, Cancel, CancelOk, Consume, Publish, Qos, QosOk,
};
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
