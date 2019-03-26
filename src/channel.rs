use crate::io_loop::ChannelHandle;
use crate::{Consumer, Delivery, ErrorKind, Result};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{AMQPProperties, Ack, Cancel, CancelOk, Consume, Publish};
use amq_protocol::types::FieldTable;
use log::{debug, trace};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Channel {
    inner: Arc<Mutex<Inner>>,
}

impl Channel {
    pub(crate) fn new(handle: ChannelHandle) -> Channel {
        let inner = Arc::new(Mutex::new(Inner::new(handle)));
        Channel { inner }
    }

    pub fn close(self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.close()
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
        let mut inner = self.inner.lock().unwrap();
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
    ) -> Result<Consumer> {
        let mut inner = self.inner.lock().unwrap();
        let handle = inner.get_handle_mut()?;

        let (tag, rx) = handle.consume(Consume {
            ticket: 0,
            queue: queue.into(),
            consumer_tag: String::new(),
            no_local,
            no_ack,
            exclusive,
            nowait: false,                // TODO should we support this?
            arguments: FieldTable::new(), // TODO anything to put here?
        })?;
        Ok(Consumer::new(self.clone(), tag, rx))
    }

    pub fn basic_ack(&self, delivery: &Delivery, multiple: bool) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let handle = inner.get_handle_mut()?;

        handle.call_nowait(AmqpBasic::Ack(Ack {
            delivery_tag: delivery.delivery_tag(),
            multiple,
        }))
    }

    pub fn basic_cancel(&self, consumer: &Consumer) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
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
