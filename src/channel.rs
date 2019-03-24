use crate::io_loop::ChannelHandle;
use crate::{Consumer, Result};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{AMQPProperties, Consume, Publish};
use amq_protocol::types::FieldTable;
use std::sync::{Arc, Mutex};

pub struct Channel {
    inner: Arc<Mutex<Inner>>,
}

impl Drop for Channel {
    fn drop(&mut self) {
        let _ = self.close_impl();
    }
}

impl Channel {
    pub(crate) fn new(handle: ChannelHandle) -> Channel {
        let inner = Arc::new(Mutex::new(Inner {
            handle,
            closed: false,
        }));
        Channel { inner }
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return Ok(());
        }
        // Go ahead and mark the channel as closed even before we know whether handle.close()
        // fails. The client can't retry anyway (since close() took ownership of self) and it
        // prevents drop from trying to close again.
        inner.closed = true;
        inner.handle.close()
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

        inner.handle.send_nowait(AmqpBasic::Publish(Publish {
            ticket: 0,
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            mandatory,
            immediate,
        }))?;
        inner
            .handle
            .send_content(content.as_ref(), Publish::get_class_id(), properties)
    }

    pub fn basic_consume<S: Into<String>>(
        &self,
        queue: S,
        no_local: bool,
        no_ack: bool,
        exclusive: bool,
    ) -> Result<Consumer> {
        let mut inner = self.inner.lock().unwrap();

        let (tag, rx) = inner.handle.consume(Consume {
            ticket: 0,
            queue: queue.into(),
            consumer_tag: String::new(),
            no_local,
            no_ack,
            exclusive,
            nowait: false,                // TODO should we support this?
            arguments: FieldTable::new(), // TODO anything to put here?
        })?;
        Ok(Consumer::new(tag, rx))
    }
}

struct Inner {
    handle: ChannelHandle,
    closed: bool,
}
