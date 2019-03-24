use crate::io_loop::ChannelHandle;
use crate::Result;
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{AMQPProperties, Publish};
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
}

struct Inner {
    handle: ChannelHandle,
    closed: bool,
}
