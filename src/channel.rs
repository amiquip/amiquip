use crate::event_loop::EventLoopHandle;
use crate::{ErrorKind, Result};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{AMQPProperties, Publish};
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::CloseOk;
use amq_protocol::protocol::AMQPClass;
use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::ResultExt;
use log::{debug, trace};

pub(crate) struct ChannelHandle {
    pub(crate) rpc: Sender<AMQPClass>,
    id: u16,
}

pub(crate) struct ChannelBuilder {
    pub(crate) rpc: Receiver<AMQPClass>,
    id: u16,
}

impl ChannelHandle {
    pub(crate) fn new(id: u16) -> (ChannelHandle, ChannelBuilder) {
        let (tx, rx) = unbounded();
        (
            ChannelHandle { rpc: tx, id },
            ChannelBuilder { rpc: rx, id },
        )
    }

    pub(crate) fn send_rpc(&self, class: AMQPClass) -> Result<()> {
        Ok(self
            .rpc
            .send(class)
            .context(ErrorKind::ChannelDropped(self.id))?)
    }
}

pub struct Channel {
    loop_handle: EventLoopHandle,
    rpc: Receiver<AMQPClass>,
    id: u16,
    closed: bool,
}

impl Drop for Channel {
    fn drop(&mut self) {
        let _ = self.close_and_wait();
    }
}

impl Channel {
    pub(crate) fn new(loop_handle: EventLoopHandle, builder: ChannelBuilder) -> Channel {
        Channel {
            id: builder.id,
            loop_handle,
            rpc: builder.rpc,
            closed: false,
        }
    }

    pub fn close(mut self) -> Result<()> {
        self.close_and_wait()
    }

    pub fn basic_publish<T: AsRef<[u8]>, S0: Into<String>, S1: Into<String>>(
        &mut self,
        content: T,
        exchange: S0,
        routing_key: S1,
        mandatory: bool,
        immediate: bool,
        properties: &AMQPProperties,
    ) -> Result<()> {
        self.loop_handle.call_nowait(
            self.id,
            AmqpBasic::Publish(Publish {
                ticket: 0,
                exchange: exchange.into(),
                routing_key: routing_key.into(),
                mandatory,
                immediate,
            }),
        )?;

        let mut content = content.as_ref();
        self.loop_handle.send_content_header(
            self.id,
            Publish::get_class_id(),
            content.len(),
            properties,
        )?;
        while content.len() > 4088 {
            self.loop_handle
                .send_content_body(self.id, &content[..4088])?;
            content = &content[4088..];
        }
        self.loop_handle.send_content_body(self.id, content)
    }

    fn close_and_wait(&mut self) -> Result<()> {
        if self.closed {
            // only possible if we're being called again from our Drop impl
            Ok(())
        } else {
            self.closed = true;
            debug!("closing channel {}", self.id);
            let close_ok: CloseOk =
                self.loop_handle
                    .call(self.id, method::channel_close(), &self.rpc)?;
            trace!("got close-ok for channel {}: {:?}", self.id, close_ok);
            Ok(())
        }
    }
}

mod method {
    use super::*;
    use amq_protocol::protocol::channel::Close;

    pub fn channel_close() -> AmqpChannel {
        AmqpChannel::Close(Close {
            reply_code: 0,              // TODO
            reply_text: "".to_string(), // TODO
            class_id: 0,
            method_id: 0,
        })
    }
}
