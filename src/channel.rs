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
        let inner = Arc::new(Mutex::new(Inner::Open(handle)));
        Channel { inner }
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        Ok(match &mut *inner {
            Inner::Open(handle) => {
                handle.close()?;
                *inner = Inner::ClientClosed;
            }
            Inner::ClientClosed => (),
        })
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

        match &mut *inner {
            Inner::Open(handle) => {
                handle.send_nowait(AmqpBasic::Publish(Publish {
                    ticket: 0,
                    exchange: exchange.into(),
                    routing_key: routing_key.into(),
                    mandatory,
                    immediate,
                }))?;
                handle.send_content(content.as_ref(), Publish::get_class_id(), properties)
            }
            Inner::ClientClosed => unreachable!("close consumes self; cannot call publish"),
        }
    }
}

enum Inner {
    Open(ChannelHandle),
    ClientClosed,
}

/*
use crate::event_loop::EventLoopHandle;
use crate::{ErrorKind, Result};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::{AMQPProperties, Publish};
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::{Close, CloseOk};
use amq_protocol::protocol::AMQPClass;
use crossbeam_channel::{unbounded, Receiver, Sender};
use failure::ResultExt;
use log::{debug, trace};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct ServerClosedError {
    is_closed: AtomicBool,
    error: Mutex<Option<ErrorKind>>,
}

pub(crate) struct ChannelHandle {
    pub(crate) rpc: Sender<AMQPClass>,
    server_closed: Arc<ServerClosedError>,
    id: u16,
}

pub(crate) struct ChannelBuilder {
    pub(crate) rpc: Receiver<AMQPClass>,
    server_closed: Arc<ServerClosedError>,
    id: u16,
}

impl ChannelHandle {
    pub(crate) fn new(id: u16) -> (ChannelHandle, ChannelBuilder) {
        let server_closed = Arc::default();
        let (tx, rx) = unbounded();
        (
            ChannelHandle {
                rpc: tx,
                server_closed: Arc::clone(&server_closed),
                id,
            },
            ChannelBuilder {
                rpc: rx,
                server_closed,
                id,
            },
        )
    }

    pub(crate) fn send_rpc(&self, class: AMQPClass) -> Result<()> {
        Ok(self
            .rpc
            .send(class)
            .context(ErrorKind::ChannelDropped(self.id))?)
    }

    pub(crate) fn set_server_closed(&self, close: Close) {
        {
            let mut error = self.server_closed.error.lock().unwrap();
            *error = Some(ErrorKind::ServerClosedChannel(
                self.id,
                close.reply_code,
                close.reply_text,
            ));
        }
        self.server_closed.is_closed.store(true, Ordering::SeqCst);
    }
}

pub struct Channel {
    loop_handle: EventLoopHandle,
    rpc: Receiver<AMQPClass>,
    id: u16,
    closed: bool,
    server_closed: Arc<ServerClosedError>,
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
            server_closed: builder.server_closed,
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
        self.check_server_closed()?;
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

        self.loop_handle.send_content(
            self.id,
            content.as_ref(),
            Publish::get_class_id(),
            properties,
        )
    }

    fn check_server_closed(&self) -> Result<()> {
        if !self.server_closed.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // got a server close request - bail with the error we were given; safe to
        // unwrap because is_closed is only set after the error is filled in
        let error = self.server_closed.error.lock().unwrap();
        Err(error.clone().unwrap())?
    }

    fn close_and_wait(&mut self) -> Result<()> {
        // if server already closed, nothing for us to do.
        self.check_server_closed()?;

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
*/
