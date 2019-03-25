use super::{ConsumerMessage, CrossbeamReceiver, IoLoopCommand, IoLoopHandle, IoLoopRpc};
use crate::serialize::{IntoAmqpClass, TryFromAmqpClass};
use crate::{Error, ErrorKind, Result};
use amq_protocol::protocol::basic::{AMQPProperties, Consume};
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::Close as ChannelClose;
use amq_protocol::protocol::channel::CloseOk as ChannelCloseOk;
use amq_protocol::protocol::channel::Open as ChannelOpen;
use amq_protocol::protocol::channel::OpenOk as ChannelOpenOk;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use amq_protocol::protocol::constants::REPLY_SUCCESS;
use log::{debug, trace};
use std::fmt::Debug;

// Each frame has 8 bytes of overhead (7 byte header, 1 byte frame-end), so when
// we break our frames up into frame_max pieces, we need to account for this many
// bytes.
const FRAME_OVERHEAD: usize = 8;

pub(crate) struct Channel0Handle {
    handle: IoLoopHandle,
    frame_max: usize,
}

impl Channel0Handle {
    pub(super) fn new(handle: IoLoopHandle, mut frame_max: usize) -> Channel0Handle {
        assert!(
            handle.channel_id == 0,
            "handle for Channel0 must be channel 0"
        );
        if frame_max == 0 {
            frame_max = usize::max_value();
        }
        frame_max -= FRAME_OVERHEAD;
        Channel0Handle { handle, frame_max }
    }

    pub(crate) fn close_connection(&mut self) -> Result<()> {
        debug_assert!(self.handle.buf.is_empty());
        let close = AmqpConnection::Close(ConnectionClose {
            reply_code: REPLY_SUCCESS as u16,
            reply_text: "goodbye".to_string(),
            class_id: 0,
            method_id: 0,
        });

        let buf = self.handle.make_buf(close)?;
        self.handle
            .call_rpc::<ConnectionCloseOk>(IoLoopRpc::ConnectionClose(buf))?;
        Ok(())
    }

    pub(crate) fn open_channel(&mut self, channel_id: Option<u16>) -> Result<ChannelHandle> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.handle
            .send_command(IoLoopCommand::AllocateChannel(channel_id, tx))?;

        // double ?? - peel off recv error then channel allocation error
        let mut handle = rx
            .recv()
            .map_err(|_| Error::from(ErrorKind::EventLoopDropped))??;

        debug!("opening channel {}", handle.channel_id);
        let out_of_band = String::new();
        let open = AmqpChannel::Open(ChannelOpen { out_of_band });

        let open_ok = handle.call::<_, ChannelOpenOk>(open)?;
        trace!("got open-ok: {:?}", open_ok);
        Ok(ChannelHandle {
            handle,
            frame_max: self.frame_max,
        })
    }
}

pub(crate) struct ChannelHandle {
    handle: IoLoopHandle,
    frame_max: usize,
}

impl ChannelHandle {
    pub(crate) fn close(&mut self) -> Result<()> {
        let close = AmqpChannel::Close(ChannelClose {
            reply_code: 0,
            reply_text: String::new(),
            class_id: 0,
            method_id: 0,
        });
        debug!("closing channel {}", self.handle.channel_id);
        let close_ok = self.handle.call::<_, ChannelCloseOk>(close)?;
        trace!("got close-ok: {:?}", close_ok);
        Ok(())
    }

    pub(crate) fn consume(
        &mut self,
        consume: Consume,
    ) -> Result<(String, CrossbeamReceiver<ConsumerMessage>)> {
        trace!(
            "starting consumer on channel {}: {:?}",
            self.handle.channel_id,
            consume
        );
        self.handle.consume(consume)
    }

    pub(crate) fn call<M: IntoAmqpClass + Debug, T: TryFromAmqpClass>(
        &mut self,
        method: M,
    ) -> Result<T> {
        trace!(
            "calling rpc method on channel {}: {:?}",
            self.handle.channel_id,
            method
        );
        self.handle.call(method)
    }

    pub(crate) fn send_nowait<M: IntoAmqpClass + Debug>(&mut self, method: M) -> Result<()> {
        trace!(
            "sending method on channel {} without expecting a response: {:?}",
            self.handle.channel_id,
            method
        );
        self.handle.send_nowait(method)
    }

    pub(crate) fn send_content(
        &mut self,
        mut content: &[u8],
        class_id: u16,
        properties: &AMQPProperties,
    ) -> Result<()> {
        trace!(
            "sending content header on channel {} (class_id = {}, len = {})",
            self.handle.channel_id,
            class_id,
            content.len()
        );
        self.handle
            .send_content_header(class_id, content.len(), properties)?;

        while content.len() > self.frame_max {
            trace!(
                "sending partial content body frame on channel {} (len = {})",
                self.handle.channel_id,
                self.frame_max
            );
            self.handle.send_content_body(&content[..self.frame_max])?;
            content = &content[self.frame_max..];
        }
        trace!(
            "sending final content body frame on channel {} (len = {})",
            self.handle.channel_id,
            content.len()
        );
        self.handle.send_content_body(content)
    }
}
