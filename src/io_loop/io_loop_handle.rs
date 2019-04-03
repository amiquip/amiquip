use super::{ChannelMessage, ConnectionBlockedNotification, ConsumerMessage, IoLoopMessage};
use crate::serialize::{IntoAmqpClass, OutputBuffer, TryFromAmqpClass};
use crate::{AmqpProperties, Confirm, Error, ErrorKind, Get, Result, Return};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::Consume;
use amq_protocol::protocol::basic::Get as AmqpGet;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::Close as ConnectionClose;
use amq_protocol::protocol::connection::CloseOk as ConnectionCloseOk;
use crossbeam_channel::Receiver as CrossbeamReceiver;
use crossbeam_channel::Sender as CrossbeamSender;
use log::error;
use mio_extras::channel::SyncSender as MioSyncSender;
use std::ops::{Deref, DerefMut};

pub(super) struct IoLoopHandle {
    channel_id: u16,
    buf: OutputBuffer,
    tx: MioSyncSender<IoLoopMessage>,
    rx: CrossbeamReceiver<Result<ChannelMessage>>,
}

impl IoLoopHandle {
    pub(super) fn new(
        channel_id: u16,
        tx: MioSyncSender<IoLoopMessage>,
        rx: CrossbeamReceiver<Result<ChannelMessage>>,
    ) -> IoLoopHandle {
        IoLoopHandle {
            channel_id,
            buf: OutputBuffer::empty(),
            tx,
            rx,
        }
    }

    #[inline]
    pub(super) fn channel_id(&self) -> u16 {
        self.channel_id
    }

    fn make_buf<M: IntoAmqpClass>(&mut self, method: M) -> OutputBuffer {
        debug_assert!(self.buf.is_empty());
        self.buf.push_method(self.channel_id, method);
        self.buf.drain_into_new_buf()
    }

    pub(super) fn set_return_handler(
        &mut self,
        handler: Option<CrossbeamSender<Return>>,
    ) -> Result<()> {
        self.send(IoLoopMessage::SetReturnHandler(handler))
    }

    pub(super) fn set_pub_confirm_handler(
        &mut self,
        handler: Option<CrossbeamSender<Confirm>>,
    ) -> Result<()> {
        self.send(IoLoopMessage::SetPubConfirmHandler(handler))
    }

    pub(super) fn get(&mut self, get: AmqpGet) -> Result<Option<Get>> {
        let buf = self.make_buf(AmqpBasic::Get(get));
        self.send(IoLoopMessage::Send(buf))?;
        match self.recv()? {
            ChannelMessage::GetOk(get) => Ok(*get),
            ChannelMessage::Method(_) | ChannelMessage::ConsumeOk(_, _) => {
                Err(ErrorKind::FrameUnexpected)?
            }
        }
    }

    pub(super) fn consume(
        &mut self,
        consume: Consume,
    ) -> Result<(String, CrossbeamReceiver<ConsumerMessage>)> {
        let buf = self.make_buf(AmqpBasic::Consume(consume));
        self.send(IoLoopMessage::Send(buf))?;
        match self.recv()? {
            ChannelMessage::ConsumeOk(tag, rx) => Ok((tag, rx)),
            ChannelMessage::Method(_) | ChannelMessage::GetOk(_) => {
                Err(ErrorKind::FrameUnexpected)?
            }
        }
    }

    pub(super) fn call_connection_close(
        &mut self,
        close: ConnectionClose,
    ) -> Result<ConnectionCloseOk> {
        let buf = self.make_buf(AmqpConnection::Close(close));
        self.call_message(IoLoopMessage::ConnectionClose(buf))
    }

    pub(super) fn call<M: IntoAmqpClass, T: TryFromAmqpClass>(&mut self, method: M) -> Result<T> {
        let buf = self.make_buf(method);
        self.call_message(IoLoopMessage::Send(buf))
    }

    fn call_message<T: TryFromAmqpClass>(&mut self, message: IoLoopMessage) -> Result<T> {
        self.send(message)?;
        match self.recv()? {
            ChannelMessage::Method(method) => T::try_from(method),
            ChannelMessage::ConsumeOk(_, _) | ChannelMessage::GetOk(_) => {
                Err(ErrorKind::FrameUnexpected)?
            }
        }
    }

    pub(super) fn call_nowait<M: IntoAmqpClass>(&mut self, method: M) -> Result<()> {
        let buf = self.make_buf(method);
        self.send(IoLoopMessage::Send(buf))
    }

    pub(super) fn send_content_header(
        &mut self,
        class_id: u16,
        len: usize,
        properties: &AmqpProperties,
    ) -> Result<()> {
        debug_assert!(self.buf.is_empty());
        self.buf
            .push_content_header(self.channel_id, class_id, len, properties);
        let buf = self.buf.drain_into_new_buf();
        self.send(IoLoopMessage::Send(buf))
    }

    pub(super) fn send_content_body(&mut self, content: &[u8]) -> Result<()> {
        debug_assert!(self.buf.is_empty());
        self.buf.push_content_body(self.channel_id, content);
        let buf = self.buf.drain_into_new_buf();
        self.send(IoLoopMessage::Send(buf))
    }

    fn send(&mut self, message: IoLoopMessage) -> Result<()> {
        self.tx
            .send(message)
            .map_err(|_| self.check_recv_for_error())
    }

    fn recv(&mut self) -> Result<ChannelMessage> {
        self.rx
            .recv()
            .map_err(|_| Error::from(ErrorKind::EventLoopDropped))?
    }

    fn check_recv_for_error(&mut self) -> Error {
        // failed to send to the I/O thread; possible causes are:
        //   1. Server closed channel; we should see if there's a relevant message
        //      waiting for us on rx.
        //   2. I/O loop is actually gone.
        // In either case, recv() will return Err. If it doesn't, we got somehow
        // got a frame after a send failure - this should be impossible, but return
        // FrameUnexpected just in case.
        match self.recv() {
            Ok(_) => {
                error!("internal error - received unexpected frame after I/O thread disappeared");
                ErrorKind::FrameUnexpected.into()
            }
            Err(err) => err,
        }
    }
}

pub(super) struct IoLoopHandle0 {
    common: IoLoopHandle,
    set_blocked_tx: MioSyncSender<CrossbeamSender<ConnectionBlockedNotification>>,
    alloc_chan_req_tx: MioSyncSender<Option<u16>>,
    alloc_chan_rep_rx: CrossbeamReceiver<Result<IoLoopHandle>>,
}

impl IoLoopHandle0 {
    pub(super) fn new(
        common: IoLoopHandle,
        set_blocked_tx: MioSyncSender<CrossbeamSender<ConnectionBlockedNotification>>,
        alloc_chan_req_tx: MioSyncSender<Option<u16>>,
        alloc_chan_rep_rx: CrossbeamReceiver<Result<IoLoopHandle>>,
    ) -> IoLoopHandle0 {
        IoLoopHandle0 {
            common,
            set_blocked_tx,
            alloc_chan_req_tx,
            alloc_chan_rep_rx,
        }
    }

    pub(super) fn allocate_channel(&mut self, channel_id: Option<u16>) -> Result<IoLoopHandle> {
        self.alloc_chan_req_tx
            .send(channel_id)
            .map_err(|_| self.common.check_recv_for_error())?;
        self.alloc_chan_rep_rx
            .recv()
            .map_err(|_| Error::from(ErrorKind::EventLoopDropped))?
    }

    pub(super) fn set_blocked_tx(
        &mut self,
        tx: CrossbeamSender<ConnectionBlockedNotification>,
    ) -> Result<()> {
        self.set_blocked_tx
            .send(tx)
            .map_err(|_| self.common.check_recv_for_error())
    }
}

impl Deref for IoLoopHandle0 {
    type Target = IoLoopHandle;

    fn deref(&self) -> &IoLoopHandle {
        &self.common
    }
}

impl DerefMut for IoLoopHandle0 {
    fn deref_mut(&mut self) -> &mut IoLoopHandle {
        &mut self.common
    }
}
