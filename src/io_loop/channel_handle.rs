use super::{IoLoopCommand, IoLoopHandle, IoLoopRpc};
use crate::Result;
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

pub(crate) struct Channel0Handle(IoLoopHandle);

impl Channel0Handle {
    pub(super) fn new(handle: IoLoopHandle) -> Channel0Handle {
        assert!(
            handle.channel_id == 0,
            "handle for Channel0 must be channel 0"
        );
        Channel0Handle(handle)
    }

    #[inline]
    pub(crate) fn io_loop_result(&self) -> Option<Result<()>> {
        self.0.io_loop_result()
    }

    pub(crate) fn close_connection(&mut self) -> Result<()> {
        debug_assert!(self.0.buf.is_empty());
        let close = AmqpConnection::Close(ConnectionClose {
            reply_code: REPLY_SUCCESS as u16,
            reply_text: "goodbye".to_string(),
            class_id: 0,
            method_id: 0,
        });

        let buf = self.0.make_buf(close)?;
        self.0
            .call_rpc::<ConnectionCloseOk>(IoLoopRpc::ConnectionClose(buf))?;
        Ok(())
    }

    pub(crate) fn open_channel(&mut self, channel_id: Option<u16>) -> Result<ChannelHandle> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.0
            .send_command(IoLoopCommand::AllocateChannel(channel_id, tx))?;

        // double ?? - peel off recv error then channel allocation error
        let mut handle = rx.recv().map_err(|_| self.0.io_loop_error())??;

        debug!("opening channel {}", handle.channel_id);
        let out_of_band = String::new();
        let open = AmqpChannel::Open(ChannelOpen { out_of_band });

        let open_ok = handle.call::<_, ChannelOpenOk>(open)?;
        trace!("got open-ok: {:?}", open_ok);
        Ok(ChannelHandle(handle))
    }
}

pub(crate) struct ChannelHandle(IoLoopHandle);

impl ChannelHandle {
    pub(crate) fn close(&mut self) -> Result<()> {
        let close = AmqpChannel::Close(ChannelClose {
            reply_code: 0,
            reply_text: String::new(),
            class_id: 0,
            method_id: 0,
        });
        debug!("closing channel {}", self.0.channel_id);
        let close_ok = self.0.call::<_, ChannelCloseOk>(close)?;
        trace!("got close-ok: {:?}", close_ok);
        Ok(())
    }
}
