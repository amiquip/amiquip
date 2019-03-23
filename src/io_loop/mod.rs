use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::errors::ArcError;
use crate::frame_buffer::FrameBuffer;
use crate::serialize::{IntoAmqpClass, OutputBuffer, TryFromAmqpClass, TryFromAmqpFrame};
use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::TuneOk;
use crossbeam_channel::Receiver as CrossbeamReceiver;
use crossbeam_channel::SendError;
use crossbeam_channel::Sender as CrossbeamSender;
use failure::{Fail, ResultExt};
use log::{debug, error, trace, warn};
use mio::net::TcpStream;
use mio::{Event, Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel::sync_channel as mio_sync_channel;
use mio_extras::channel::Receiver as MioReceiver;
use mio_extras::channel::SyncSender as MioSyncSender;
use std::io;
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, Mutex};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;

mod channel_handle;
mod channel_slots;
mod connection_state;
mod handshake_state;
mod heartbeat_timers;

pub(crate) use channel_handle::{Channel0Handle, ChannelHandle};
use channel_slots::ChannelSlots;
use connection_state::ConnectionState;
use handshake_state::HandshakeState;
use heartbeat_timers::{HeartbeatKind, HeartbeatState, HeartbeatTimers};

const STREAM: Token = Token(u16::max_value() as usize + 1);
const HEARTBEAT: Token = Token(u16::max_value() as usize + 2);

enum IoLoopRpc {
    ConnectionClose(OutputBuffer),
    Call(OutputBuffer),
}

enum IoLoopCommand {
    AllocateChannel(Option<u16>, CrossbeamSender<Result<IoLoopHandle>>),
}

enum IoLoopMessage {
    Rpc(IoLoopRpc),
    Command(IoLoopCommand),
}

struct ChannelSlot {
    rx: MioReceiver<IoLoopMessage>,
    tx: CrossbeamSender<AMQPFrame>,
}

impl ChannelSlot {
    fn new(
        mio_channel_bound: usize,
        channel_id: u16,
        io_loop_result: Arc<Mutex<Option<Result<()>>>>,
    ) -> (ChannelSlot, IoLoopHandle) {
        let (mio_tx, mio_rx) = mio_sync_channel(mio_channel_bound);
        let (tx, rx) = crossbeam_channel::unbounded();

        let channel_slot = ChannelSlot { rx: mio_rx, tx };

        let loop_handle = IoLoopHandle {
            channel_id,
            buf: OutputBuffer::empty(),
            tx: mio_tx,
            rx,
            io_loop_result,
        };

        (channel_slot, loop_handle)
    }
}

struct IoLoopHandle {
    channel_id: u16,
    buf: OutputBuffer,
    tx: MioSyncSender<IoLoopMessage>,
    rx: CrossbeamReceiver<AMQPFrame>,
    io_loop_result: Arc<Mutex<Option<Result<()>>>>,
}

impl IoLoopHandle {
    fn make_buf<M: IntoAmqpClass>(&mut self, method: M) -> Result<OutputBuffer> {
        debug_assert!(self.buf.is_empty());
        self.buf.push_method(self.channel_id, method)?;
        Ok(self.buf.drain_into_new_buf())
    }

    fn send_command(&mut self, command: IoLoopCommand) -> Result<()> {
        self.tx
            .send(IoLoopMessage::Command(command))
            .map_err(|_| self.io_loop_error())
    }

    fn call<M: IntoAmqpClass, T: TryFromAmqpClass>(&mut self, method: M) -> Result<T> {
        let buf = self.make_buf(method)?;
        self.call_rpc(IoLoopRpc::Call(buf))
    }

    fn call_rpc<T: TryFromAmqpClass>(&mut self, rpc: IoLoopRpc) -> Result<T> {
        self.tx
            .send(IoLoopMessage::Rpc(rpc))
            .map_err(|_| self.io_loop_error())?;
        let response = self.rx.recv().map_err(|_| self.io_loop_error())?;
        <T as TryFromAmqpFrame>::try_from(self.channel_id, response)
    }

    fn io_loop_error(&self) -> ArcError {
        let result = self.io_loop_result.lock().unwrap();
        match &*result {
            Some(Ok(())) | None => ErrorKind::EventLoopDropped.into(),
            Some(Err(err)) => err.clone(),
        }
    }

    fn io_loop_result(&self) -> Option<Result<()>> {
        self.io_loop_result.lock().unwrap().clone()
    }
}

pub(crate) struct IoLoop {
    stream: TcpStream,
    poll: Poll,
    poll_timeout: Option<Duration>,
    frame_buffer: FrameBuffer,
    inner: Inner,
}

impl IoLoop {
    pub(crate) fn new(
        stream: TcpStream,
        mem_channel_bound: usize,
        poll_timeout: Option<Duration>,
    ) -> Result<Self> {
        let heartbeats = HeartbeatTimers::default();

        let poll = Poll::new().context(ErrorKind::Io)?;
        poll.register(
            &stream,
            STREAM,
            Ready::readable() | Ready::writable(),
            PollOpt::edge(),
        )
        .context(ErrorKind::Io)?;
        poll.register(
            &heartbeats.timer,
            HEARTBEAT,
            Ready::readable(),
            PollOpt::edge(),
        )
        .context(ErrorKind::Io)?;

        Ok(IoLoop {
            stream,
            poll,
            poll_timeout,
            frame_buffer: FrameBuffer::new(),
            inner: Inner::new(heartbeats, mem_channel_bound),
        })
    }

    pub(crate) fn start<Auth: Sasl>(
        self,
        options: ConnectionOptions<Auth>,
    ) -> Result<(JoinHandle<()>, Channel0Handle)> {
        let (handshake_done_tx, handshake_done_rx) = crossbeam_channel::bounded(1);
        let io_loop_result = Arc::clone(&self.inner.io_loop_result);

        let (ch0_slot, ch0_handle) =
            ChannelSlot::new(self.inner.mio_channel_bound, 0, Arc::clone(&io_loop_result));

        let join_handle = Builder::new()
            .name("amiquip-io".to_string())
            .spawn(move || {
                let result = self.thread_main(options, handshake_done_tx, ch0_slot);
                let mut io_loop_result = io_loop_result.lock().unwrap();
                *io_loop_result = Some(result);
            })
            .context(ErrorKind::ForkFailed)?;

        match handshake_done_rx.recv() {
            Ok(()) => Ok((join_handle, Channel0Handle::new(ch0_handle))),
            Err(_) => {
                let result = match join_handle.join() {
                    Ok(()) => ch0_handle.io_loop_result().unwrap(),
                    Err(err) => Err(ErrorKind::IoThreadPanic(format!("{:?}", err)).into()),
                };
                assert!(
                    result.is_err(),
                    "i/o thread died without sending us tune-ok"
                );
                Err(result.unwrap_err())
            }
        }
    }

    fn thread_main<Auth: Sasl>(
        mut self,
        options: ConnectionOptions<Auth>,
        handshake_done_tx: crossbeam_channel::Sender<()>,
        ch0_slot: ChannelSlot,
    ) -> Result<()> {
        self.poll
            .register(&ch0_slot.rx, Token(0), Ready::readable(), PollOpt::edge())
            .context(ErrorKind::Io)?;
        let tune_ok = self.run_handshake(options)?;
        let channel_max = tune_ok.channel_max;
        match handshake_done_tx.send(()) {
            Ok(_) => (),
            Err(_) => return Ok(()),
        }
        self.inner.chan_slots.set_channel_max(channel_max);
        self.inner.frame_max = tune_ok.frame_max as usize;
        self.inner
            .chan_slots
            .insert(Some(0), |_| Ok((ch0_slot, ())))?;
        self.run_connection()
    }

    fn run_handshake<Auth: Sasl>(&mut self, options: ConnectionOptions<Auth>) -> Result<TuneOk> {
        let mut state = HandshakeState::Start(options);
        self.run_io_loop(
            &mut state,
            Self::handle_handshake_event,
            Self::is_handshake_done,
        )?;
        match state {
            HandshakeState::Start(_)
            | HandshakeState::Secure(_)
            | HandshakeState::Tune(_)
            | HandshakeState::Open(_) => unreachable!(),
            HandshakeState::Done(tune_ok) => Ok(tune_ok),
            HandshakeState::ServerClosing(close) => Err(ErrorKind::ServerClosedConnection(
                close.reply_code,
                close.reply_text,
            ))?,
        }
    }

    fn handle_handshake_event<Auth: Sasl>(
        &mut self,
        state: &mut HandshakeState<Auth>,
        event: Event,
    ) -> Result<()> {
        Ok(match event.token() {
            STREAM => {
                if event.readiness().is_writable() {
                    self.inner.write_to_stream(&mut self.stream)?;
                }
                if event.readiness().is_readable() {
                    self.inner.read_from_stream(
                        &mut self.stream,
                        &mut self.frame_buffer,
                        |inner, frame| state.process(inner, frame),
                    )?;
                }
            }
            HEARTBEAT => self.inner.process_heartbeat_timers()?,
            _ => unreachable!(),
        })
    }

    fn is_handshake_done<Auth: Sasl>(&self, state: &HandshakeState<Auth>) -> bool {
        match state {
            HandshakeState::Start(_)
            | HandshakeState::Secure(_)
            | HandshakeState::Tune(_)
            | HandshakeState::Open(_) => false,
            HandshakeState::Done(_) => true,
            HandshakeState::ServerClosing(_) => {
                // server initiated a close (e.g., bad vhost). don't report that we're
                // done until all our writes have gone out
                assert!(
                    self.inner.are_writes_sealed(),
                    "writes should be sealed after getting a server close request"
                );
                !self.inner.has_data_to_write()
            }
        }
    }

    fn run_connection(&mut self) -> Result<()> {
        let mut state = ConnectionState::Steady;
        self.run_io_loop(
            &mut state,
            Self::handle_steady_event,
            Self::is_connection_done,
        )?;
        match state {
            ConnectionState::Steady => unreachable!(),
            ConnectionState::ServerClosing(close) => Err(ErrorKind::ServerClosedConnection(
                close.reply_code,
                close.reply_text,
            ))?,
            ConnectionState::ClientException => Err(ErrorKind::ClientException)?,
            ConnectionState::ClientClosed => Ok(()),
        }
    }

    fn handle_steady_event(&mut self, state: &mut ConnectionState, event: Event) -> Result<()> {
        Ok(match event.token() {
            STREAM => {
                if event.readiness().is_writable() {
                    self.inner.write_to_stream(&mut self.stream)?;
                }
                if event.readiness().is_readable() {
                    self.inner.read_from_stream(
                        &mut self.stream,
                        &mut self.frame_buffer,
                        |inner, frame| state.process(inner, frame),
                    )?;
                }
            }
            HEARTBEAT => self.inner.process_heartbeat_timers()?,
            Token(n) if n <= u16::max_value() as usize => {
                self.inner.process_channel_request(n as u16, &self.poll)?
            }
            _ => unreachable!(),
        })
    }

    fn is_connection_done(&self, state: &ConnectionState) -> bool {
        match state {
            ConnectionState::Steady => false,
            ConnectionState::ClientClosed => true,
            ConnectionState::ServerClosing(_) | ConnectionState::ClientException => {
                // we're mid-close, but not actually done until all our writes have gone out
                assert!(
                    self.inner.are_writes_sealed(),
                    "writes should be sealed after getting a server close request"
                );
                !self.inner.has_data_to_write()
            }
        }
    }

    fn run_io_loop<State, F, G>(
        &mut self,
        state: &mut State,
        mut handle_event: F,
        is_done: G,
    ) -> Result<()>
    where
        F: FnMut(&mut Self, &mut State, Event) -> Result<()>,
        G: Fn(&Self, &State) -> bool,
    {
        let mut events = Events::with_capacity(128);
        loop {
            self.poll
                .poll(&mut events, self.poll_timeout)
                .context(ErrorKind::Io)?;
            if events.is_empty() {
                // TODO we get spurious wakeups when (e.g.) channels are closed and
                // the receiver goes away. Need to check for poll timeout some other
                // way...
                //return Err(ErrorKind::SocketPollTimeout)?;
                continue;
            }

            let had_data_to_write = self.inner.has_data_to_write();

            for event in events.iter() {
                handle_event(self, state, event)?;
            }

            if is_done(self, state) {
                return Ok(());
            }

            // possibly change how we're registered to the socket:
            // 1. If we had data and now we don't, switch to readable only.
            // 2. If we didn't have data and now we do, switch to read+write.
            let have_data_to_write = self.inner.has_data_to_write();
            if had_data_to_write && !have_data_to_write {
                trace!("had queued data but now we don't - waiting for socket to be readable");
                self.poll
                    .reregister(&self.stream, STREAM, Ready::readable(), PollOpt::edge())
                    .context(ErrorKind::Io)?;
            } else if !had_data_to_write && have_data_to_write {
                trace!("didn't have queued data but now we do - waiting for socket to be readable or writable");
                self.poll
                    .reregister(
                        &self.stream,
                        STREAM,
                        Ready::readable() | Ready::writable(),
                        PollOpt::edge(),
                    )
                    .context(ErrorKind::Io)?;
            }
        }
    }
}

struct Inner {
    // Buffer of data waiting to be written. May contain multiple serialized frames.
    outbuf: OutputBuffer,

    // If true, no new frames (except heartbeats) may be pushed onto outbuf. This should
    // be called when we've sent a close-ok, for example.
    writes_sealed: bool,

    // Handle to I/O loop timers for tracking rx/tx heartbeats.
    heartbeats: HeartbeatTimers,

    // Slots for open channels. Channel 0 should be here once handshake is done.
    chan_slots: ChannelSlots<ChannelSlot>,

    // Frame size (negotiated during handshake).
    frame_max: usize,

    // Bound for in-memory channels that send to our I/O thread. (Channels going _from_
    // the I/O thread are unbounded to prevent blocking the I/O thread on slow receviers.)
    mio_channel_bound: usize,

    // Slot to store the final result of our I/O thread.
    io_loop_result: Arc<Mutex<Option<Result<()>>>>,
}

impl Inner {
    fn new(heartbeats: HeartbeatTimers, mio_channel_bound: usize) -> Self {
        Inner {
            outbuf: OutputBuffer::with_protocol_header(),
            writes_sealed: false,
            heartbeats,
            chan_slots: ChannelSlots::new(),
            frame_max: 0,
            mio_channel_bound,
            io_loop_result: Arc::default(),
        }
    }

    #[inline]
    fn are_writes_sealed(&self) -> bool {
        self.writes_sealed
    }

    #[inline]
    fn seal_writes(&mut self) {
        trace!("sealing writes - no more data should be enqueued");
        self.writes_sealed = true;
    }

    #[inline]
    fn push_method<M: IntoAmqpClass>(&mut self, channel_id: u16, method: M) -> Result<()> {
        if self.writes_sealed {
            // discard data - we're in the process of closing the connection
            Ok(())
        } else {
            self.outbuf.push_method(channel_id, method)
        }
    }

    #[inline]
    fn start_heartbeats(&mut self, interval: u16) {
        if interval > 0 {
            debug!("starting heartbeat timers ({} sec)", interval);
            self.heartbeats.start(Duration::from_secs(interval as u64));
        }
    }

    #[inline]
    fn has_data_to_write(&self) -> bool {
        !self.outbuf.is_empty()
    }

    fn process_heartbeat_timers(&mut self) -> Result<()> {
        while let Some(kind) = self.heartbeats.timer.poll() {
            match kind {
                HeartbeatKind::Rx => match self.heartbeats.fire_rx() {
                    HeartbeatState::StillRunning => {
                        trace!("rx heartbeat timer fired, but have received data since last");
                    }
                    HeartbeatState::Expired => {
                        error!("missed heartbeats from server - closing connection");
                        return Err(ErrorKind::MissedServerHeartbeats)?;
                    }
                },
                HeartbeatKind::Tx => match self.heartbeats.fire_tx() {
                    HeartbeatState::StillRunning => {
                        trace!("tx heartbeat timer fired, but have sent data since last");
                    }
                    HeartbeatState::Expired => {
                        // if we already have data queued up to send, don't bother also
                        // enqueuing up a heartbeat frame
                        if self.outbuf.is_empty() {
                            debug!("sending heartbeat");
                            self.outbuf.push_heartbeat();
                        } else {
                            warn!("tx heartbeat fired, but already have queued data to write - possible socket problem");
                        }
                    }
                },
            }
        }
        Ok(())
    }

    fn process_channel_request(&mut self, channel_id: u16, poll: &Poll) -> Result<()> {
        loop {
            let slot = self
                .chan_slots
                .get(channel_id)
                .expect("received mio event for channel we deleted - this should be impossible");
            let command = match slot.rx.try_recv() {
                Ok(command) => command,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => return Err(ErrorKind::EventLoopClientDropped)?,
            };

            if !self.writes_sealed {
                match command {
                    IoLoopMessage::Rpc(IoLoopRpc::ConnectionClose(buf)) => {
                        self.outbuf.append(buf);
                        trace!("sealing writes - no more data should be enqueued");
                        self.writes_sealed = true;
                    }
                    IoLoopMessage::Rpc(IoLoopRpc::Call(buf)) => {
                        self.outbuf.append(buf);
                    }
                    IoLoopMessage::Command(IoLoopCommand::AllocateChannel(channel_id, tx)) => {
                        let io_loop_result = Arc::clone(&self.io_loop_result);
                        let mio_channel_bound = self.mio_channel_bound;
                        let result = self.chan_slots.insert(channel_id, |channel_id| {
                            let (slot, handle) =
                                ChannelSlot::new(mio_channel_bound, channel_id, io_loop_result);
                            poll.register(
                                &slot.rx,
                                Token(channel_id as usize),
                                Ready::readable(),
                                PollOpt::edge(),
                            )
                            .context(ErrorKind::Io)?;
                            Ok((slot, handle))
                        });
                        match tx.send(result) {
                            Ok(()) => (),
                            Err(SendError(Ok(handle))) => {
                                // send failed - clear the allocated channel
                                self.chan_slots.remove(handle.channel_id);
                            }
                            Err(SendError(Err(_))) => {
                                // send failed, but so did channel creation. do nothing
                            }
                        }
                    }
                }
            }
        }
    }

    fn read_from_stream<S, F>(
        &mut self,
        stream: &mut S,
        frame_buffer: &mut FrameBuffer,
        mut handler: F,
    ) -> Result<()>
    where
        S: io::Read,
        F: FnMut(&mut Inner, AMQPFrame) -> Result<()>,
    {
        let n = frame_buffer.read_from(stream, |frame| {
            trace!("read frame {:?}", frame);
            Ok(match frame {
                // Heartbeats can come at any time, so filter them out here.
                // Let ConnectionState handle all other frames.
                AMQPFrame::Heartbeat(0) => debug!("received heartbeat"),
                frame => handler(self, frame)?,
            })
        })?;
        if n > 0 {
            self.heartbeats.record_rx_activity();
        }
        Ok(())
    }

    fn write_to_stream<S: io::Write>(&mut self, stream: &mut S) -> Result<()> {
        let len = self.outbuf.len();
        let mut pos = 0;

        // Keep writing until we've written all len bytes or we hit WouldBlock.
        while pos < len {
            trace!("trying to write {} bytes", len - pos);
            let n = match stream.write(&self.outbuf[pos..]) {
                Ok(n) => {
                    self.heartbeats.record_tx_activity();
                    n
                }
                Err(err) => match err.kind() {
                    io::ErrorKind::WouldBlock => {
                        let _ = self.outbuf.drain_written(pos);
                        return Ok(());
                    }
                    _ => return Err(err.context(ErrorKind::Io))?,
                },
            };
            pos += n;
        }

        // Wrote everything we have - use clear instead of .drain_written().
        // TODO see if more writes are incoming from clients first?
        self.outbuf.clear();
        Ok(())
    }
}
