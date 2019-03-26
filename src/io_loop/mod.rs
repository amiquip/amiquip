use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::frame_buffer::FrameBuffer;
use crate::serialize::{IntoAmqpClass, OutputBuffer, SealableOutputBuffer};
use crate::{ConsumerMessage, ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::TuneOk;
use amq_protocol::protocol::AMQPClass;
use crossbeam_channel::Receiver as CrossbeamReceiver;
use crossbeam_channel::SendError;
use crossbeam_channel::Sender as CrossbeamSender;
use failure::{Fail, ResultExt};
use log::{debug, error, trace, warn};
use mio::net::TcpStream;
use mio::{Event, Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel::sync_channel as mio_sync_channel;
use mio_extras::channel::Receiver as MioReceiver;
use std::collections::hash_map::HashMap;
use std::io;
use std::sync::mpsc::TryRecvError;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;

mod channel_handle;
mod channel_slots;
mod connection_state;
mod delivery_collector;
mod handshake_state;
mod heartbeat_timers;
mod io_loop_handle;

pub(crate) use channel_handle::{Channel0Handle, ChannelHandle};
use channel_slots::ChannelSlots;
use connection_state::ConnectionState;
use delivery_collector::DeliveryCollector;
use handshake_state::HandshakeState;
use heartbeat_timers::{HeartbeatKind, HeartbeatState, HeartbeatTimers};
use io_loop_handle::{IoLoopHandle, IoLoopHandle0};

const STREAM: Token = Token(u16::max_value() as usize + 1);
const HEARTBEAT: Token = Token(u16::max_value() as usize + 2);

#[derive(Debug)]
enum IoLoopRpc {
    ConnectionClose(OutputBuffer),
    Send(OutputBuffer),
}

#[derive(Debug)]
enum IoLoopCommand {
    AllocateChannel(Option<u16>, CrossbeamSender<Result<IoLoopHandle>>),
}

#[derive(Debug)]
enum IoLoopMessage {
    Rpc(IoLoopRpc),
    Command(IoLoopCommand),
}

#[derive(Debug)]
enum ChannelMessage {
    ConsumeOk(String, CrossbeamReceiver<ConsumerMessage>),
    Method(AMQPClass),
}

struct ChannelSlot {
    rx: MioReceiver<IoLoopMessage>,
    tx: CrossbeamSender<Result<ChannelMessage>>,
    collector: DeliveryCollector,
    consumers: HashMap<String, CrossbeamSender<ConsumerMessage>>,
}

impl ChannelSlot {
    fn new(mio_channel_bound: usize, channel_id: u16) -> (ChannelSlot, IoLoopHandle) {
        let (mio_tx, mio_rx) = mio_sync_channel(mio_channel_bound);
        let (tx, rx) = crossbeam_channel::unbounded();

        let channel_slot = ChannelSlot {
            rx: mio_rx,
            tx,
            collector: DeliveryCollector::new(),
            consumers: HashMap::new(),
        };

        let loop_handle = IoLoopHandle::new(channel_id, mio_tx, rx);

        (channel_slot, loop_handle)
    }
}

#[derive(Debug)]
pub enum ConnectionBlockedNotification {
    Blocked(String),
    Unblocked,
}

struct Channel0Slot {
    common: ChannelSlot,
    blocked_tx: CrossbeamSender<ConnectionBlockedNotification>,
}

impl Channel0Slot {
    fn new(mio_channel_bound: usize) -> (Channel0Slot, IoLoopHandle0) {
        let (common_slot, common_handle) = ChannelSlot::new(mio_channel_bound, 0);
        let (blocked_tx, blocked_rx) = crossbeam_channel::unbounded();

        let slot = Channel0Slot {
            common: common_slot,
            blocked_tx,
        };
        let handle = IoLoopHandle0::new(common_handle, blocked_rx);

        (slot, handle)
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
    ) -> Result<(JoinHandle<Result<()>>, Channel0Handle)> {
        let (handshake_done_tx, handshake_done_rx) = crossbeam_channel::bounded(1);

        let (ch0_slot, ch0_handle) = Channel0Slot::new(self.inner.mio_channel_bound);

        let join_handle = Builder::new()
            .name("amiquip-io".to_string())
            .spawn(move || self.thread_main(options, handshake_done_tx, ch0_slot))
            .context(ErrorKind::ForkFailed)?;

        match handshake_done_rx.recv() {
            Ok(frame_max) => Ok((join_handle, Channel0Handle::new(ch0_handle, frame_max))),

            // If sender was dropped without sending, the I/O thread has failed; peel out
            // its final error.
            Err(_) => match join_handle.join() {
                Ok(Ok(())) => {
                    unreachable!("I/O thread ended successfully without completing handshake")
                }
                Ok(Err(err)) => Err(err),
                Err(err) => Err(ErrorKind::IoThreadPanic(format!("{:?}", err)).into()),
            },
        }
    }

    fn thread_main<Auth: Sasl>(
        mut self,
        options: ConnectionOptions<Auth>,
        handshake_done_tx: crossbeam_channel::Sender<usize>,
        ch0_slot: Channel0Slot,
    ) -> Result<()> {
        self.poll
            .register(
                &ch0_slot.common.rx,
                Token(0),
                Ready::readable(),
                PollOpt::edge(),
            )
            .context(ErrorKind::Io)?;
        let tune_ok = self.run_handshake(options)?;
        let channel_max = tune_ok.channel_max;
        match handshake_done_tx.send(tune_ok.frame_max as usize) {
            Ok(_) => (),
            Err(_) => return Ok(()),
        }
        self.inner.chan_slots.set_channel_max(channel_max);
        self.run_connection(ch0_slot)
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

    fn run_connection(&mut self, ch0_slot: Channel0Slot) -> Result<()> {
        let mut state = ConnectionState::Steady(ch0_slot);
        self.run_io_loop(
            &mut state,
            Self::handle_steady_event,
            Self::is_connection_done,
        )?;
        match state {
            ConnectionState::Steady(_) => unreachable!(),
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
            Token(0) => match &state {
                ConnectionState::Steady(ch0_slot) => {
                    self.inner.handle_channel0_readable(ch0_slot, &self.poll)?
                }
                ConnectionState::ServerClosing(_)
                | ConnectionState::ClientException
                | ConnectionState::ClientClosed => {
                    unreachable!("ch0 slot cannot be readable after it is dropped")
                }
            },
            Token(n) if n <= u16::max_value() as usize => {
                self.inner.handle_channel_readable(n as u16, &self.poll)?
            }
            _ => unreachable!(),
        })
    }

    fn is_connection_done(&self, state: &ConnectionState) -> bool {
        match state {
            ConnectionState::Steady(_) => false,
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

            trace!("-- processing poll events --");
            for event in events.iter() {
                handle_event(self, state, event)?;
            }

            if is_done(self, state) {
                return Ok(());
            }

            // If we have data to write, reregister for readable|writable. This may be a
            // spurious reregistration, but also may not - if we wrote all the data we have
            // but didn't get a WouldBlock, and then later in the processing loop added
            // more data to write but didn't write it, mio won't wake us back up again next
            // pass unless we reregister.
            //
            // If we don't have data to write, only reregister for readable (without
            // writable) if we had data to write after the last poll; otherwise we know
            // we were already registered as readable only and don't need to rereg.
            if self.inner.has_data_to_write() {
                trace!("ending poll loop with data still to write - reregistering for writable");
                self.poll
                    .reregister(
                        &self.stream,
                        STREAM,
                        Ready::readable() | Ready::writable(),
                        PollOpt::edge(),
                    )
                    .context(ErrorKind::Io)?;
            } else if had_data_to_write {
                trace!("had queued data but now we don't - waiting for socket to be readable");
                self.poll
                    .reregister(&self.stream, STREAM, Ready::readable(), PollOpt::edge())
                    .context(ErrorKind::Io)?;
            }
        }
    }
}

struct Inner {
    // Buffer of data waiting to be written. May contain multiple serialized frames.
    // Once we've appended a connection Close or CloseOk, it will be sealed (so any
    // future writes will be silently discarded).
    outbuf: SealableOutputBuffer,

    // Handle to I/O loop timers for tracking rx/tx heartbeats.
    heartbeats: HeartbeatTimers,

    // Slots for open channels. Channel 0 should be here once handshake is done.
    chan_slots: ChannelSlots<ChannelSlot>,

    // Bound for in-memory channels that send to our I/O thread. (Channels going _from_
    // the I/O thread are unbounded to prevent blocking the I/O thread on slow receviers.)
    mio_channel_bound: usize,
}

impl Inner {
    fn new(heartbeats: HeartbeatTimers, mio_channel_bound: usize) -> Self {
        Inner {
            outbuf: SealableOutputBuffer::new(OutputBuffer::with_protocol_header()),
            heartbeats,
            chan_slots: ChannelSlots::new(),
            mio_channel_bound,
        }
    }

    #[inline]
    fn are_writes_sealed(&self) -> bool {
        self.outbuf.is_sealed()
    }

    #[inline]
    fn seal_writes(&mut self) {
        trace!("sealing writes - no more data should be enqueued");
        self.outbuf.seal();
    }

    #[inline]
    fn push_method<M: IntoAmqpClass>(&mut self, channel_id: u16, method: M) -> Result<()> {
        self.outbuf.push_method(channel_id, method)
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

    fn handle_channel0_readable(&mut self, ch0_slot: &Channel0Slot, poll: &Poll) -> Result<()> {
        match ch0_slot.common.rx.try_recv() {
            Ok(message) => self.process_channel_message(message, poll),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => Err(ErrorKind::EventLoopClientDropped)?,
        }
    }

    fn handle_channel_readable(&mut self, channel_id: u16, poll: &Poll) -> Result<()> {
        loop {
            let slot = match self.chan_slots.get(channel_id) {
                Some(slot) => slot,
                None => {
                    // We've been asked to poll a receiver for a channel we dropped; this
                    // is rare, but could happen if (e.g.) the server initiated a Close in this
                    // same poll processing loop and we already saw it. In that case, we've
                    // already removed channel_id from chan_slots, but now we've landed in a
                    // still-pending readable event from poll. Bail out now without an error;
                    // the dropped channel will propogate an appropriate message back out to
                    // the channel handle.
                    return Ok(());
                }
            };
            match slot.rx.try_recv() {
                Ok(message) => self.process_channel_message(message, poll)?,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => return Err(ErrorKind::EventLoopClientDropped)?,
            }
        }
    }

    fn process_channel_message(&mut self, message: IoLoopMessage, poll: &Poll) -> Result<()> {
        match message {
            IoLoopMessage::Rpc(IoLoopRpc::ConnectionClose(buf)) => {
                self.outbuf.append(buf);
                self.seal_writes();
            }
            IoLoopMessage::Rpc(IoLoopRpc::Send(buf)) => {
                self.outbuf.append(buf);
            }
            IoLoopMessage::Command(IoLoopCommand::AllocateChannel(channel_id, tx)) => {
                let mio_channel_bound = self.mio_channel_bound;
                let result = self.chan_slots.insert(channel_id, |channel_id| {
                    let (slot, handle) = ChannelSlot::new(mio_channel_bound, channel_id);
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
        Ok(())
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
            handler(self, frame)
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
                    trace!("wrote {} bytes", n);
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
