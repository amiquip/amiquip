use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::errors::ArcError;
use crate::frame_buffer::FrameBuffer;
use crate::serialize::{IntoAmqpClass, OutputBuffer, TryFromAmqpClass, TryFromAmqpFrame};
use crate::{ErrorKind, Result};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::connection::{Close, CloseOk, TuneOk};
use amq_protocol::protocol::constants::REPLY_SUCCESS;
use crossbeam_channel::Receiver as CrossbeamReceiver;
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

mod channel_slots;
mod connection_state;
mod handshake_state;
mod heartbeat_timers;

use channel_slots::ChannelSlots;
use connection_state::ConnectionState;
use handshake_state::HandshakeState;
use heartbeat_timers::{HeartbeatKind, HeartbeatState, HeartbeatTimers};

const STREAM: Token = Token(u16::max_value() as usize + 1);
const HEARTBEAT: Token = Token(u16::max_value() as usize + 2);

enum IoLoopCommand {
    CloseConnection(OutputBuffer),
    Send(OutputBuffer),
}

struct ChannelSlot {
    rx: MioReceiver<IoLoopCommand>,
    tx: CrossbeamSender<AMQPFrame>,
}

pub(crate) struct ChannelHandle {
    io_loop_result: Arc<Mutex<Option<Result<()>>>>,
    frame_max: usize,
    channel_id: u16,
    tx: MioSyncSender<IoLoopCommand>,
    rx: CrossbeamReceiver<AMQPFrame>,
}
// TODO close on drop if we're not channel 0. Or should channel 0 close the connection
// if it's dropped?

impl ChannelHandle {
    pub(crate) fn close_connection(&mut self) -> Result<CloseOk> {
        assert!(
            self.channel_id == 0,
            "connection can only be closed from channel 0"
        );

        let close = AmqpConnection::Close(Close {
            reply_code: REPLY_SUCCESS as u16,
            reply_text: "goodbye".to_string(),
            class_id: 0,
            method_id: 0,
        });

        let buf = OutputBuffer::with_method(self.channel_id, close)?;
        self.call_command(IoLoopCommand::CloseConnection(buf))
    }

    pub(crate) fn call<M: IntoAmqpClass, T: TryFromAmqpClass>(&mut self, method: M) -> Result<T> {
        let buf = OutputBuffer::with_method(self.channel_id, method)?;
        self.call_command(IoLoopCommand::Send(buf))
    }

    pub(crate) fn io_loop_result(&self) -> Option<Result<()>> {
        self.io_loop_result.lock().unwrap().clone()
    }

    fn call_command<T: TryFromAmqpClass>(&mut self, command: IoLoopCommand) -> Result<T> {
        self.tx.send(command).map_err(|_| self.io_loop_error())?;
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
}

pub(crate) struct IoLoop {
    stream: TcpStream,
    poll: Poll,
    poll_timeout: Option<Duration>,
    mio_channel_bound: usize,
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
            mio_channel_bound: mem_channel_bound,
            frame_buffer: FrameBuffer::new(),
            inner: Inner::new(heartbeats),
        })
    }

    pub(crate) fn start<Auth: Sasl>(
        self,
        options: ConnectionOptions<Auth>,
    ) -> Result<(JoinHandle<()>, ChannelHandle)> {
        let (tune_ok_tx, tune_ok_rx) = crossbeam_channel::bounded(1);
        let (ch0_mio_tx, ch0_mio_rx) = mio_sync_channel(self.mio_channel_bound);
        let (ch0_tx, ch0_rx) = crossbeam_channel::unbounded();

        let ch0_slot = ChannelSlot {
            rx: ch0_mio_rx,
            tx: ch0_tx,
        };

        let io_loop_result = Arc::new(Mutex::new(None));
        let io_loop_result2 = Arc::clone(&io_loop_result);

        let join_handle = Builder::new()
            .name("amiquip-io".to_string())
            .spawn(move || {
                let result = self.thread_main(options, tune_ok_tx, ch0_slot);
                let mut io_loop_result2 = io_loop_result2.lock().unwrap();
                *io_loop_result2 = Some(result);
            })
            .context(ErrorKind::ForkFailed)?;

        match tune_ok_rx.recv() {
            Ok(tune_ok) => Ok((
                join_handle,
                ChannelHandle {
                    io_loop_result,
                    channel_id: 0,
                    frame_max: tune_ok.frame_max as usize,
                    tx: ch0_mio_tx,
                    rx: ch0_rx,
                },
            )),
            Err(_) => {
                let result = match join_handle.join() {
                    Ok(()) => io_loop_result.lock().unwrap().clone().unwrap(),
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
        tune_ok_tx: crossbeam_channel::Sender<TuneOk>,
        ch0_slot: ChannelSlot,
    ) -> Result<()> {
        self.poll
            .register(&ch0_slot.rx, Token(0), Ready::readable(), PollOpt::edge())
            .context(ErrorKind::Io)?;
        let tune_ok = self.run_handshake(options)?;
        let channel_max = tune_ok.channel_max;
        match tune_ok_tx.send(tune_ok) {
            Ok(_) => (),
            Err(_) => return Ok(()),
        }
        self.inner.chan_slots.set_channel_max(channel_max);
        self.inner.chan_slots.insert(Some(0), |_| ch0_slot)?;
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
                self.inner.process_channel_request(n as u16)?
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
                return Err(ErrorKind::SocketPollTimeout)?;
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
}

impl Inner {
    fn new(heartbeats: HeartbeatTimers) -> Self {
        Inner {
            outbuf: OutputBuffer::with_protocol_header(),
            writes_sealed: false,
            heartbeats,
            chan_slots: ChannelSlots::new(),
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

    fn process_channel_request(&mut self, channel_id: u16) -> Result<()> {
        let slot = self
            .chan_slots
            .get(channel_id)
            .expect("received mio event for channel we deleted - this should be impossible");
        loop {
            let command = match slot.rx.try_recv() {
                Ok(command) => command,
                Err(TryRecvError::Empty) => return Ok(()),
                Err(TryRecvError::Disconnected) => return Err(ErrorKind::EventLoopClientDropped)?,
            };

            if !self.writes_sealed {
                match command {
                    IoLoopCommand::CloseConnection(buf) => {
                        self.outbuf.append(buf);
                        trace!("sealing writes - no more data should be enqueued");
                        self.writes_sealed = true;
                    }
                    IoLoopCommand::Send(buf) => {
                        self.outbuf.append(buf);
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
