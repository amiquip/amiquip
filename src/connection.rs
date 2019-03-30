use crate::connection_options::ConnectionOptions;
use crate::io_loop::{Channel0Handle, IoLoop};
use crate::{Channel, ErrorKind, FieldTable, IoStream, Result, Sasl};
use crossbeam_channel::Receiver;
use log::debug;
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(feature = "native-tls")]
use crate::TlsConnector;

/// Asynchronous notifications sent by the server when it temporarily blocks a connection,
/// typically due to a resource alarm.
///
/// Use [`Connection::listen_for_connection_blocked`](struct.Connection.html#method.listen_for_connection_blocked)
/// to receive these notifications.
#[derive(Debug, Clone)]
pub enum ConnectionBlockedNotification {
    /// The connection has been blocked for the given reason.
    Blocked(String),

    /// The connection has been unblocked.
    Unblocked,
}

/// Tuning parameters for the amiquip client.
///
/// The options are solely used to control local behavior of the client. They are not part of the
/// AMQP spec and are not communicated with the server in any way. For options that configure the
/// AMQP connection, see [`ConnectionOptions`](struct.ConnectionOptions.html).
pub struct ConnectionTuning {
    /// Set the bound used when creating `mio_extras::channel::sync_channel()` channels for sending
    /// messages to the connection's I/O thread. The default value for this field is 16.
    ///
    /// See the discussion on [amiquip's design details](index.html#design-details) for more
    /// information.
    pub mem_channel_bound: usize,

    /// Set the maximum amount of data in bytes that the I/O thread will buffer before it begins to
    /// apply backpressure on clients by not reading from their channels. If the high water mark is
    /// reached, the I/O loop will not resume reading from client channels until the amount of
    /// buffered data drops below
    /// [`buffered_writes_low_water`](struct.ConnectionTuning.html#structfield.buffered_writes_low_water)
    /// bytes. The default value for this field is 16 MiB.
    ///
    /// See the discussion on [amiquip's design details](index.html#design-details) for more
    /// information.
    pub buffered_writes_high_water: usize,

    /// Set the low water mark for the I/O thread to resume reading from client channels. See
    /// [`buffered_writes_high_water`](struct.ConnectionTuning.html#structfield.buffered_writes_high_water)
    /// above. The default value for this field is 0 bytes.
    pub buffered_writes_low_water: usize,

    /// Set the poll timeout to be used by the I/O thread. Note that setting this value can have
    /// surprising behavior:
    ///
    /// * Internal messages to the I/O thread reset the poll timeout counter, so it is possible for
    /// the underlying TCP connection to stop responding without ever triggering a poll timeout.
    /// * If the poll timeout is set to a value less than the
    /// [heartbeats](struct.ConnectionOptions.html#method.heartbeat) specified for the
    /// connection, the poll timeout could fire (killing the I/O thread and ultimately connection)
    /// even if the underlying connection is still healthy.
    ///
    /// The primary intention for this value is to allow the initial connection to the server to
    /// time out. If you are not sure, leaving this value as `None` (the default) or setting it to
    /// some value larger than twice the heartbeat option should be sufficient.
    pub poll_timeout: Option<Duration>,
}

impl Default for ConnectionTuning {
    fn default() -> Self {
        // NOTE: If we change this, make sure to change the docs above.
        ConnectionTuning {
            mem_channel_bound: 16,
            buffered_writes_high_water: 16 << 20,
            buffered_writes_low_water: 0,
            poll_timeout: None,
        }
    }
}

impl ConnectionTuning {
    /// Set the [channel memory bound](#structfield.mem_channel_bound).
    pub fn mem_channel_bound(self, mem_channel_bound: usize) -> Self {
        ConnectionTuning {
            mem_channel_bound,
            ..self
        }
    }

    /// Set the [high water mark](#structfield.buffered_writes_high_water) for buffered data to be
    /// written to the underlying stream.
    pub fn buffered_writes_high_water(self, buffered_writes_high_water: usize) -> Self {
        ConnectionTuning {
            buffered_writes_high_water,
            ..self
        }
    }

    /// Set the [low water mark](#structfield.buffered_writes_low_water) for buffered data to be
    /// written to the underlying stream.
    pub fn buffered_writes_low_water(self, buffered_writes_low_water: usize) -> Self {
        ConnectionTuning {
            buffered_writes_low_water,
            ..self
        }
    }

    /// Set the [poll timeout](#structfield.poll_timeout).
    pub fn poll_timeout(self, poll_timeout: Option<Duration>) -> Self {
        ConnectionTuning {
            poll_timeout,
            ..self
        }
    }
}

pub struct Connection {
    join_handle: Option<JoinHandle<Result<()>>>,
    channel0: Channel0Handle,
    server_properties: FieldTable,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.close_impl();
    }
}

impl Connection {
    pub fn open<Auth: Sasl, S: IoStream>(
        stream: S,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let io_loop = IoLoop::new(tuning)?;
        let (join_handle, server_properties, channel0) = io_loop.start(stream, options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
            server_properties,
        })
    }

    #[cfg(feature = "native-tls")]
    pub fn open_tls<Auth: Sasl, C: Into<TlsConnector>, S: IoStream>(
        connector: C,
        domain: &str,
        stream: S,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let stream = connector.into().connect(domain, stream)?;
        let io_loop = IoLoop::new(tuning)?;
        let (join_handle, server_properties, channel0) = io_loop.start_tls(stream, options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
            server_properties,
        })
    }

    pub fn server_properties(&self) -> &FieldTable {
        &self.server_properties
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    pub fn listen_for_connection_blocked(
        &mut self,
    ) -> Result<Receiver<ConnectionBlockedNotification>> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.channel0.set_blocked_tx(tx)?;
        Ok(rx)
    }

    fn close_impl(&mut self) -> Result<()> {
        if let Some(join_handle) = self.join_handle.take() {
            debug!("closing connection");
            self.channel0.close_connection()?;
            join_handle
                .join()
                .map_err(|err| ErrorKind::IoThreadPanic(format!("{:?}", err)))?
        } else {
            // no join handle left - someone already took it, which is only possible
            // if we're being called from Drop after someone called close(), and drop
            // doesn't care what we return.
            Ok(())
        }
    }

    pub fn open_channel(&mut self, channel_id: Option<u16>) -> Result<Channel> {
        let handle = self.channel0.open_channel(channel_id)?;
        Ok(Channel::new(handle))
    }
}
