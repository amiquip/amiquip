use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::io_loop::{Channel0Handle, IoLoop};
use crate::{
    Channel, ConnectionBlockedNotification, ErrorKind, IoStream, NotificationListener, Result,
};
use log::debug;
use std::thread::JoinHandle;
use std::time::Duration;

#[cfg(feature = "native-tls")]
use crate::TlsConnector;

pub struct Connection {
    join_handle: Option<JoinHandle<Result<()>>>,
    channel0: Channel0Handle,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.close_impl();
    }
}

pub struct ConnectionTuning {
    pub mem_channel_bound: usize,
    pub buffered_writes_high_water: usize,
    pub buffered_writes_low_water: usize,
    pub poll_timeout: Option<Duration>,
}

impl Default for ConnectionTuning {
    fn default() -> Self {
        ConnectionTuning {
            mem_channel_bound: 16,
            buffered_writes_high_water: 16 << 20,
            buffered_writes_low_water: 0,
            poll_timeout: None,
        }
    }
}

impl ConnectionTuning {
    pub fn mem_channel_bound(self, mem_channel_bound: usize) -> Self {
        ConnectionTuning {
            mem_channel_bound,
            ..self
        }
    }

    pub fn buffered_writes_high_water(self, buffered_writes_high_water: usize) -> Self {
        ConnectionTuning {
            buffered_writes_high_water,
            ..self
        }
    }

    pub fn buffered_writes_low_water(self, buffered_writes_low_water: usize) -> Self {
        ConnectionTuning {
            buffered_writes_low_water,
            ..self
        }
    }

    pub fn poll_timeout(self, poll_timeout: Option<Duration>) -> Self {
        ConnectionTuning {
            poll_timeout,
            ..self
        }
    }
}

impl Connection {
    pub fn open<Auth: Sasl, S: IoStream>(
        stream: S,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let io_loop = IoLoop::new(tuning)?;
        let (join_handle, channel0) = io_loop.start(stream, options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
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
        let (join_handle, channel0) = io_loop.start_tls(stream, options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
        })
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    pub fn register_blocked_listener(&self) -> NotificationListener<ConnectionBlockedNotification> {
        self.channel0.register_conn_blocked_listener()
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
