use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::io_loop::{Channel0Handle, IoLoop};
use crate::{Channel, ConnectionBlockedNotification, ErrorKind, Result};
use crossbeam_channel::Receiver;
use log::debug;
use mio::net::TcpStream;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct Connection {
    join_handle: Option<JoinHandle<Result<()>>>,
    channel0: Channel0Handle,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.close_impl();
    }
}

impl Connection {
    pub fn open<Auth: Sasl>(
        stream: TcpStream,
        options: ConnectionOptions<Auth>,
        mem_channel_bound: usize,
        poll_timeout: Option<Duration>,
    ) -> Result<Connection> {
        let io_loop = IoLoop::new(stream, mem_channel_bound, poll_timeout)?;
        let (join_handle, channel0) = io_loop.start(options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
        })
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    pub fn blocked_notifications(&self) -> &Receiver<ConnectionBlockedNotification> {
        self.channel0.blocked_notifications()
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
