use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;
use crate::io_loop::{ChannelHandle, IoLoop};
use crate::{ErrorKind, Result};
use log::debug;
use mio::net::TcpStream;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct Connection {
    join_handle: Option<JoinHandle<()>>,
    channel_0: ChannelHandle,
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
        let (join_handle, channel_0) = io_loop.start(options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel_0,
        })
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        if let Some(join_handle) = self.join_handle.take() {
            debug!("closing connection");
            self.channel_0.close_connection()?;
            join_handle
                .join()
                .map_err(|err| ErrorKind::IoThreadPanic(format!("{:?}", err)))?;

            // if join_handle joined successfully, it's safe to unwrap the io_loop_result
            // because it filled it in before exiting.
            self.channel_0.io_loop_result().unwrap()
        } else {
            // no join handle left - someone already took it, which is only possible
            // if we're being called from Drop after someone called close(), and drop
            // doesn't care what we return.
            Ok(())
        }
    }
}

/*
use crate::auth::Sasl;
use crate::channel::{Channel, ChannelHandle};
use crate::connection_options::ConnectionOptions;
use crate::event_loop::{EventLoop, EventLoopHandle};
use crate::{ErrorKind, Result};
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::channel::OpenOk;
use crossbeam_channel::bounded;
use failure::ResultExt;
use log::{debug, trace};
use mio::net::TcpStream;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Arc, Mutex};
use std::thread::{Builder, JoinHandle};

pub struct Connection {
    loop_handle: EventLoopHandle,
    event_loop_thread: Option<JoinHandle<Result<()>>>,
    channels: Arc<Mutex<HashMap<u16, ChannelHandle>>>,
    channel_max: u16,
    closed: bool,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.send_close();
    }
}

impl Connection {
    pub fn new<Auth: Sasl>(
        stream: TcpStream,
        options: ConnectionOptions<Auth>,
    ) -> Result<Connection> {
        let channels = Arc::default();
        let event_loop = EventLoop::new(options, stream, Arc::clone(&channels))?;
        let (setup_tx, setup_rx) = bounded(1);

        let event_loop_thread = Builder::new()
            .name("amiquip-io".to_string())
            .spawn(move || event_loop.run(setup_tx))
            .context(ErrorKind::ForkFailed)?;

        let (channel_max, loop_handle) = match setup_rx.recv() {
            Ok((channel_max, loop_handle)) => (channel_max, loop_handle),
            Err(_) => {
                // TODO get rid of this unwrap
                let res = event_loop_thread.join().unwrap();
                assert!(res.is_err(), "event loop dropped tune_tx but didn't fail");
                return Err(res.unwrap_err());
            }
        };

        Ok(Connection {
            loop_handle,
            event_loop_thread: Some(event_loop_thread),
            channels,
            channel_max,
            closed: false,
        })
    }

    pub fn close(mut self) -> Result<()> {
        self.send_close()?;
        self.event_loop_thread.take().unwrap().join().unwrap() // TODO unwrap
    }

    fn send_close(&mut self) -> Result<()> {
        if self.closed {
            // only possible if we're being called again from our Drop impl
            Ok(())
        } else {
            self.closed = true;
            self.loop_handle.close_connection()
        }
    }

    pub fn open_channel(&mut self, id: u16) -> Result<Channel> {
        if id == 0 {
            return Err(ErrorKind::UnavailableChannelId(id))?;
        }
        let builder = {
            let mut channels = self.channels.lock().unwrap();
            match channels.entry(id) {
                Entry::Occupied(_) => return Err(ErrorKind::UnavailableChannelId(id))?,
                Entry::Vacant(vacant) => {
                    let (handle, builder) = ChannelHandle::new(id);
                    vacant.insert(handle);
                    builder
                }
            }
        };

        debug!("attempting to open channel {}", id);
        match self
            .loop_handle
            .call::<_, OpenOk>(id, method::channel_open(), &builder.rpc)
        {
            Ok(open_ok) => {
                trace!("got open-ok for channel {}: {:?}", id, open_ok);
                Ok(Channel::new(self.loop_handle.clone(), builder))
            }
            Err(err) => {
                // If there was an error opening the channel, go back and remove it.
                let mut channels = self.channels.lock().unwrap();
                channels.remove(&id);
                Err(err)
            }
        }
    }
}

mod method {
    use super::*;
    use amq_protocol::protocol::channel::Open;

    pub fn channel_open() -> AmqpChannel {
        AmqpChannel::Open(Open {
            out_of_band: "".to_string(),
        })
    }
}

mod is_sync {
    use super::*;
    fn is_sync<T: Sync>() {}
    fn _check_sync() {
        is_sync::<Connection>();
    }
}
*/
