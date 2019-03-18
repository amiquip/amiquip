use crate::auth::Sasl;
use crate::channel::{Channel, ChannelHandle};
use crate::connection_options::ConnectionOptions;
use crate::event_loop::{ConnectionParameters, EventLoop, EventLoopHandle};
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
    conn_params: ConnectionParameters,
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
        let (event_loop, loop_handle) = EventLoop::new(options, stream, Arc::clone(&channels))?;
        let (tune_tx, tune_rx) = bounded(1);

        let event_loop_thread = Builder::new()
            .name("amiquip-io".to_string())
            .spawn(move || event_loop.run(tune_tx))
            .context(ErrorKind::ForkFailed)?;

        let conn_params = match tune_rx.recv() {
            Ok(params) => params,
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
            conn_params,
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
