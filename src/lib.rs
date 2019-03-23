#![allow(dead_code)]

mod auth;
mod connection_options;
mod errors;
//mod event_loop;
mod channel;
mod connection;
mod frame_buffer;
mod heartbeats;
mod io_loop;
mod serialize;

pub use auth::Auth;
pub use channel::Channel;
pub use connection::Connection;
pub use connection_options::ConnectionOptions;
pub use errors::{ErrorKind, Result};
pub use errors::ArcError as Error;

pub use amq_protocol::protocol::basic::AMQPProperties as AmqpProperties;

#[allow(dead_code)]
mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
