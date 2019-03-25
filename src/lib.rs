#![allow(dead_code)]

mod auth;
mod channel;
mod connection;
mod connection_options;
mod consumer;
mod delivery;
mod errors;
mod frame_buffer;
mod heartbeats;
mod io_loop;
mod serialize;

pub use auth::Auth;
pub use channel::Channel;
pub use connection::Connection;
pub use connection_options::ConnectionOptions;
pub use consumer::{Consumer, ConsumerMessage};
pub use delivery::Delivery;
pub use errors::ArcError as Error;
pub use errors::{ErrorKind, Result};
pub use io_loop::ConnectionBlockedNotification;

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
