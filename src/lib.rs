#![allow(dead_code)]

mod auth;
mod channel;
mod connection;
mod connection_options;
mod consumer;
mod delivery;
mod errors;
mod exchange;
mod frame_buffer;
mod get;
mod heartbeats;
mod io_loop;
mod notification_listeners;
mod queue;
mod return_;
mod serialize;
mod stream;

pub use auth::Auth;
pub use channel::Channel;
pub use connection::{Connection, ConnectionTuning};
pub use connection_options::ConnectionOptions;
pub use consumer::{Consumer, ConsumerMessage};
pub use delivery::Delivery;
pub use errors::ArcError as Error;
pub use errors::{ErrorKind, Result};
pub use exchange::{Exchange, ExchangeDeclareOptions, ExchangeDeleteOptions, ExchangeType};
pub use get::Get;
pub use io_loop::ConnectionBlockedNotification;
pub use notification_listeners::NotificationListener;
pub use queue::{Queue, QueueDeclareOptions, QueueDeleteOptions};
pub use return_::Return;
pub use stream::IoStream;

#[cfg(feature = "native-tls")]
pub use stream::TlsConnector;

pub use amq_protocol::protocol::basic::AMQPProperties as AmqpProperties;
pub use amq_protocol::types::AMQPValue as AmqpValue;
pub use amq_protocol::types::FieldTable;

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
