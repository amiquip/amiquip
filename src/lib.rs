#![allow(dead_code)]

mod auth;
mod connection_options;
mod errors;
mod event_loop;
mod frame_buffer;
mod heartbeats;
mod serialize;
mod connection;
mod channel;

pub use auth::Auth;
pub use connection_options::ConnectionOptions;
pub use connection::Connection;
pub use errors::{Error, ErrorKind, Result};

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
