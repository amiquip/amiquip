mod errors;
pub mod frame_buffer;
pub mod heartbeats;
mod auth;
pub mod connection_options;

pub use errors::{ErrorKind, Result};

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
