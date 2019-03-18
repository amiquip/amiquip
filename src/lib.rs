mod errors;
pub mod frame_buffer;
pub mod heartbeats;

pub use errors::{ErrorKind, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
