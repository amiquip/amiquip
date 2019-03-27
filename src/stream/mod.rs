use mio::net::TcpStream;
use mio::Evented;
use std::io::{Read, Write};
use crate::Result;

pub(crate) trait HandshakeStream: Evented + Send + 'static {
    type Stream: IoStream;

    fn progress_handshake(&mut self) -> Result<Option<Self::Stream>>;
}

pub trait IoStream: Read + Write + Evented + Send + 'static {}

impl IoStream for TcpStream {}

#[cfg(feature = "native-tls")]
mod native_tls;

#[cfg(feature = "native-tls")]
pub use self::native_tls::TlsConnector;
