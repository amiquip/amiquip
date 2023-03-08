use crate::Result;
use mio::net::TcpStream;
use mio::Evented;
use std::io::{Read, Write};

pub(crate) trait HandshakeStream: Evented + Send + 'static {
    type Stream: IoStream;

    fn progress_handshake(&mut self) -> Result<Option<Self::Stream>>;
}

/// Combination trait for readable, writable streams that can be polled by mio.
pub trait IoStream: Read + Write + Evented + Send + 'static {}

impl IoStream for TcpStream {}

#[cfg(feature = "native-tls")]
mod native_tls;
#[cfg(feature  = "rustls-tls")]
mod rustls_tls;

#[cfg(feature = "native-tls")]
pub use self::native_tls::TlsConnector;
#[cfg(feature  = "rustls-tls")]
pub use self::rustls_tls::TlsConnector;
