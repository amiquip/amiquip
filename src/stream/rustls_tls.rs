use super::{HandshakeStream, IoStream};
use crate::errors::*;
use mio::{Evented, Poll, PollOpt, Ready, Token};
use snafu::ResultExt;
use std::io::{self, Read, Write};
use std::marker::{Sync, Send};
use std::fmt::Debug;
use rustls_connector::{MidHandshakeTlsStream, HandshakeError};

/// Newtype wrapper around a `native_tls::TlsConnector` to make it usable by amiquip's I/O loop.
pub struct TlsConnector(rustls_connector::RustlsConnector);

impl TlsConnector {
    pub(crate) fn connect<S>(&self, domain: &str, stream: S) -> Result<TlsHandshakeStream<S>>
    where
        S: Read + Write + Debug + Sync + Send + 'static,
    {
        let inner = Some(match self.0.connect(domain, stream) {
            Ok(s) => InnerHandshake::Done(s),
            Err(HandshakeError::WouldBlock(s)) => InnerHandshake::MidHandshake(s),
            Err(HandshakeError::Failure(err)) => Err(err).context(TlsHandshakeSnafu)?,
        });
        Ok(TlsHandshakeStream { inner })
    }
}


impl From<rustls_connector::RustlsConnector> for TlsConnector {
    fn from(inner: rustls_connector::RustlsConnector) -> TlsConnector {
        TlsConnector(inner)
    }
}

pub(crate) struct TlsHandshakeStream<S: Read + Write> {
    inner: Option<InnerHandshake<S>>,
}

enum InnerHandshake<S: Read + Write> {
    MidHandshake(MidHandshakeTlsStream<S>),
    Done(rustls_connector::TlsStream<S>),
}

impl<S: Read + Write + Debug + Sync + Send + 'static> InnerHandshake<S> {
    fn get_ref(&self) -> &S {
        match self {
            InnerHandshake::MidHandshake(s) => s.get_ref(),
            InnerHandshake::Done(s) => s.get_ref(),
        }
    }
}

impl<S: Evented + Read + Write + Send + Sync + Debug + 'static> HandshakeStream for TlsHandshakeStream<S> {
    type Stream = TlsStream<S>;

    fn progress_handshake(&mut self) -> Result<Option<Self::Stream>> {
        let mid_hs = match self.inner.take().unwrap() {
            InnerHandshake::MidHandshake(mid_hs) => mid_hs,
            InnerHandshake::Done(s) => return Ok(Some(TlsStream(s))),
        };

        match mid_hs.handshake() {
            Ok(s) => Ok(Some(TlsStream(s))),
            Err(HandshakeError::WouldBlock(s)) => {
                self.inner = Some(InnerHandshake::MidHandshake(s));
                Ok(None)
            }
            Err(HandshakeError::Failure(err)) => Err(err).context(TlsHandshakeSnafu)?,
        }
    }
}

impl<S: Evented + Read + Write + Send + Debug + Sync + 'static> Evented for TlsHandshakeStream<S> {
    #[inline]
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.inner
            .as_ref()
            .unwrap()
            .get_ref()
            .register(poll, token, interest, opts)
    }

    #[inline]
    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.inner
            .as_ref()
            .unwrap()
            .get_ref()
            .reregister(poll, token, interest, opts)
    }

    #[inline]
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.inner.as_ref().unwrap().get_ref().deregister(poll)
    }
}

pub(crate) struct TlsStream<S : Read + Write + Send>(rustls_connector::TlsStream<S>);

impl<S: Evented + Read + Write + Send + 'static> IoStream for TlsStream<S> {}

impl<S: Read + Write + Send> Read for TlsStream<S> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl<S: Read + Write + Send> Write for TlsStream<S> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<S: Evented + Read + Write + Send> Evented for TlsStream<S> {
    #[inline]
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.0.get_ref().register(poll, token, interest, opts)
    }

    #[inline]
    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.0.get_ref().reregister(poll, token, interest, opts)
    }

    #[inline]
    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.0.get_ref().deregister(poll)
    }
}
