use amq_protocol::frame::AMQPFrame;
use failure::{Backtrace, Context, Fail};
use std::{fmt, result};

/// A type alias for handling errors throughout amiquip.
pub type Result<T> = result::Result<T, Error>;

/// An error that can occur from amiquip.
#[derive(Debug)]
pub struct Error {
    ctx: Context<ErrorKind>,
}

impl Error {
    pub fn kind(&self) -> &ErrorKind {
        self.ctx.get_context()
    }
}

impl Fail for Error {
    fn cause(&self) -> Option<&Fail> {
        self.ctx.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.ctx.backtrace()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.ctx.fmt(f)
    }
}

/// The specific kind of error that can occur.
#[derive(Clone, Debug, PartialEq, Fail)]
pub enum ErrorKind {
    #[fail(display = "underlying socket closed unexpectedly")]
    UnexpectedSocketClose,

    #[fail(display = "received malformed data")]
    ReceivedMalformed,

    #[fail(display = "I/O error")]
    Io,

    #[fail(display = "requested auth mechanism unavailable (available = {})", _0)]
    UnsupportedAuthMechanism(String),

    #[fail(display = "requested locale unavailable (available = {})", _0)]
    UnsupportedLocale(String),

    #[fail(display = "requested frame max is too small (min = {})", _0)]
    FrameMaxTooSmall(u32),

    #[fail(display = "timeout occurred while waiting for socket events")]
    SocketPollTimeout,

    #[fail(display = "internal serialization error (THIS IS A BUG)")]
    InternalSerializationError,

    #[fail(display = "SASL secure/secure-ok exchanges are not supported")]
    SaslSecureNotSupported,

    #[fail(display = "invalid credentials")]
    InvalidCredentials,

    #[fail(display = "handshake failure - server sent a frame unexpectedly")]
    HandshakeUnexpectedServerFrame(AMQPFrame),

    #[fail(display = "handshake protocol failure - expected {} frame", _0)]
    HandshakeWrongServerFrame(&'static str, AMQPFrame),

    #[fail(display = "missed heartbeats from server")]
    MissedServerHeartbeats,

    #[fail(display = "server closed connection (code={} message={})", _0, _1)]
    ServerClosedConnection(u16, String),

    #[fail(display = "client closed connection (code={} message={})", _0, _1)]
    ClientClosedConnection(u16, String),

    #[fail(display = "event loop thread tried to send a message to a nonexistent client")]
    EventLoopClientDropped,

    #[doc(hidden)]
    #[fail(display = "invalid error case")]
    __Nonexhaustive,
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error::from(Context::new(kind))
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(ctx: Context<ErrorKind>) -> Error {
        Error { ctx }
    }
}
