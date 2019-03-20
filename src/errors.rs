use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::AMQPClass;
use failure::{Backtrace, Context, Fail};
use std::sync::Arc;
use std::{fmt, result};

/// A type alias for handling errors throughout amiquip.
pub type Result<T> = result::Result<T, ArcError>;

#[derive(Debug, Clone)]
pub struct ArcError(pub Arc<Error>);

/// An error that can occur from amiquip.
#[derive(Debug)]
pub struct Error {
    ctx: Context<ErrorKind>,
}

impl ArcError {
    pub fn kind(&self) -> &ErrorKind {
        self.0.ctx.get_context()
    }
}

impl Fail for ArcError {
    fn cause(&self) -> Option<&Fail> {
        self.0.ctx.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.0.ctx.backtrace()
    }
}

impl fmt::Display for ArcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.ctx.fmt(f)
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

    #[fail(display = "server closed channel {} (code={}, message={})", _0, _1, _2)]
    ServerClosedChannel(u16, u16, String),

    #[fail(display = "event loop thread tried to communicate with a nonexistent client")]
    EventLoopClientDropped,

    #[fail(display = "event loop thread died (no further information available)")]
    EventLoopDropped,

    #[fail(display = "channel {} dropped without being cleanly closed", _0)]
    ChannelDropped(u16),

    #[fail(display = "AMQP protocol error - received unexpected response")]
    BadRpcResponse(AMQPClass),

    #[fail(display = "fork failed")]
    ForkFailed,

    #[fail(display = "requested channel id {} is unavailable", _0)]
    UnavailableChannelId(u16),

    #[doc(hidden)]
    #[fail(display = "invalid error case")]
    __Nonexhaustive,
}

impl From<ErrorKind> for ArcError {
    fn from(kind: ErrorKind) -> ArcError {
        ArcError(Arc::new(Error::from(Context::new(kind))))
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(ctx: Context<ErrorKind>) -> Error {
        Error { ctx }
    }
}

impl From<Context<ErrorKind>> for ArcError {
    fn from(ctx: Context<ErrorKind>) -> ArcError {
        ArcError(Arc::new(Error { ctx }))
    }
}
