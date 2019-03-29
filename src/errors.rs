use failure::{Backtrace, Context, Fail};
use std::sync::Arc;
use std::{fmt, result};

/// A type alias for handling errors throughout amiquip.
pub type Result<T> = result::Result<T, Error>;

/// An error that can occur from amiquip.
#[derive(Clone, Debug)]
pub struct Error {
    ctx: Arc<Context<ErrorKind>>,
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

    #[fail(display = "received malformed data - expected AMQP frame")]
    MalformedFrame,

    #[fail(display = "I/O error")]
    Io,

    #[cfg(feature = "native-tls")]
    #[fail(display = "TLS handshake failed")]
    TlsHandshake,

    #[fail(display = "requested auth mechanism unavailable (available = {})", _0)]
    UnsupportedAuthMechanism(String),

    #[fail(display = "requested locale unavailable (available = {})", _0)]
    UnsupportedLocale(String),

    #[fail(display = "requested frame max is too small (min = {})", _0)]
    FrameMaxTooSmall(u32),

    #[fail(display = "timeout occurred while waiting for socket events")]
    SocketPollTimeout,

    #[fail(display = "internal serialization error (this is a bug in amiquip)")]
    InternalSerializationError,

    #[fail(display = "SASL secure/secure-ok exchanges are not supported")]
    SaslSecureNotSupported,

    #[fail(display = "invalid credentials")]
    InvalidCredentials,

    #[fail(display = "missed heartbeats from server")]
    MissedServerHeartbeats,

    #[fail(display = "server closed connection (code={} message={})", _0, _1)]
    ServerClosedConnection(u16, String),

    #[fail(display = "client closed connection")]
    ClientClosedConnection,

    #[fail(display = "server closed channel {} (code={}, message={})", _0, _1, _2)]
    ServerClosedChannel(u16, u16, String),

    #[fail(display = "channel has been closed")]
    ClientClosedChannel,

    #[fail(display = "event loop thread tried to communicate with a nonexistent client")]
    EventLoopClientDropped,

    #[fail(display = "event loop thread died (no further information available)")]
    EventLoopDropped,

    #[fail(display = "channel {} dropped without being cleanly closed", _0)]
    ChannelDropped(u16),

    #[fail(display = "AMQP protocol error - received unexpected frame")]
    FrameUnexpected,

    #[fail(display = "fork failed")]
    ForkFailed,

    #[fail(display = "no more channel ids are available")]
    ExhaustedChannelIds,

    #[fail(display = "requested channel id {} is unavailable", _0)]
    UnavailableChannelId(u16),

    #[fail(display = "internal client exception - received unhandled frames from server")]
    ClientException,

    #[fail(display = "received message for nonexistent channel {}", _0)]
    ReceivedFrameWithBogusChannelId(u16),

    #[fail(display = "I/O thread died unexpectedly: {}", _0)]
    IoThreadPanic(String),

    #[fail(
        display = "server sent duplicate consumer tag for channel {}: {}",
        _0, _1
    )]
    DuplicateConsumerTag(u16, String),

    #[fail(
        display = "received delivery with unknown consumer tag for channel {}: {}",
        _0, _1
    )]
    UnknownConsumerTag(u16, String),

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
        Error { ctx: Arc::new(ctx) }
    }
}
