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
#[derive(Clone, Debug, Eq, PartialEq, Fail)]
pub enum ErrorKind {
    #[fail(display = "underlying socket closed unexpectedly")]
    UnexpectedSocketClose,

    #[fail(display = "received malformed data")]
    ReceivedMalformed,

    #[fail(display = "I/O error")]
    Io,

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
