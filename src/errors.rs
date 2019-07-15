use snafu::Snafu;
//use std::sync::Arc;
use std::{io, result};
use url::Url;

/// A type alias for handling errors throughout amiquip.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Specific error cases returned by amiquip.
#[derive(Snafu, Debug)]
#[snafu(visibility = "pub(crate)")]
pub enum Error {
    /// URL parsing failed.
    #[snafu(display("could not parse url: {}", source))]
    UrlParseError { source: url::ParseError },

    /// A TLS connection was requested (e.g., via URL), but the amiquip was built without TLS
    /// support.
    #[snafu(display("amiquip built without TLS support"))]
    TlsFeatureNotEnabled,

    /// An insecure URL was supplied to [`Connection::open`](struct.Connection.html#method.open),
    /// which only allows `amqps://...` URLs.
    #[snafu(display(
        "insecure URL passed to method that only allows secure connections: {}",
        url
    ))]
    InsecureUrl { url: Url },

    /// The underlying socket was closed.
    #[snafu(display("underlying socket closed unexpectedly"))]
    UnexpectedSocketClose,

    /// An I/O error occurred while reading from the socket.
    #[snafu(display("I/O error while reading socket: {}", source))]
    IoErrorReadingSocket { source: io::Error },

    /// An I/O error occurred while writing from the socket.
    #[snafu(display("I/O error while writing socket: {}", source))]
    IoErrorWritingSocket { source: io::Error },

    /// We received data that could not be parsed as an AMQP frame.
    #[snafu(display("received malformed data - expected AMQP frame"))]
    MalformedFrame,

    /// Failed to resolve a URL into an IP address (or addresses).
    #[snafu(display("URL did not resolve to an IP address: {}", url))]
    UrlNoSocketAddrs { url: Url },

    /// Error resolving a URL into an IP address (or addresses).
    #[snafu(display("failed to resolve IP address of {}: {}", url, source))]
    ResolveUrlToSocketAddr { url: Url, source: io::Error },

    /// Failed to open TCP connection.
    #[snafu(display("failed to connect to {}: {}", url, source))]
    FailedToConnect { url: Url, source: io::Error },

    /// Failed to set the port on a URL.
    #[snafu(display("cannot specify port for URL {}", url))]
    SpecifyUrlPort { url: Url },

    /// Invalid scheme for URL.
    #[snafu(display("invalid scheme for URL {}: expected `amqp` or `amqps`", url))]
    InvalidUrlScheme { url: Url },

    /// URL is missing domain.
    #[snafu(display("invalid URL (missing domain): {}", url))]
    UrlMissingDomain { url: Url },

    /// URL contains extra path segments.
    #[snafu(display("URL contains extraneous path segments: {}", url))]
    ExtraUrlPathSegments { url: Url },

    /// Could not parse heartbeat parameter of URL.
    #[snafu(display("could not parse heartbeat parameter of URL {}: {}", url, source))]
    UrlParseHeartbeat { url: Url, source: std::num::ParseIntError },

    /// Could not parse channel_max parameter of URL.
    #[snafu(display("could not parse channel_max parameter of URL {}: {}", url, source))]
    UrlParseChannelMax { url: Url, source: std::num::ParseIntError },

    /// Could not parse connection_timeout parameter of URL.
    #[snafu(display("could not parse connection_timeout parameter of URL {}: {}", url, source))]
    UrlParseConnectionTimeout { url: Url, source: std::num::ParseIntError },

    /// Invalid auth mechanism requested in URL.
    #[snafu(display("invalid auth mechanism for URL {}: {} (expected `external`)", url, mechanism))]
    UrlInvalidAuthMechanism { url: Url, mechanism: String },

    /// Unsupported URL parameter.
    #[snafu(display("unsupported parameter in URL {}: {}", url, parameter))]
    UrlUnsupportedParameter { url: Url, parameter: String },

    /// Could not create mio Poll handle.
    #[snafu(display("failed to create polling handle: {}", source))]
    CreatePollHandle { source: io::Error },

    /// Could not register descriptor with Poll handle.
    #[snafu(display("failed to register object with polling handle: {}", source))]
    RegisterWithPollHandle { source: io::Error },

    /// Could not register descriptor with Poll handle.
    #[snafu(display("failed to deregister object with polling handle: {}", source))]
    DeregisterWithPollHandle { source: io::Error },

    /// Failed to poll mio Poll handle.
    #[snafu(display("failed to poll: {}", source))]
    FailedToPoll { source: io::Error },

    /// The TLS handshake failed.
    #[cfg(feature = "native-tls")]
    #[snafu(display("TLS handshake failed: {}", source))]
    TlsHandshake { source: native_tls::Error },

    /// Error from underlying TLS implementation.
    #[cfg(feature = "native-tls")]
    #[snafu(display("could not create TLS connector: {}", source))]
    CreateTlsConnector { source: native_tls::Error },

    /// The server does not support the requested auth mechanism.
    #[snafu(display(
        "requested auth mechanism unavailable (available = {}, requested = {})",
        available,
        requested
    ))]
    UnsupportedAuthMechanism {
        available: String,
        requested: String,
    },

    /// The server does not support the requested locale.
    #[snafu(display(
        "requested locale unavailable (available = {}, requested = {})",
        available,
        requested
    ))]
    UnsupportedLocale {
        available: String,
        requested: String,
    },

    /// The requested frame size is smaller than the minimum required by AMQP.
    #[snafu(display(
        "requested frame max is too small (min = {}, requested = {})",
        min,
        requested
    ))]
    FrameMaxTooSmall { min: u32, requested: u32 },

    /// Timeout occurred while performing the initial TCP connection.
    #[snafu(display("timeout occurred while waiting for TCP connection"))]
    ConnectionTimeout,

    /// The server requested a Secure/Secure-Ok exchange, which are currently unsupported.
    #[snafu(display("SASL secure/secure-ok exchanges are not supported"))]
    SaslSecureNotSupported,

    /// The supplied authentication credentials were not accepted by the server.
    #[snafu(display("invalid credentials"))]
    InvalidCredentials,

    /// The server missed too many successive heartbeats.
    #[snafu(display("missed heartbeats from server"))]
    MissedServerHeartbeats,

    /// The server closed the connection with the given reply code and text.
    #[snafu(display("server closed connection (code={} message={})", code, message))]
    ServerClosedConnection { code: u16, message: String },

    /// The client closed the connection.
    #[snafu(display("client closed connection"))]
    ClientClosedConnection,

    /// The server closed the given channel with the given reply code and text.
    #[snafu(display(
        "server closed channel {} (code={}, message={})",
        channel_id,
        code,
        message
    ))]
    ServerClosedChannel {
        channel_id: u16,
        code: u16,
        message: String,
    },

    /// The client closed the channel.
    #[snafu(display("channel has been closed"))]
    ClientClosedChannel,

    /// The I/O loop attempted to send a message to a caller that did not exist. This
    /// indicates either a bug in amiquip or a connection that is in a bad state and in the process
    /// of tearing down.
    #[snafu(display("i/o loop thread tried to communicate with a nonexistent client"))]
    EventLoopClientDropped,

    /// The I/O loop has dropped the sending side of a channel, typically because it has exited due
    /// to another error.
    #[snafu(display("i/o loop dropped sending side of a channel"))]
    EventLoopDropped,

    /// We received a valid AMQP frame but not one we expected; e.g., receiving an incorrect
    /// response to an AMQP method call.
    #[snafu(display("AMQP protocol error - received unexpected frame"))]
    FrameUnexpected,

    /// Forking the I/O thread failed.
    #[snafu(display("fork failed: {}", source))]
    ForkFailed { source: io::Error },

    /// No more channels can be opened because there are already
    /// [`channel_max`](struct.ConnectionOptions.html#method.channel_max) channels open.
    #[snafu(display("no more channel ids are available"))]
    ExhaustedChannelIds,

    /// An explicit channel ID was requested, but that channel is unavailable for use (e.g.,
    /// because there is another open channel with the same ID).
    #[snafu(display("requested channel id ({}) is unavailable", channel_id))]
    UnavailableChannelId { channel_id: u16 },

    /// The client sent an AMQP exception to the server and closed the connection.
    #[snafu(display("internal client exception - received unhandled frames from server"))]
    ClientException,

    /// The server sent frames for a channel ID we don't know about.
    #[snafu(display("received message for nonexistent channel {}", channel_id))]
    ReceivedFrameWithBogusChannelId { channel_id: u16 },

    /// The I/O thread panicked.
    #[snafu(display("I/O thread panicked"))]
    IoThreadPanic,

    /// The server sent us a consumer tag that is equal to another consumer tag we already have on
    /// the same channel.
    #[snafu(display(
        "server sent duplicate consumer tag for channel {}: {}",
        channel_id,
        consumer_tag
    ))]
    DuplicateConsumerTag { channel_id: u16, consumer_tag: String },

    /// The server sent us a [`Delivery`](struct.Delivery.html) for a channel we don't know about.
    #[snafu(display(
        "received delivery with unknown consumer tag for channel {}: {}",
        channel_id,
        consumer_tag
    ))]
    UnknownConsumerTag { channel_id: u16, consumer_tag: String },

    #[doc(hidden)]
    #[snafu(display("invalid error case"))]
    __Nonexhaustive,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn our_error_impls_std_error() {
        fn is_err<T: std::error::Error>() {}
        is_err::<Error>();
    }
}
