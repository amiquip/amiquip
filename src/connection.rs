use crate::connection_options::ConnectionOptions;
use crate::io_loop::{Channel0Handle, IoLoop};
use crate::{Channel, ErrorKind, FieldTable, IoStream, Result, Sasl};
use crossbeam_channel::Receiver;
use log::debug;
use std::thread::JoinHandle;

#[cfg(feature = "native-tls")]
use crate::TlsConnector;

/// Asynchronous notifications sent by the server when it temporarily blocks a connection,
/// typically due to a resource alarm.
///
/// Use [`Connection::listen_for_connection_blocked`](struct.Connection.html#method.listen_for_connection_blocked)
/// to receive these notifications.
#[derive(Debug, Clone)]
pub enum ConnectionBlockedNotification {
    /// The connection has been blocked for the given reason.
    Blocked(String),

    /// The connection has been unblocked.
    Unblocked,
}

/// Tuning parameters for the amiquip client.
///
/// The options are solely used to control local behavior of the client. They are not part of the
/// AMQP spec and are not communicated with the server in any way. For options that configure the
/// AMQP connection, see [`ConnectionOptions`](struct.ConnectionOptions.html).
#[derive(Debug, Clone)]
pub struct ConnectionTuning {
    /// Set the bound used when creating `mio_extras::channel::sync_channel()` channels for sending
    /// messages to the connection's I/O thread. The default value for this field is 16.
    ///
    /// See the discussion on [connection tuning](struct.Connection.html#tuning) for more
    /// information.
    pub mem_channel_bound: usize,

    /// Set the maximum amount of data in bytes that the I/O thread will buffer before it begins to
    /// apply backpressure on clients by not reading from their channels. If the high water mark is
    /// reached, the I/O loop will not resume reading from client channels until the amount of
    /// buffered data drops below
    /// [`buffered_writes_low_water`](struct.ConnectionTuning.html#structfield.buffered_writes_low_water)
    /// bytes. The default value for this field is 16 MiB.
    ///
    /// See the discussion on [connection tuning](struct.Connection.html#tuning) for more
    /// information.
    pub buffered_writes_high_water: usize,

    /// Set the low water mark for the I/O thread to resume reading from client channels. The
    /// default value for this field is 0 bytes.
    ///
    /// See the discussion on [connection tuning](struct.Connection.html#tuning) for more
    /// information.
    pub buffered_writes_low_water: usize,
}

impl Default for ConnectionTuning {
    fn default() -> Self {
        // NOTE: If we change this, make sure to change the docs above.
        ConnectionTuning {
            mem_channel_bound: 16,
            buffered_writes_high_water: 16 << 20,
            buffered_writes_low_water: 0,
        }
    }
}

impl ConnectionTuning {
    /// Set the [channel memory bound](#structfield.mem_channel_bound).
    pub fn mem_channel_bound(self, mem_channel_bound: usize) -> Self {
        ConnectionTuning {
            mem_channel_bound,
            ..self
        }
    }

    /// Set the [high water mark](#structfield.buffered_writes_high_water) for buffered data to be
    /// written to the underlying stream.
    pub fn buffered_writes_high_water(self, buffered_writes_high_water: usize) -> Self {
        ConnectionTuning {
            buffered_writes_high_water,
            ..self
        }
    }

    /// Set the [low water mark](#structfield.buffered_writes_low_water) for buffered data to be
    /// written to the underlying stream.
    pub fn buffered_writes_low_water(self, buffered_writes_low_water: usize) -> Self {
        ConnectionTuning {
            buffered_writes_low_water,
            ..self
        }
    }
}

/// Handle for an AMQP connection.
///
/// Opening an AMQP connection creates at least one thread - the I/O thread which is responsible
/// for driving the underlying socket and communicating back with the `Connection` handle and any
/// open [`Channel`](struct.Channel.html)s. An additional thread may be created to handle heartbeat
/// timers; this is an implementation detail that may not be true on all platforms.
///
/// Closing the connection (or dropping it) will attempt to `join` on the I/O thread. This can
/// block; see the [`close`](#method.close) for notes.
///
/// # Tuning
///
/// Opening a connection requires specifying [`ConnectionTuning`](struct.ConnectionTuning.html)
/// parameters. These control resources and backpressure between the I/O loop thread and its
/// `Connection` handle and open channels. This structure has three fields:
///
/// * [`mem_channel_bound`](struct.ConnectionTuning.html#structfield.mem_channel_bound) controls
/// the channel size for communication from a `Connection` and its channels into the I/O thread.
/// Setting this to 0 means all communications from a handle into the I/O thread will block until
/// the I/O thread is ready to receive the message; setting it to something higher than 0 means
/// messages into the I/O thread will be buffered and will not block. Note that many methods are
/// synchronous in the AMQP sense (e.g., [`Channel::queue_declare`](struct.Channel.html)) and will
/// see no benefit from buffering, as they must wait for a response from the I/O thread before they
/// return. This bound may improve performance for asynchronous messages, but see the next two
/// fields.
///
/// * [`buffered_writes_high_water`](struct.ConnectionTuning.html#structfield.buffered_writes_high_water)
/// and
/// [`buffered_writes_low_water`](struct.ConnectionTuning.html#structfield.buffered_writes_low_water)
/// control how much outgoing data the I/O thread is willing to buffer up before it starts
/// applying backpressure to the in-memory channels that `Connection` and its AMQP channels use to
/// send it messages. This prevents unbounded memory growth in the I/O thread if local clients are
/// attempting to send data faster than the AMQP server is able to receive it. If the I/O thread
/// buffers up more than `buffered_writes_high_water` bytes of data, it will stop polling channels
/// until the amount of data drops below `buffered_writes_low_water`. These values combine with
/// `mem_channel_bound` to apply two different kinds of buffering and backpressure.
///
/// For example, suppose a connection is used exclusively for publishing data, and it is attempting
/// to publish data as quickly as possible. It sets `mem_channel_bound` to 16,
/// `buffered_writes_high_water` to 16 MiB, and `buffered_writes_low_water` to 1 MiB. Once it has
/// published enough data that the I/O thread crosses the 16 MiB mark for buffered outgoing data,
/// the publisher will be able to send 16 more messages into the I/O thread (note that this does
/// necessarily mean full data messages, as AMQP messages will be broken up into muliple framed
/// messages internally), at which point additional sends into the I/O thread will block. Once the
/// I/O thread's buffered data amount drops below 1 MiB, it will resume polling the in-memory
/// channel, pulling from the 16 buffered messages, freeing up space and unblocking the publisher.
///
/// # Thread Safety
///
/// `Connection` implements both `Send` and `Sync`; however, its most useful method
/// ([`open_channel`](#method.open_channel)) takes `&mut self`, which requires unique ownership.
/// The channels returned by [`open_channel`](#method.open_channel) themselves implement `Send` but
/// not `Sync`. After opening a connection one thread, you are free to create any number of
/// channels and send them to other threads for use. However, they are all tied back to the
/// original connection; when it is closed or dropped, future operations on them will fail.
#[derive(Debug)]
pub struct Connection {
    join_handle: Option<JoinHandle<Result<()>>>,
    channel0: Channel0Handle,
    server_properties: FieldTable,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.close_impl();
    }
}

impl Connection {
    /// Calls [`open_tuned`](#method.open_tuned) with default
    /// [`ConnectionTuning`](struct.ConnectionTuning.html) settings.
    #[cfg(feature = "native-tls")]
    pub fn open(url: &str) -> Result<Connection> {
        Self::open_tuned(url, ConnectionTuning::default())
    }

    /// Equivalent to [`insecure_open_tuned`](#method.insecure_open_tuned), except
    /// only secure URLs (`amqps://...`) are allowed. Calling this method with an insecure
    /// (`amqp://...`) URL will return an error with [kind
    /// `InsecureUrl`](enum.ErrorKind.html#variant.InsecureUrl).
    #[cfg(feature = "native-tls")]
    pub fn open_tuned(url: &str, tuning: ConnectionTuning) -> Result<Connection> {
        self::amqp_url::open(url, tuning, false)
    }

    /// Calls [`insecure_open_tuned`](#method.insecure_open_tuned) with default
    /// [`ConnectionTuning`](struct.ConnectionTuning.html) settings.
    pub fn insecure_open(url: &str) -> Result<Connection> {
        Self::insecure_open_tuned(url, ConnectionTuning::default())
    }

    /// Open an AMQP connection from an `amqp://...` or `amqps://...` URL. Mostly follows the
    /// [RabbitMQ URI Specification](https://www.rabbitmq.com/uri-spec.html), with the following
    /// differences:
    ///
    /// * If the username and password are omitted, a username/password of `guest`/`guest` is used.
    /// * There is no way to specify a vhost of `""` (the empty string). Passing a URL without a
    /// vhost will result in an open request to the default AMQP vhost of `/`.
    ///
    /// A subset of the [RabbitMQ query
    /// parameters](https://www.rabbitmq.com/uri-query-parameters.html) are supported:
    ///
    /// * `heartbeat`
    /// * `connection_timeout`
    /// * `channel_max`
    /// * `auth_mechanism` (partial); the only allowed value is `external`, and if this query
    /// parameter is given any username or password on the URL will be ignored.
    ///
    /// Using `amqps` URLs requires amiquip to be built with the `native-tls` feature. The
    /// TLS-related RabbitMQ query parameters are not supported; use
    /// [`open_tls_stream`](#method.open_tls_stream) with a configured `TlsConnector` if you need
    /// control over the TLS parameters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use amiquip::{Auth, Connection, ConnectionOptions, ConnectionTuning, Result};
    /// use std::time::Duration;
    ///
    /// // Examples below assume a helper function to open a TcpStream from an address string with
    /// // a signature like this is available:
    /// //   fn tcp_stream(addr: &str) -> Result<mio::net::TcpStream>;
    /// # use mio::net::TcpStream;
    /// # fn tcp_stream(addr: &str) -> Result<TcpStream> {
    /// #     Ok(TcpStream::connect(&addr.parse().unwrap()).unwrap())
    /// # }
    ///
    /// # fn open_examples() -> Result<()> {
    /// // Empty amqp URL is equivalent to default options; handy for initial debugging and
    /// // development.
    /// let conn1 = Connection::insecure_open("amqp://")?;
    /// let conn1 = Connection::insecure_open_stream(
    ///     tcp_stream("localhost:5672")?,
    ///     ConnectionOptions::<Auth>::default(),
    ///     ConnectionTuning::default(),
    /// )?;
    ///
    /// // All possible options specified in the URL except auth_mechanism=external (which would
    /// // override the username and password).
    /// let conn3 = Connection::insecure_open(
    ///     "amqp://user:pass@example.com:12345/myvhost?heartbeat=30&channel_max=1024&connection_timeout=10000",
    /// )?;
    /// let conn3 = Connection::insecure_open_stream(
    ///     tcp_stream("example.com:12345")?,
    ///     ConnectionOptions::default()
    ///         .auth(Auth::Plain {
    ///             username: "user".to_string(),
    ///             password: "pass".to_string(),
    ///         })
    ///         .heartbeat(30)
    ///         .channel_max(1024)
    ///         .connection_timeout(Some(Duration::from_millis(10_000))),
    ///     ConnectionTuning::default(),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insecure_open_tuned(url: &str, tuning: ConnectionTuning) -> Result<Connection> {
        self::amqp_url::open(url, tuning, true)
    }

    /// Open an encrypted AMQP connection on a stream (typically a `mio::net::TcpStream`)
    /// using the provided [`TlsConnector`](struct.TlsConnector.html).
    #[cfg(feature = "native-tls")]
    pub fn open_tls_stream<Auth: Sasl, C: Into<TlsConnector>, S: IoStream>(
        connector: C,
        domain: &str,
        stream: S,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let stream = connector.into().connect(domain, stream)?;
        let io_loop = IoLoop::new(tuning)?;
        let (join_handle, server_properties, channel0) = io_loop.start_tls(stream, options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
            server_properties,
        })
    }

    /// Open an AMQP connection on an insecure stream (typically a `mio::net::TcpStream`).
    ///
    /// Consider using [`open_tls_stream`](#method.open_tls_stream) instead, unless you are sure an
    /// insecure connection is acceptable (e.g., you're connecting to `localhost`).
    pub fn insecure_open_stream<Auth: Sasl, S: IoStream>(
        stream: S,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let io_loop = IoLoop::new(tuning)?;
        let (join_handle, server_properties, channel0) = io_loop.start(stream, options)?;
        Ok(Connection {
            join_handle: Some(join_handle),
            channel0,
            server_properties,
        })
    }

    /// Get the properties reported by the server during the initial AMQP handshake. This typically
    /// includes string fields like:
    ///
    /// * `cluster_name`
    /// * `copyright`
    /// * `platform`
    /// * `product`
    /// * `version`
    ///
    /// It also typically includes a nested `FieldTable` under the key `capabilities` that
    /// describes extensions supported by the server. Relevant capabilities to amiquip include:
    ///
    /// * `basic.nack` - required to use [`Delivery::nack`](struct.Delivery.html#method.nack)
    /// * `connection.blocked` - required for
    /// [`listen_for_connection_blocked`](#method.listen_for_connection_blocked) to receive
    /// notifications.
    /// * `consumer_cancel_notify` - if present, the server may cancel consumers (e.g., if its
    /// queue is deleted)
    /// * `exchange_exchange_bindings` - required for
    /// [`Exchange::bind_to_source`](struct.Exchange.html#method.bind_to_source) and related
    /// exchange-to-exchange binding methods.
    pub fn server_properties(&self) -> &FieldTable {
        &self.server_properties
    }

    /// Open an AMQP channel on this connection. If `channel_id` is `Some`, the returned channel
    /// will have the request ID if possible, or an error will be returned if that channel ID not
    /// available. If `channel_id` is `None`, the connection will choose an available channel ID
    /// (unless all available channel IDs are exhausted, in which case this method will return
    /// [`ErrorKind::ExhaustedChannelIds`](enum.ErrorKind.html#variant.ExhaustedChannelIds).
    ///
    /// The returned channel is tied to this connection in a logical sense but not in any ownership
    /// way. For example, it may be passed to a thread for use. However, closing (or dropping,
    /// which also closes) this connection will cause operations on all opened channels to fail.
    pub fn open_channel(&mut self, channel_id: Option<u16>) -> Result<Channel> {
        let handle = self.channel0.open_channel(channel_id)?;
        Ok(Channel::new(handle))
    }

    /// Open a crossbeam channel to receive [connection blocked
    /// notifications](https://www.rabbitmq.com/connection-blocked.html) from the server.
    ///
    /// There can be only one connection blocked listener. If you call this method a second (or
    /// more) time, the I/O thread will drop the sending side of previously returned channels.
    ///
    /// Dropping the `Receiver` returned by this method is harmless. If the I/O loop receives a
    /// connection blocked notification and there is no listener registered or the
    /// previously-registered listener has been dropped, it will discard the notification.
    pub fn listen_for_connection_blocked(
        &mut self,
    ) -> Result<Receiver<ConnectionBlockedNotification>> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.channel0.set_blocked_tx(tx)?;
        Ok(rx)
    }

    /// Close this connection. This method will join on the I/O thread handle, so it may block for
    /// a nontrivial amount of time. If heartbeats are not enabled, it is possible this method
    /// could block indefinitely waiting for the server to respond to our close request.
    ///
    /// Closing a connection will cause future operations on any channels opened on this connection
    /// to fail.
    ///
    /// If the I/O thread panics, this method will return an error with its
    /// [kind](struct.Error.html#method.kind) set to
    /// [`ErrorKind::IoThreadPanic`](enum.ErrorKind.html#variant.IoThreadPanic). (Note - the I/O
    /// thread _should not_ panic. If it does, please [file an
    /// issue](https://github.com/jgallagher/amiquip/issues).) For this reason, applications that
    /// want more detail about errors to separate the use of the connection from closing it. For
    /// example:
    ///
    /// ```rust
    /// use amiquip::{Connection, Result, ErrorKind};
    ///
    /// fn use_connection(connection: &mut Connection) -> Result<()> {
    ///     // ...do all the things...
    ///     # Ok(())
    /// }
    ///
    /// fn run_connection(mut connection: Connection) -> Result<()> {
    ///     // capture any errors from using the connection, but don't immediately return.
    ///     let use_result = use_connection(&mut connection);
    ///
    ///     // close the connection; if this fails, it is probably the most useful error
    ///     // message (since it may indicate an I/O thread panic or other fatal error).
    ///     connection.close()?;
    ///
    ///     // if close completed succsssfully, return `use_result`, which might still contain
    ///     // some other kind of error.
    ///     use_result
    /// }
    /// ```
    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        if let Some(join_handle) = self.join_handle.take() {
            debug!("closing connection");
            // capture close result, but don't return it yet (if the I/O thread panicked,
            // for example, this will fail but we want to capture the panic thread when
            // we join the thread momentarily).
            let close_result = self.channel0.close_connection();

            // wait for the I/O thread to end, and return its panic or error.
            join_handle.join().map_err(|_| ErrorKind::IoThreadPanic)??;

            // join ended cleanly; return the result of closing the connection.
            close_result
        } else {
            // no join handle left - someone already took it, which is only possible
            // if we're being called from Drop after someone called close(), and drop
            // doesn't care what we return.
            Ok(())
        }
    }
}

mod amqp_url {
    use super::*;
    use crate::{Auth, Error};
    use failure::ResultExt;
    use mio::net::TcpStream;
    use std::borrow::Cow;
    use std::net::ToSocketAddrs;
    use std::time::Duration;
    use url::{percent_encoding, Url};

    pub fn open(url: &str, tuning: ConnectionTuning, allow_insecure: bool) -> Result<Connection> {
        let mut url = Url::parse(url)?;
        let scheme = populate_host_and_port(&mut url)?;
        let options = decode(&url)?;

        match scheme {
            Scheme::Amqp => {
                if allow_insecure {
                    open_amqp(url, options, tuning)
                } else {
                    Err(ErrorKind::InsecureUrl)?
                }
            }
            Scheme::Amqps => open_amqps(url, options, tuning),
        }
    }

    fn open_amqp(
        url: Url,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let mut last_err = Error::from(ErrorKind::InvalidUrl(url.clone()));
        for addr in url.to_socket_addrs().context(ErrorKind::Io)? {
            let result = TcpStream::connect(&addr)
                .context(ErrorKind::Io)
                .map_err(Error::from)
                .and_then(|stream| {
                    Connection::insecure_open_stream(stream, options.clone(), tuning.clone())
                });
            match result {
                Ok(connection) => return Ok(connection),
                Err(err) => {
                    last_err = err;
                }
            }
        }
        Err(last_err)
    }

    #[cfg(not(feature = "native-tls"))]
    fn open_amqps(_: Url, _: ConnectionOptions<Auth>, _: ConnectionTuning) -> Result<Connection> {
        Err(ErrorKind::TlsFeatureNotEnabled)?
    }

    #[cfg(feature = "native-tls")]
    fn open_amqps(
        url: Url,
        options: ConnectionOptions<Auth>,
        tuning: ConnectionTuning,
    ) -> Result<Connection> {
        let mut last_err = Error::from(ErrorKind::InvalidUrl(url.clone()));
        let connector =
            native_tls::TlsConnector::new().map_err(|e| ErrorKind::TlsError(format!("{}", e)))?;
        let domain = match url.domain() {
            Some(domain) => domain,
            None => return Err(last_err),
        };
        for addr in url.to_socket_addrs().context(ErrorKind::Io)? {
            let result = TcpStream::connect(&addr)
                .context(ErrorKind::Io)
                .map_err(|err| Error::from(err))
                .and_then(|stream| {
                    Connection::open_tls_stream(
                        connector.clone(),
                        domain,
                        stream,
                        options.clone(),
                        tuning.clone(),
                    )
                });
            match result {
                Ok(connection) => return Ok(connection),
                Err(err) => {
                    last_err = err;
                }
            }
        }
        Err(last_err)
    }

    #[derive(Debug, PartialEq)]
    enum Scheme {
        Amqp,
        Amqps,
    }

    fn populate_host_and_port(url: &mut Url) -> Result<Scheme> {
        if !url.has_host() || url.host_str() == Some("") {
            url.set_host(Some("localhost"))?;
        }
        match url.scheme() {
            "amqp" => {
                url.set_port(Some(url.port().unwrap_or(5672)))
                    .map_err(|_| ErrorKind::InvalidUrl(url.clone()))?;
                Ok(Scheme::Amqp)
            }
            "amqps" => {
                url.set_port(Some(url.port().unwrap_or(5671)))
                    .map_err(|_| ErrorKind::InvalidUrl(url.clone()))?;
                Ok(Scheme::Amqps)
            }
            _ => Err(ErrorKind::InvalidUrl(url.clone()))?,
        }
    }

    fn decode(url: &Url) -> Result<ConnectionOptions<Auth>> {
        fn percent_decode(s: &str) -> Cow<str> {
            let s = percent_encoding::percent_decode(s.as_bytes());
            s.decode_utf8_lossy()
        }
        let invalid_url = || ErrorKind::InvalidUrl(url.clone());

        let mut options = ConnectionOptions::default();
        if let Some(mut path_segments) = url.path_segments() {
            // first unwrap guaranteed to be safe by docs for url
            let vhost = path_segments.next().unwrap();

            // this is tricky; rabbit docs suggest "amqp://" should have a
            // vhost of None (therefore we should get the default vhost of "/"), but
            // "amqp://host/" should have a vhost of Some(""). But we can't tell
            // the difference between these two with the url lib, so we'll just toss
            // out the latter. We now have no way of specifying a vhost of "".
            if vhost != "" {
                options = options.virtual_host(percent_decode(vhost));
            }

            // make sure there are no other path segments
            if path_segments.next().is_some() {
                return Err(invalid_url())?;
            }
        }

        if url.username() != "" || url.password().is_some() {
            let username = match url.username() {
                "" => "guest",
                other => other,
            };
            let auth = Auth::Plain {
                username: percent_decode(username).to_string(),
                password: percent_decode(url.password().unwrap_or("guest")).to_string(),
            };
            options = options.auth(auth);
        }

        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "heartbeat" => {
                    let v = v.parse::<u16>().map_err(|_| invalid_url())?;
                    options = options.heartbeat(v);
                }
                "channel_max" => {
                    let v = v.parse::<u16>().map_err(|_| invalid_url())?;
                    options = options.channel_max(v);
                }
                "connection_timeout" => {
                    let v = v.parse::<u64>().map_err(|_| invalid_url())?;
                    options = options.connection_timeout(Some(Duration::from_millis(v)));
                }
                "auth_mechanism" => {
                    if v == "external" {
                        options = options.auth(Auth::External);
                    } else {
                        return Err(invalid_url())?;
                    }
                }
                _ => return Err(invalid_url())?,
            }
        }

        Ok(options)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn decode_s(s: &str) -> Result<ConnectionOptions<Auth>> {
            decode(&Url::parse(s).unwrap())
        }

        #[test]
        #[cfg(feature = "native-tls")]
        fn open_rejects_amqp_urls() {
            let result = Connection::open("amqp://localhost/");
            assert_eq!(*result.unwrap_err().kind(), ErrorKind::InsecureUrl);
        }

        #[test]
        fn empty_default() {
            let options = decode_s("amqp://").unwrap();
            assert_eq!(options, ConnectionOptions::default());
            let options = decode_s("amqps://").unwrap();
            assert_eq!(options, ConnectionOptions::default());
        }

        #[test]
        fn vhost() {
            let options = decode_s("amqp:///vhost").unwrap();
            assert_eq!(options, ConnectionOptions::default().virtual_host("vhost"));
            let options = decode_s("amqp:///v%2fhost").unwrap();
            assert_eq!(options, ConnectionOptions::default().virtual_host("v/host"));
            assert!(decode_s("amqp:///vhost/nonescapedslash").is_err());
        }

        #[test]
        fn user_pass() {
            let options = decode_s("amqp://user:pass@/").unwrap();
            assert_eq!(
                options,
                ConnectionOptions::default().auth(Auth::Plain {
                    username: "user".to_string(),
                    password: "pass".to_string()
                })
            );
            let options = decode_s("amqp://user%61:pass%62@/").unwrap();
            assert_eq!(
                options,
                ConnectionOptions::default().auth(Auth::Plain {
                    username: "usera".to_string(),
                    password: "passb".to_string()
                })
            );
        }

        #[test]
        fn heartbeat() {
            let options = decode_s("amqp://?heartbeat=13").unwrap();
            assert_eq!(options, ConnectionOptions::default().heartbeat(13));
        }

        #[test]
        fn channel_max() {
            let options = decode_s("amqp://?channel_max=13").unwrap();
            assert_eq!(options, ConnectionOptions::default().channel_max(13));
        }

        #[test]
        fn connection_timeout() {
            let options = decode_s("amqp://?connection_timeout=13").unwrap();
            assert_eq!(
                options,
                ConnectionOptions::default().connection_timeout(Some(Duration::from_millis(13)))
            );
        }

        #[test]
        fn auth_mechanism() {
            let options = decode_s("amqp://?auth_mechanism=external").unwrap();
            assert_eq!(options, ConnectionOptions::default().auth(Auth::External));
        }

        #[test]
        fn populate_host() {
            let mut url = Url::parse("amqp://").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.host_str(), Some("localhost"));

            let mut url = Url::parse("amqp://:35").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.host_str(), Some("localhost"));

            let mut url = Url::parse("amqp://foo.com").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.host_str(), Some("foo.com"));
        }

        #[test]
        fn populate_port() {
            let mut url = Url::parse("amqp://").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.port(), Some(5672));

            let mut url = Url::parse("amqps://").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.port(), Some(5671));

            let mut url = Url::parse("amqp://:35").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.port(), Some(35));

            let mut url = Url::parse("amqps://:35").unwrap();
            populate_host_and_port(&mut url).unwrap();
            assert_eq!(url.port(), Some(35));
        }
    }
}
