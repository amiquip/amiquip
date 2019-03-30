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
    /// See the discussion on [amiquip's design details](index.html#design-details) for more
    /// information.
    pub mem_channel_bound: usize,

    /// Set the maximum amount of data in bytes that the I/O thread will buffer before it begins to
    /// apply backpressure on clients by not reading from their channels. If the high water mark is
    /// reached, the I/O loop will not resume reading from client channels until the amount of
    /// buffered data drops below
    /// [`buffered_writes_low_water`](struct.ConnectionTuning.html#structfield.buffered_writes_low_water)
    /// bytes. The default value for this field is 16 MiB.
    ///
    /// See the discussion on [amiquip's design details](index.html#design-details) for more
    /// information.
    pub buffered_writes_high_water: usize,

    /// Set the low water mark for the I/O thread to resume reading from client channels. See
    /// [`buffered_writes_high_water`](struct.ConnectionTuning.html#structfield.buffered_writes_high_water)
    /// above. The default value for this field is 0 bytes.
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
    pub fn open_url(url: &str, tuning: ConnectionTuning) -> Result<Connection> {
        self::amqp_url::open(url, tuning)
    }

    pub fn open<Auth: Sasl, S: IoStream>(
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

    #[cfg(feature = "native-tls")]
    pub fn open_tls<Auth: Sasl, C: Into<TlsConnector>, S: IoStream>(
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

    pub fn server_properties(&self) -> &FieldTable {
        &self.server_properties
    }

    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    pub fn listen_for_connection_blocked(
        &mut self,
    ) -> Result<Receiver<ConnectionBlockedNotification>> {
        let (tx, rx) = crossbeam_channel::unbounded();
        self.channel0.set_blocked_tx(tx)?;
        Ok(rx)
    }

    fn close_impl(&mut self) -> Result<()> {
        if let Some(join_handle) = self.join_handle.take() {
            debug!("closing connection");
            self.channel0.close_connection()?;
            join_handle
                .join()
                .map_err(|err| ErrorKind::IoThreadPanic(format!("{:?}", err)))?
        } else {
            // no join handle left - someone already took it, which is only possible
            // if we're being called from Drop after someone called close(), and drop
            // doesn't care what we return.
            Ok(())
        }
    }

    pub fn open_channel(&mut self, channel_id: Option<u16>) -> Result<Channel> {
        let handle = self.channel0.open_channel(channel_id)?;
        Ok(Channel::new(handle))
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

    pub fn open(url: &str, tuning: ConnectionTuning) -> Result<Connection> {
        let mut url = Url::parse(url)?;
        let scheme = populate_host_and_port(&mut url)?;
        let options = decode(&url)?;

        match scheme {
            Scheme::Amqp => open_amqp(url, options, tuning),
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
                .map_err(|err| Error::from(err))
                .and_then(|stream| Connection::open(stream, options.clone(), tuning.clone()));
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
                    Connection::open_tls(
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
