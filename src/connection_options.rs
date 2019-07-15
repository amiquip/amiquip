use crate::errors::*;
use crate::Sasl;
use amq_protocol::protocol::connection::{Open, Start, StartOk, Tune, TuneOk};
use amq_protocol::protocol::constants::FRAME_MIN_SIZE;
use amq_protocol::types::{AMQPValue, FieldTable};
use std::time::Duration;

/// Options that control the overall AMQP connection.
///
/// `ConnectionOptions` uses the builder pattern. The default settings are equivalent to
///
/// ```rust
/// use amiquip::{Auth, ConnectionOptions};
///
/// # fn default_connection_options() -> ConnectionOptions<Auth> {
/// ConnectionOptions::default()
///     .auth(Auth::default())
///     .virtual_host("/")
///     .locale("en_US")
///     .channel_max(0)
///     .frame_max(0)
///     .heartbeat(60)
///     .connection_timeout(None)
///     .information(None)
/// # }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionOptions<Auth: Sasl> {
    pub(crate) auth: Auth,
    pub(crate) virtual_host: String,
    pub(crate) locale: String,
    pub(crate) channel_max: u16,
    pub(crate) frame_max: u32,
    pub(crate) heartbeat: u16,
    pub(crate) connection_timeout: Option<Duration>,
    information: Option<String>,
}

impl<Auth: Sasl> Default for ConnectionOptions<Auth> {
    // NOTE: If we change this, make sure to change the doc comment above.
    fn default() -> Self {
        ConnectionOptions {
            auth: Auth::default(),
            virtual_host: "/".to_string(),
            locale: "en_US".to_string(),
            channel_max: 0,
            frame_max: 0,
            heartbeat: 60,
            connection_timeout: None,
            information: None,
        }
    }
}

impl<Auth: Sasl> ConnectionOptions<Auth> {
    /// Sets the SASL authentication method.
    pub fn auth(self, auth: Auth) -> Self {
        ConnectionOptions { auth, ..self }
    }

    /// Sets the AMQP virtual host.
    pub fn virtual_host<T: Into<String>>(self, virtual_host: T) -> Self {
        ConnectionOptions {
            virtual_host: virtual_host.into(),
            ..self
        }
    }

    /// Sets the locale. AMQP requires servers support the `en_US` locale (which is also the
    /// default locale for `ConnectionOptions`).
    pub fn locale<T: Into<String>>(self, locale: T) -> Self {
        ConnectionOptions {
            locale: locale.into(),
            ..self
        }
    }

    /// Sets the maximum number of channels that can be opened simultaneously on this connection.
    /// Setting this value to 0 means to let the server choose. If this value is set to a nonzero
    /// value that is different from the server's requested value, the lower of the two will be
    /// used.
    pub fn channel_max(self, channel_max: u16) -> Self {
        ConnectionOptions {
            channel_max,
            ..self
        }
    }

    /// Sets the maximum size in bytes of frames used for this connection.  Setting this value to 0
    /// means to let the server choose. If this value is set to a nonzero value that is different
    /// from the server's requested value, the lower of the two will be used.
    ///
    /// The frame max setting says nothing about the maximum size of messages; messages larger than
    /// `frame_max` bytes will be broken up into multiple frames.
    ///
    /// Note that AMQP specifies a minimum frame_max of 4096; attempting to set a value lower than
    /// this will result in an error when attempting to open the connection.
    pub fn frame_max(self, frame_max: u32) -> Self {
        ConnectionOptions { frame_max, ..self }
    }

    /// Sets the heartbeat interval in seconds. Setting this value to 0 disables heartbeats. If
    /// this value is greater than 0 but different than the server's requested heartbeat interval,
    /// the lower of the two will be used.
    pub fn heartbeat(self, heartbeat: u16) -> Self {
        ConnectionOptions { heartbeat, ..self }
    }

    /// Sets the timeout for the initial TCP connection. If None (the default), there is no
    /// timeout.
    pub fn connection_timeout(self, connection_timeout: Option<Duration>) -> Self {
        ConnectionOptions {
            connection_timeout,
            ..self
        }
    }

    /// Sets the "information" string reported during handshaking to the server. This string
    /// is displayed in the RabbitMQ management interface under "Client properties" of a
    /// connection.
    pub fn information(self, information: Option<String>) -> Self {
        ConnectionOptions {
            information,
            ..self
        }
    }

    pub(crate) fn make_start_ok(&self, start: Start) -> Result<(StartOk, FieldTable)> {
        // helper to search space-separated strings (mechanisms and locales)
        fn server_supports(server: &str, client: &str) -> bool {
            server.split(' ').any(|s| s == client)
        }

        // ensure our requested auth mechanism and locale are available
        let mechanism = self.auth.mechanism();
        if !server_supports(&start.mechanisms, &mechanism) {
            return UnsupportedAuthMechanism {
                available: start.mechanisms.clone(),
                requested: mechanism,
            }
            .fail();
        }
        if !server_supports(&start.locales, &self.locale) {
            return UnsupportedLocale {
                available: start.locales.clone(),
                requested: self.locale.clone(),
            }
            .fail();
        }

        // bundle up info about this crate as client properties
        let mut client_properties = FieldTable::new();
        let mut set_prop = |k: &str, v: String| {
            client_properties.insert(k.to_string(), AMQPValue::LongString(v));
        };
        set_prop("product", crate::built_info::PKG_NAME.to_string());
        set_prop("version", crate::built_info::PKG_VERSION.to_string());
        set_prop(
            "platform",
            format!(
                "{} / {}",
                crate::built_info::CFG_OS,
                crate::built_info::RUSTC_VERSION
            ),
        );
        if let Some(information) = &self.information {
            set_prop("information", information.to_string());
        }
        let mut capabilities = FieldTable::new();
        let mut set_cap = |k: &str| {
            capabilities.insert(k.to_string(), AMQPValue::Boolean(true));
        };
        set_cap("consumer_cancel_notify");
        set_cap("connection.blocked");
        client_properties.insert(
            "capabilities".to_string(),
            AMQPValue::FieldTable(capabilities),
        );

        Ok((
            StartOk {
                client_properties,
                mechanism,
                response: self.auth.response(),
                locale: self.locale.clone(),
            },
            start.server_properties,
        ))
    }

    pub(crate) fn make_tune_ok(&self, tune: Tune) -> Result<TuneOk> {
        fn promote_0_u16(mut val: u16) -> u16 {
            if val == 0 {
                val = u16::max_value();
            }
            val
        }
        fn promote_0_u32(mut val: u32) -> u32 {
            if val == 0 {
                val = u32::max_value();
            }
            val
        }

        let chan_max0 = promote_0_u16(tune.channel_max);
        let chan_max1 = promote_0_u16(self.channel_max);

        let frame_max0 = promote_0_u32(tune.frame_max);
        let frame_max1 = promote_0_u32(self.frame_max);

        let channel_max = u16::min(chan_max0, chan_max1);
        let frame_max = u32::min(frame_max0, frame_max1);
        let heartbeat = u16::min(tune.heartbeat, self.heartbeat);

        if frame_max < u32::from(FRAME_MIN_SIZE) {
            return FrameMaxTooSmall {
                min: u32::from(FRAME_MIN_SIZE),
                requested: frame_max,
            }
            .fail();
        }

        Ok(TuneOk {
            channel_max,
            frame_max,
            heartbeat,
        })
    }

    pub(crate) fn make_open(&self) -> Open {
        Open {
            virtual_host: self.virtual_host.clone(),
            capabilities: "".to_string(), // reserved
            insist: false,                // reserved
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Auth;

    #[test]
    fn channel_max() {
        fn tune_with_channel_max(channel_max: u16) -> Tune {
            Tune {
                channel_max,
                frame_max: 1 << 17,
                heartbeat: 60,
            }
        }

        let options = ConnectionOptions::<Auth>::default().channel_max(0);
        let tune = tune_with_channel_max(0);
        let tune_ok = options.make_tune_ok(tune).unwrap();
        assert_eq!(tune_ok.channel_max, 65535);

        let options = ConnectionOptions::<Auth>::default().channel_max(10);
        let tune = tune_with_channel_max(0);
        let tune_ok = options.make_tune_ok(tune).unwrap();
        assert_eq!(tune_ok.channel_max, 10);

        let options = ConnectionOptions::<Auth>::default().channel_max(0);
        let tune = tune_with_channel_max(10);
        let tune_ok = options.make_tune_ok(tune).unwrap();
        assert_eq!(tune_ok.channel_max, 10);

        let options = ConnectionOptions::<Auth>::default().channel_max(20);
        let tune = tune_with_channel_max(10);
        let tune_ok = options.make_tune_ok(tune).unwrap();
        assert_eq!(tune_ok.channel_max, 10);

        let options = ConnectionOptions::<Auth>::default().channel_max(10);
        let tune = tune_with_channel_max(20);
        let tune_ok = options.make_tune_ok(tune).unwrap();
        assert_eq!(tune_ok.channel_max, 10);
    }

    #[test]
    fn unsupported_auth_mechanism() {
        let options = ConnectionOptions::<Auth>::default();

        let server_mechanisms = "NOTPLAIN SOMETHINGELSE";
        let start = Start {
            version_major: 0,
            version_minor: 9,
            server_properties: FieldTable::new(),
            mechanisms: server_mechanisms.to_string(),
            locales: options.locale.clone(),
        };

        let res = options.make_start_ok(start);
        assert!(res.is_err());
        match res.unwrap_err() {
            Error::UnsupportedAuthMechanism { .. } => (),
            err => panic!("unexpected error {}", err),
        }
    }

    #[test]
    fn unsupported_locale() {
        let server_locales = "en_US es_ES";

        let options = ConnectionOptions::<Auth>::default().locale("nonexistent");

        let start = Start {
            version_major: 0,
            version_minor: 9,
            server_properties: FieldTable::new(),
            mechanisms: options.auth.mechanism().clone(),
            locales: server_locales.to_string(),
        };

        let res = options.make_start_ok(start);
        assert!(res.is_err());
        match res.unwrap_err() {
            Error::UnsupportedLocale { .. } => (),
            err => panic!("unexpected error {}", err),
        }
    }

    #[test]
    fn frame_max_too_small() {
        let frame_max = u32::from(FRAME_MIN_SIZE) - 1;
        let options = ConnectionOptions::<Auth>::default().frame_max(frame_max);

        let tune = Tune {
            channel_max: u16::max_value(),
            frame_max: 1 << 17,
            heartbeat: 60,
        };

        let res = options.make_tune_ok(tune);
        assert!(res.is_err());
        match res.unwrap_err() {
            Error::FrameMaxTooSmall { .. } => (),
            err => panic!("unexpected error {}", err),
        }
    }
}
