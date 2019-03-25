use crate::auth::Sasl;
use crate::{ErrorKind, Result};
use amq_protocol::protocol::connection::{Open, Start, StartOk, Tune, TuneOk};
use amq_protocol::protocol::constants::FRAME_MIN_SIZE;
use amq_protocol::types::{AMQPValue, FieldTable};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ConnectionOptions<Auth: Sasl> {
    pub(crate) auth: Auth,
    pub(crate) virtual_host: String,
    pub(crate) locale: String,
    pub(crate) channel_max: u16,
    pub(crate) frame_max: u32,
    pub(crate) heartbeat: u16,
    pub(crate) poll_timeout: Option<Duration>, // TODO remove
    pub(crate) io_thread_channel_bound: usize, // TODO remove
}

impl<Auth: Sasl> Default for ConnectionOptions<Auth> {
    fn default() -> Self {
        ConnectionOptions {
            auth: Auth::default(),
            virtual_host: "/".to_string(),
            locale: "en_US".to_string(),
            channel_max: u16::max_value(),
            frame_max: 1 << 17,
            heartbeat: 60,
            poll_timeout: None,

            // TODO what is a reasonable default here?
            io_thread_channel_bound: 8,
        }
    }
}

impl<Auth: Sasl> ConnectionOptions<Auth> {
    pub fn auth(self, auth: Auth) -> Self {
        ConnectionOptions { auth, ..self }
    }

    pub fn virtual_host<T: Into<String>>(self, virtual_host: T) -> Self {
        ConnectionOptions {
            virtual_host: virtual_host.into(),
            ..self
        }
    }

    pub fn locale<T: Into<String>>(self, locale: T) -> Self {
        ConnectionOptions {
            locale: locale.into(),
            ..self
        }
    }

    pub fn channel_max(self, channel_max: u16) -> Self {
        ConnectionOptions {
            channel_max,
            ..self
        }
    }

    pub fn frame_max(self, frame_max: u32) -> Self {
        ConnectionOptions { frame_max, ..self }
    }

    pub fn heartbeat(self, heartbeat: u16) -> Self {
        ConnectionOptions { heartbeat, ..self }
    }

    pub fn poll_timeout(self, poll_timeout: Option<Duration>) -> Self {
        ConnectionOptions {
            poll_timeout,
            ..self
        }
    }

    pub fn io_thread_channel_bound(self, io_thread_channel_bound: usize) -> Self {
        ConnectionOptions {
            io_thread_channel_bound,
            ..self
        }
    }

    pub(crate) fn make_start_ok(&self, start: Start) -> Result<StartOk> {
        // helper to search space-separated strings (mechanisms and locales)
        fn server_supports(server: &str, client: &str) -> bool {
            server.split(" ").any(|s| s == client)
        }

        // ensure our requested auth mechanism and locale are available
        let mechanism = self.auth.mechanism();
        if !server_supports(&start.mechanisms, &mechanism) {
            return Err(ErrorKind::UnsupportedAuthMechanism(
                start.mechanisms.clone(),
            ))?;
        }
        if !server_supports(&start.locales, &self.locale) {
            return Err(ErrorKind::UnsupportedLocale(start.locales.clone()))?;
        }

        // bundle up info about this crate as client properties
        let mut client_properties = FieldTable::new();
        let mut set_prop = |k: &str, v: &str| {
            client_properties.insert(k.to_string(), AMQPValue::LongString(v.to_string()));
        };
        set_prop("product", crate::built_info::PKG_NAME);
        set_prop("version", crate::built_info::PKG_VERSION);
        set_prop("platform", crate::built_info::CFG_OS);
        client_properties.insert(
            "information".to_string(),
            AMQPValue::LongString(format!(
                "built by {} at {}",
                crate::built_info::RUSTC_VERSION,
                crate::built_info::BUILT_TIME_UTC
            )),
        );
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

        Ok(StartOk {
            client_properties,
            mechanism,
            response: self.auth.response(),
            locale: self.locale.clone(),
        })
    }

    pub(crate) fn make_tune_ok(&self, tune: Tune) -> Result<TuneOk> {
        let chan_max0 = if tune.channel_max == 0 {
            u16::max_value()
        } else {
            tune.channel_max
        };
        let chan_max1 = if self.channel_max == 0 {
            u16::max_value()
        } else {
            self.channel_max
        };
        let channel_max = u16::min(chan_max0, chan_max1);
        let frame_max = u32::min(tune.frame_max, self.frame_max);
        let heartbeat = u16::min(tune.heartbeat, self.heartbeat);

        if frame_max < FRAME_MIN_SIZE as u32 {
            return Err(ErrorKind::FrameMaxTooSmall(FRAME_MIN_SIZE as u32))?;
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
        assert_eq!(
            *res.unwrap_err().kind(),
            ErrorKind::UnsupportedAuthMechanism(server_mechanisms.to_string())
        );
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
        assert_eq!(
            *res.unwrap_err().kind(),
            ErrorKind::UnsupportedLocale(server_locales.to_string())
        );
    }

    #[test]
    fn frame_max_too_small() {
        let frame_max = FRAME_MIN_SIZE as u32 - 1;
        let options = ConnectionOptions::<Auth>::default().frame_max(frame_max);

        let tune = Tune {
            channel_max: u16::max_value(),
            frame_max: 1 << 17,
            heartbeat: 60,
        };

        let res = options.make_tune_ok(tune);
        assert!(res.is_err());
        assert_eq!(
            *res.unwrap_err().kind(),
            ErrorKind::FrameMaxTooSmall(FRAME_MIN_SIZE as u32)
        );
    }
}
