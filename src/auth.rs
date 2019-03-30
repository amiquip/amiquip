/// A trait encapsulating the operations required to authenticate to an AMQP server.
///
/// # Warning
///
/// SASL mechanisms that require AMQP secure / secure-ok exchanges are currently
/// not supported.
pub trait Sasl: Default + Clone + Send + 'static {
    /// The SASL mechanism to report. The server must support this mechanism
    /// for authentication to succeed.
    fn mechanism(&self) -> String;

    /// The response body to send along with the mechanism.
    fn response(&self) -> String;
}

/// Built-in authentication mechanisms.
///
/// The [`default`](#impl-Default) implementation creates an [`Auth::Plain`](#variant.Plain)
/// variant with the username and password both set to `guest`.
#[derive(Debug, Clone, PartialEq)]
pub enum Auth {
    /// PLAIN authentication via a username and passwords.
    Plain { username: String, password: String },

    /// EXTERNAL authentication, typically supported via SSL certificates.
    External,
}

impl Default for Auth {
    fn default() -> Auth {
        Auth::Plain {
            username: "guest".to_string(),
            password: "guest".to_string(),
        }
    }
}

impl Sasl for Auth {
    fn mechanism(&self) -> String {
        match *self {
            Auth::Plain { .. } => "PLAIN".to_string(),
            Auth::External => "EXTERNAL".to_string(),
        }
    }

    fn response(&self) -> String {
        match self {
            Auth::Plain { username, password } => format!("\x00{}\x00{}", username, password),
            Auth::External => "".to_string(),
        }
    }
}
