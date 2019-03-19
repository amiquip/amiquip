pub trait Sasl: Default + Send + 'static {
    fn mechanism(&self) -> String;
    fn response(&self) -> String;
}

pub enum Auth {
    Plain { username: String, password: String },
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
        }
    }

    fn response(&self) -> String {
        match self {
            Auth::Plain { username, password } => format!("\x00{}\x00{}", username, password),
        }
    }
}
