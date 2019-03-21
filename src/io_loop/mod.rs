use crate::auth::Sasl;
use crate::connection_options::ConnectionOptions;

mod channel_slots;
mod connection_state;

struct Inner<Auth: Sasl> {
    options: ConnectionOptions<Auth>,
}
