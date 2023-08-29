use crate::AmqpProperties;
use amq_protocol::protocol::basic::Return as AmqpReturn;

/// An unpublished message returned to the publishing channel.
///
/// To receive returned messages, you must call
/// [`Channel::listen_for_returns`](struct.Channel.html#method.listen_for_returns). If the server
/// returns messages and that method has not been called, the returned message will be discarded.
#[derive(Clone, Debug)]
pub struct Return {
    /// AMQP code providing information about why the message was undeliverable.
    pub reply_code: u16,

    /// Text providing information about why the message was undeliverable.
    pub reply_text: String,

    /// The name of the exchange this message was originally published to. May be an empty string
    /// (the default exhange).
    pub exchange: String,

    /// The routing key specified when this message was published.
    pub routing_key: String,

    /// The content body containing the message.
    pub content: Vec<u8>,

    /// Properties associated with the message.
    pub properties: AmqpProperties,
}

impl Return {
    pub(crate) fn new(ret: AmqpReturn, content: Vec<u8>, properties: AmqpProperties) -> Return {
        Return {
            reply_code: ret.reply_code,
            reply_text: ret.reply_text.to_string(),
            exchange: ret.exchange.to_string(),
            routing_key: ret.routing_key.to_string(),
            content,
            properties,
        }
    }
}
