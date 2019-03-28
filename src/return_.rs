use crate::AmqpProperties;
use amq_protocol::protocol::basic::Return as AmqpReturn;

#[derive(Clone, Debug)]
pub struct Return {
    reply_code: u16,
    reply_text: String,
    exchange: String,
    routing_key: String,
    content: Vec<u8>,
    properties: AmqpProperties,
}

impl Return {
    pub(crate) fn new(ret: AmqpReturn, content: Vec<u8>, properties: AmqpProperties) -> Return {
        Return {
            reply_code: ret.reply_code,
            reply_text: ret.reply_text,
            exchange: ret.exchange,
            routing_key: ret.routing_key,
            content,
            properties,
        }
    }
}
