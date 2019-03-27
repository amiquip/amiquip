use crate::{AmqpProperties, Channel, FieldTable, Result};

pub enum ExchangeType {
    Direct,
    Fanout,
    Topic,
    Headers,
    Custom(String),
}

impl AsRef<str> for ExchangeType {
    fn as_ref(&self) -> &str {
        use self::ExchangeType::*;
        match self {
            Direct => "direct",
            Fanout => "fanout",
            Topic => "topic",
            Headers => "headers",
            Custom(s) => s,
        }
    }
}

pub struct ExchangeDeclareOptions {
    pub type_: ExchangeType,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

pub struct Exchange<'a> {
    channel: &'a Channel,
    name: String,
}

impl Exchange<'_> {
    pub(crate) fn new(channel: &Channel, name: String) -> Exchange {
        Exchange { channel, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn publish<T: AsRef<[u8]>, S: Into<String>>(
        &self,
        content: T,
        routing_key: S,
        mandatory: bool,
        immediate: bool,
        properties: &AmqpProperties,
    ) -> Result<()> {
        self.channel.basic_publish(
            content,
            self.name(),
            routing_key,
            mandatory,
            immediate,
            properties,
        )
    }
}
