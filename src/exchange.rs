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

    pub fn direct(channel: &Channel) -> Exchange {
        let name = "".to_string();
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

    pub fn bind_to_source<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        nowait: bool,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind(self.name(), other.name(), routing_key, nowait, arguments)
    }

    pub fn bind_to_destination<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        nowait: bool,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind(other.name(), self.name(), routing_key, nowait, arguments)
    }

    pub fn unbind_from_source<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        nowait: bool,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind(self.name(), other.name(), routing_key, nowait, arguments)
    }

    pub fn unbind_from_destination<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        nowait: bool,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind(other.name(), self.name(), routing_key, nowait, arguments)
    }
}
