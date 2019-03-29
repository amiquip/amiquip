use crate::{AmqpProperties, Channel, FieldTable, Result};
use amq_protocol::protocol::exchange::Declare;

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

#[derive(Clone, Debug, Default)]
pub struct ExchangeDeclareOptions {
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub arguments: FieldTable,
}

impl ExchangeDeclareOptions {
    pub(crate) fn into_declare(
        self,
        type_: ExchangeType,
        name: String,
        passive: bool,
        nowait: bool,
    ) -> Declare {
        Declare {
            ticket: 0,
            exchange: name,
            passive,
            type_: type_.as_ref().to_string(),
            durable: self.durable,
            auto_delete: self.auto_delete,
            internal: self.internal,
            nowait,
            arguments: self.arguments,
        }
    }
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
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind(self.name(), other.name(), routing_key, arguments)
    }

    pub fn bind_to_source_nowait<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind_nowait(self.name(), other.name(), routing_key, arguments)
    }

    pub fn bind_to_destination<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind(other.name(), self.name(), routing_key, arguments)
    }

    pub fn bind_to_destination_nowait<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind_nowait(other.name(), self.name(), routing_key, arguments)
    }

    pub fn unbind_from_source<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind(self.name(), other.name(), routing_key, arguments)
    }

    pub fn unbind_from_source_nowait<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind_nowait(self.name(), other.name(), routing_key, arguments)
    }

    pub fn unbind_from_destination<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind(other.name(), self.name(), routing_key, arguments)
    }

    pub fn unbind_from_destination_nowait<S: Into<String>>(
        &self,
        other: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind_nowait(other.name(), self.name(), routing_key, arguments)
    }

    pub fn delete(self, if_unused: bool) -> Result<()> {
        self.channel.exchange_delete(self.name(), if_unused)
    }

    pub fn delete_nowait(self, if_unused: bool) -> Result<()> {
        self.channel.exchange_delete_nowait(self.name(), if_unused)
    }
}
