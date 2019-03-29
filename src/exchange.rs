use crate::{AmqpProperties, Channel, FieldTable, Result};
use amq_protocol::protocol::exchange::Declare;

/// Types of AMQP exchanges.
pub enum ExchangeType {
    /// Direct exchange; delivers messages to queues based on the routing key.
    Direct,

    /// Fanout exchange; delivers messages to all bound queues and ignores routing key.
    Fanout,

    /// Topic exchange; delivers messages based on matching between a message routing key and the
    /// pattern that was used to bind a queue to an exchange.
    Topic,

    /// Headers exchanges; ignores routing key and routes based on message header fields.
    Headers,

    /// Custom exchange type; should begin with "x-".
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

/// Options passed to the server when declaring an exchange.
///
/// The [`default`](#impl-Default) implementation sets all boolean fields to false and has an empty
/// set of arguments.
#[derive(Clone, Debug, Default)]
pub struct ExchangeDeclareOptions {
    /// If true, declares exchange as durable (survives server restarts); if false, declares
    /// exchange as transient (will be deleted on a server restart).
    pub durable: bool,

    /// If true, declare exchange as auto-delete: it will be deleted once no queues are bound to
    /// it. The server will keep the exchange around for "a short period of time" to allow queues
    /// to be bound after it is created.
    pub auto_delete: bool,

    /// If true, declare exchange as internal: it may not be used by publishers, but only for
    /// exchange-to-exchange bindings.
    pub internal: bool,

    /// Extra arguments; these are optional in general, but may be needed for some plugins or
    /// server-specific features.
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
