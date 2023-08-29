use crate::{AmqpProperties, Channel, FieldTable, Result};
use amq_protocol::protocol::exchange::Declare;

/// Types of AMQP exchanges.
#[derive(Debug, Clone)]
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
            exchange: name.into(),
            kind: type_.as_ref().into(),
            passive,
            durable: self.durable,
            auto_delete: self.auto_delete,
            internal: self.internal,
            nowait,
            arguments: self.arguments,
        }
    }
}

/// Wrapper for a message to be published.
#[derive(Debug, Clone)]
pub struct Publish<'a> {
    /// Body of content to send.
    pub body: &'a [u8],

    /// Routing key.
    pub routing_key: String,

    /// If true, return this message to us if it cannot be routed to a queue. See
    /// [`Channel::listen_for_returns`](struct.Channel.html) for receiving returned messages.
    pub mandatory: bool,

    /// If true, return this message to us if it cannot immediately be routed to a consumer. See
    /// [`Channel::listen_for_returns`](struct.Channel.html) for receiving returned messages.
    pub immediate: bool,

    /// Other properties of the message (e.g., headers).
    pub properties: AmqpProperties,
}

impl<'a> Publish<'a> {
    /// Helper to create a message with the given body and routing key; `mandatory` and
    /// `immediate` will be set to false, and `properties` will be empty.
    pub fn new<S: Into<String>>(body: &[u8], routing_key: S) -> Publish {
        Publish {
            body,
            routing_key: routing_key.into(),
            mandatory: false,
            immediate: false,
            properties: AmqpProperties::default(),
        }
    }

    /// Helper to create a message with the given body, routing key, and properties; `mandatory`
    /// and `immediate` will be set to false.
    pub fn with_properties<S: Into<String>>(
        body: &[u8],
        routing_key: S,
        properties: AmqpProperties,
    ) -> Publish {
        Publish {
            body,
            routing_key: routing_key.into(),
            mandatory: false,
            immediate: false,
            properties,
        }
    }
}

/// Handle for a declared AMQP exchange.
pub struct Exchange<'a> {
    channel: &'a Channel,
    name: String,
}

impl Exchange<'_> {
    pub(crate) fn new(channel: &Channel, name: String) -> Exchange {
        Exchange { channel, name }
    }

    /// Construct a handle for the direct exchange on the given `channel`. This is an entirely
    /// local operation; the default exchange (named `""`) is guaranteed to exist and does not need
    /// to be declared.
    pub fn direct(channel: &Channel) -> Exchange {
        let name = "".to_string();
        Exchange { channel, name }
    }

    /// Name of this exchange.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Publish a message to this exchange.
    pub fn publish(&self, publish: Publish) -> Result<()> {
        self.channel.basic_publish(self.name(), publish)
    }

    /// Synchronously bind this exchange (as destination) to the `source` exchange with the given
    /// routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you can
    /// examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn bind_to_source<S: Into<String>>(
        &self,
        source: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind(self.name(), source.name(), routing_key, arguments)
    }

    /// Asynchronously bind this exchange (as destination) to the `source` exchange with the given
    /// routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you can
    /// examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn bind_to_source_nowait<S: Into<String>>(
        &self,
        source: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind_nowait(self.name(), source.name(), routing_key, arguments)
    }

    /// Synchronously bind this exchange (as source) to the `destination` exchange with the given
    /// routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you can
    /// examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn bind_to_destination<S: Into<String>>(
        &self,
        destination: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind(destination.name(), self.name(), routing_key, arguments)
    }

    /// Asynchronously bind this exchange (as source) to the `destination` exchange with the given
    /// routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you can
    /// examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn bind_to_destination_nowait<S: Into<String>>(
        &self,
        destination: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_bind_nowait(destination.name(), self.name(), routing_key, arguments)
    }

    /// Synchronously unbind this exchange (as destination) from the `source` exchange with the
    /// given routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you
    /// can examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn unbind_from_source<S: Into<String>>(
        &self,
        source: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind(self.name(), source.name(), routing_key, arguments)
    }

    /// Asynchronously unbind this exchange (as destination) from the `source` exchange with the
    /// given routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you
    /// can examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn unbind_from_source_nowait<S: Into<String>>(
        &self,
        source: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind_nowait(self.name(), source.name(), routing_key, arguments)
    }

    /// Synchronously unbind this exchange (as source) from the `destination` exchange with the
    /// given routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you
    /// can examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn unbind_from_destination<S: Into<String>>(
        &self,
        destination: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind(destination.name(), self.name(), routing_key, arguments)
    }

    /// Asynchronously unbind this exchange (as source) from the `destination` exchange with the
    /// given routing key and arguments. Exchange-to-exchange binding is a RabbitMQ extension; you
    /// can examine the connection's [server
    /// properties](struct.Connection.html#method.server_properties) to see if the current
    /// connection supports this feature.
    pub fn unbind_from_destination_nowait<S: Into<String>>(
        &self,
        destination: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .exchange_unbind_nowait(destination.name(), self.name(), routing_key, arguments)
    }

    /// Synchronously delete this exchange. If `if_unused` is true, the exchange will only be
    /// deleted if it has no queue bindings; if `if_unused` is true and the exchange still has
    /// queue bindings, the server will close this channel.
    pub fn delete(self, if_unused: bool) -> Result<()> {
        self.channel.exchange_delete(self.name(), if_unused)
    }

    /// Asynchronously delete this exchange. If `if_unused` is true, the exchange will only be
    /// deleted if it has no queue bindings; if `if_unused` is true and the exchange still has
    /// queue bindings, the server will close this channel.
    pub fn delete_nowait(self, if_unused: bool) -> Result<()> {
        self.channel.exchange_delete_nowait(self.name(), if_unused)
    }
}
