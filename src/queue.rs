use crate::{Channel, Consumer, Exchange, FieldTable, Result};

pub struct Queue<'a> {
    channel: &'a Channel,
    name: String,
}

pub struct QueueDeclareOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

pub struct QueueDeleteOptions {
    pub if_unused: bool,
    pub if_empty: bool,
    pub nowait: bool,
}

impl Queue<'_> {
    pub(crate) fn new(channel: &Channel, name: String) -> Queue {
        Queue { channel, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn consume(
        &self,
        no_local: bool,
        no_ack: bool,
        exclusive: bool,
        arguments: FieldTable,
    ) -> Result<Consumer> {
        self.channel
            .basic_consume(self.name.clone(), no_local, no_ack, exclusive, arguments)
    }

    pub fn bind<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        nowait: bool,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_bind(self.name(), exchange.name(), routing_key, nowait, arguments)
    }

    pub fn unbind<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_unbind(self.name(), exchange.name(), routing_key, arguments)
    }

    pub fn purge(&self, nowait: bool) -> Result<Option<u32>> {
        self.channel.queue_purge(self.name(), nowait)
    }

    pub fn delete(&self, options: QueueDeleteOptions) -> Result<Option<u32>> {
        self.channel.queue_delete(self.name(), options)
    }
}
