use crate::{Channel, Consumer, Delivery, Exchange, FieldTable, Get, Result};
use amq_protocol::protocol::queue::{Declare, Delete};

pub struct Queue<'a> {
    channel: &'a Channel,
    name: String,
    message_count: Option<u32>,
    consumer_count: Option<u32>,
}

#[derive(Clone, Debug, Default)]
pub struct QueueDeclareOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub arguments: FieldTable,
}

impl QueueDeclareOptions {
    pub(crate) fn into_declare(self, queue: String, passive: bool, nowait: bool) -> Declare {
        Declare {
            ticket: 0,
            queue,
            passive,
            durable: self.durable,
            exclusive: self.exclusive,
            auto_delete: self.auto_delete,
            nowait,
            arguments: self.arguments,
        }
    }
}

pub struct QueueDeleteOptions {
    pub if_unused: bool,
    pub if_empty: bool,
}

impl QueueDeleteOptions {
    pub(crate) fn into_delete(self, queue: String, nowait: bool) -> Delete {
        Delete {
            ticket: 0,
            queue,
            if_unused: self.if_unused,
            if_empty: self.if_empty,
            nowait,
        }
    }
}

impl Queue<'_> {
    pub(crate) fn new(
        channel: &Channel,
        name: String,
        message_count: Option<u32>,
        consumer_count: Option<u32>,
    ) -> Queue {
        Queue {
            channel,
            name,
            message_count,
            consumer_count,
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn declared_message_count(&self) -> Option<u32> {
        self.message_count
    }

    #[inline]
    pub fn declared_consumer_count(&self) -> Option<u32> {
        self.consumer_count
    }

    #[inline]
    pub fn get(&self, no_ack: bool) -> Result<Option<Get>> {
        self.channel.basic_get(self.name.clone(), no_ack)
    }

    #[inline]
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

    #[inline]
    pub fn bind<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_bind(self.name(), exchange.name(), routing_key, arguments)
    }

    #[inline]
    pub fn bind_nowait<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_bind_nowait(self.name(), exchange.name(), routing_key, arguments)
    }

    #[inline]
    pub fn unbind<S: Into<String>>(
        &self,
        exchange: &Exchange,
        routing_key: S,
        arguments: FieldTable,
    ) -> Result<()> {
        self.channel
            .queue_unbind(self.name(), exchange.name(), routing_key, arguments)
    }

    #[inline]
    pub fn purge(&self) -> Result<u32> {
        self.channel.queue_purge(self.name())
    }

    #[inline]
    pub fn purge_nowait(&self) -> Result<()> {
        self.channel.queue_purge_nowait(self.name())
    }

    #[inline]
    pub fn delete(self, options: QueueDeleteOptions) -> Result<u32> {
        self.channel.queue_delete(self.name(), options)
    }

    #[inline]
    pub fn delete_nowait(self, options: QueueDeleteOptions) -> Result<()> {
        self.channel.queue_delete_nowait(self.name(), options)
    }

    #[inline]
    pub fn ack(&self, delivery: &Delivery, multiple: bool) -> Result<()> {
        self.channel.basic_ack(delivery, multiple)
    }

    #[inline]
    pub fn nack(&self, delivery: &Delivery, multiple: bool, requeue: bool) -> Result<()> {
        self.channel.basic_nack(delivery, multiple, requeue)
    }

    #[inline]
    pub fn reject(&self, delivery: &Delivery, requeue: bool) -> Result<()> {
        self.channel.basic_reject(delivery, requeue)
    }
}
