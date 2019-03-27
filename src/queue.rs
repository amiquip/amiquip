use crate::{Channel, Consumer, FieldTable, Result};

pub struct Queue<'a> {
    channel: &'a Channel,
    name: String,
}

#[derive(Default)]
pub struct QueueDeclareOptions {
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

impl Queue<'_> {
    pub(crate) fn new(channel: &Channel, name: String) -> Queue {
        Queue { channel, name }
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
}
