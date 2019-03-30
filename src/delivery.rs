use crate::{AmqpProperties, Channel, Result};
use amq_protocol::protocol::basic::{Deliver, GetOk};

#[derive(Clone, Debug)]
pub struct Delivery {
    channel_id: u16,
    delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
    pub content: Vec<u8>,
    pub properties: AmqpProperties,
}

impl Delivery {
    pub(crate) fn new(
        channel_id: u16,
        deliver: Deliver,
        content: Vec<u8>,
        properties: AmqpProperties,
    ) -> (String, Delivery) {
        (
            deliver.consumer_tag,
            Delivery {
                channel_id,
                delivery_tag: deliver.delivery_tag,
                redelivered: deliver.redelivered,
                exchange: deliver.exchange,
                routing_key: deliver.routing_key,
                content,
                properties,
            },
        )
    }

    pub(crate) fn new_get_ok(
        channel_id: u16,
        get_ok: GetOk,
        content: Vec<u8>,
        properties: AmqpProperties,
    ) -> Delivery {
        Delivery {
            channel_id,
            delivery_tag: get_ok.delivery_tag,
            redelivered: get_ok.redelivered,
            exchange: get_ok.exchange,
            routing_key: get_ok.routing_key,
            content,
            properties,
        }
    }

    #[inline]
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }

    #[inline]
    pub fn ack(&self, channel: &Channel, multiple: bool) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot ack delivery on different channel"
        );
        channel.basic_ack(self, multiple)
    }

    #[inline]
    pub fn nack(&self, channel: &Channel, multiple: bool, requeue: bool) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot nack delivery on different channel"
        );
        channel.basic_nack(self, multiple, requeue)
    }

    #[inline]
    pub fn reject(&self, channel: &Channel, requeue: bool) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot reject delivery on different channel"
        );
        channel.basic_reject(self, requeue)
    }
}
