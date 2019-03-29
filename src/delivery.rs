use crate::AmqpProperties;
use amq_protocol::protocol::basic::{Deliver, GetOk};

#[derive(Clone, Debug)]
pub struct Delivery {
    delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
    pub content: Vec<u8>,
    pub properties: AmqpProperties,
}

impl Delivery {
    pub(crate) fn new(
        deliver: Deliver,
        content: Vec<u8>,
        properties: AmqpProperties,
    ) -> (String, Delivery) {
        (
            deliver.consumer_tag,
            Delivery {
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
        get_ok: GetOk,
        content: Vec<u8>,
        properties: AmqpProperties,
    ) -> Delivery {
        Delivery {
            delivery_tag: get_ok.delivery_tag,
            redelivered: get_ok.redelivered,
            exchange: get_ok.exchange,
            routing_key: get_ok.routing_key,
            content,
            properties,
        }
    }

    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }
}
