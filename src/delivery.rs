use crate::AmqpProperties;
use amq_protocol::protocol::basic::Deliver;

#[derive(Clone, Debug)]
pub struct Delivery {
    delivery_tag: u64,
    redelivered: bool,
    exchange: String,
    routing_key: String,
    content: Vec<u8>,
    properties: AmqpProperties,
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

    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }

    pub fn redelivered(&self) -> bool {
        self.redelivered
    }

    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    pub fn properties(&self) -> &AmqpProperties {
        &self.properties
    }

    pub fn content(&self) -> &[u8] {
        &self.content
    }

    pub fn into_content(self) -> Vec<u8> {
        self.content
    }
}
