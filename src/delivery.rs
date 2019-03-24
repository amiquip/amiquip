use amq_protocol::protocol::basic::Deliver;

#[derive(Debug)]
pub struct Delivery {
    delivery_tag: u64,
    redelivered: bool,
    exchange: String,
    routing_key: String,
    content: Vec<u8>,
}

impl Delivery {
    pub(crate) fn new(deliver: Deliver, content: Vec<u8>) -> (String, Delivery) {
        (
            deliver.consumer_tag,
            Delivery {
                delivery_tag: deliver.delivery_tag,
                redelivered: deliver.redelivered,
                exchange: deliver.exchange,
                routing_key: deliver.routing_key,
                content,
            },
        )
    }
}
