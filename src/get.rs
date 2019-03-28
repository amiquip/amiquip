use crate::Delivery;

#[derive(Clone, Debug)]
pub struct Get {
    pub delivery: Delivery,
    pub message_count: u32,
}
