use crate::{AmqpProperties, Channel, Result};
use amq_protocol::protocol::basic::{Deliver, GetOk};

/// A message delivered to a consumer.
#[derive(Clone, Debug)]
pub struct Delivery {
    channel_id: u16,
    delivery_tag: u64,

    /// If true, this message has previously been delivered to this or another consumer.
    pub redelivered: bool,

    /// The name of the exchange this message was originally published to. May be an empty string
    /// (the default exhange).
    pub exchange: String,

    /// The routing key specified when this message was published.
    pub routing_key: String,

    /// The content body containing the message.
    pub body: Vec<u8>,

    /// Properties associated with the message.
    pub properties: AmqpProperties,
}

impl Delivery {
    pub(crate) fn new(
        channel_id: u16,
        deliver: Deliver,
        body: Vec<u8>,
        properties: AmqpProperties,
    ) -> (String, Delivery) {
        (
            deliver.consumer_tag.as_str().to_owned(),
            Delivery {
                channel_id,
                delivery_tag: deliver.delivery_tag,
                redelivered: deliver.redelivered,
                exchange: deliver.exchange.to_string(),
                routing_key: deliver.routing_key.to_string(),
                body,
                properties,
            },
        )
    }

    pub(crate) fn new_get_ok(
        channel_id: u16,
        get_ok: GetOk,
        body: Vec<u8>,
        properties: AmqpProperties,
    ) -> Delivery {
        Delivery {
            channel_id,
            delivery_tag: get_ok.delivery_tag,
            redelivered: get_ok.redelivered,
            exchange: get_ok.exchange.to_string(),
            routing_key: get_ok.routing_key.to_string(),
            body,
            properties,
        }
    }

    /// The server-assigned delivery tag for this message. Delivery tags are channel-specific.
    #[inline]
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }

    /// Acknowledge this delivery, which must have been received on the given channel. If
    /// `multiple` is true, acks this delivery and all other deliveries received on this channel
    /// with smaller [`delivery_tag`](#method.delivery_tag)s.
    ///
    /// # Panics
    ///
    /// This method will attempt to panic if `channel` does not match the channel this delivery was
    /// received on. It does this by comparing channel IDs, so it is possible that an incorrect
    /// `Delivery`/`Channel` pairing will not be detected at runtime. Always ack deliveries with
    /// the channel they were received on; the result of failing to do this is unspecified by the
    /// AMQP specification.
    #[inline]
    pub fn ack(self, channel: &Channel) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot ack delivery on different channel"
        );
        channel.basic_ack(self, false)
    }

    /// Acknowledge this delivery, which must have been received on the given channel, and all
    /// other deliveries received on this channel with smaller
    /// [`delivery_tag`](#method.delivery_tag)s.
    ///
    /// # Panics
    ///
    /// This method will attempt to panic if `channel` does not match the channel this delivery was
    /// received on. It does this by comparing channel IDs, so it is possible that an incorrect
    /// `Delivery`/`Channel` pairing will not be detected at runtime. Always ack deliveries with
    /// the channel they were received on; the result of failing to do this is unspecified by the
    /// AMQP specification.
    #[inline]
    pub fn ack_multiple(self, channel: &Channel) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot ack delivery on different channel"
        );
        channel.basic_ack(self, true)
    }

    /// Reject this delivery, which must have been received on the given channel. If `requeue` is
    /// true, instructs the server to attempt to requeue the message.
    ///
    /// # Panics
    ///
    /// This method will attempt to panic if `channel` does not match the channel this delivery was
    /// received on. It does this by comparing channel IDs, so it is possible that an incorrect
    /// `Delivery`/`Channel` pairing will not be detected at runtime. Always ack deliveries with
    /// the channel they were received on; the result of failing to do this is unspecified by the
    /// AMQP specification.
    #[inline]
    pub fn nack(self, channel: &Channel, requeue: bool) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot nack delivery on different channel"
        );
        channel.basic_nack(self, false, requeue)
    }

    /// Reject this delivery, which must have been received on the given channel, and all other
    /// unacknowledged deliveries to this channel with smaller
    /// [`delivery_tag`](#method.delivery_tag)s. If `requeue` is true, instructs the server to
    /// attempt to requeue the message.
    ///
    /// # Panics
    ///
    /// This method will attempt to panic if `channel` does not match the channel this delivery was
    /// received on. It does this by comparing channel IDs, so it is possible that an incorrect
    /// `Delivery`/`Channel` pairing will not be detected at runtime. Always ack deliveries with
    /// the channel they were received on; the result of failing to do this is unspecified by the
    /// AMQP specification.
    #[inline]
    pub fn nack_multiple(self, channel: &Channel, requeue: bool) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot nack delivery on different channel"
        );
        channel.basic_nack(self, true, requeue)
    }

    /// Reject this delivery, which must have been received on the given channel. If `requeue` is
    /// true, instructs the server to attempt to requeue the message.
    ///
    /// # Panics
    ///
    /// This method will attempt to panic if `channel` does not match the channel this delivery was
    /// received on. It does this by comparing channel IDs, so it is possible that an incorrect
    /// `Delivery`/`Channel` pairing will not be detected at runtime. Always ack deliveries with
    /// the channel they were received on; the result of failing to do this is unspecified by the
    /// AMQP specification.
    #[inline]
    pub fn reject(self, channel: &Channel, requeue: bool) -> Result<()> {
        assert_eq!(
            self.channel_id,
            channel.channel_id(),
            "cannot reject delivery on different channel"
        );
        channel.basic_reject(self, requeue)
    }
}
