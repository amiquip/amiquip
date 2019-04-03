/// Payload for a publisher confirmation message (either an [ack](enum.Confirm.html#variant.Ack) or
/// a [nack](enum.Confirm.html#variant.Nack)) from the server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConfirmPayload {
    /// The tag from the server. Tags are sequentially increasing integers beginning with
    /// 1 (once publisher confirms [are
    ///   enabled](struct.Channel.html#method.enable_publisher_confirms) on the channel.
    pub delivery_tag: u64,

    /// If true, the confirmation applies to all previously-unconfirmed messages with delivery tags
    /// less than or equal to this payload's [`delivery_tag`](#structfield.delivery_tag).
    pub multiple: bool,
}

/// A publisher confirmation message from the server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Confirm {
    /// Acknowledgment that the server has received the message(s) described by the associated
    /// payload. Note that acks do not necessarily imply that the messages have been handled by a
    /// consumer, merely that they have been received by the server.
    Ack(ConfirmPayload),

    /// Notification that the message(s) described by the associated payload have been rejected.
    Nack(ConfirmPayload),
}
