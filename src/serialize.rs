use crate::errors::*;
use amq_protocol::frame::AMQPContentHeader;
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::AMQPProperties;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::confirm::AMQPMethod as AmqpConfirm;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::exchange::AMQPMethod as AmqpExchange;
use amq_protocol::protocol::queue::AMQPMethod as AmqpQueue;
use amq_protocol::protocol::AMQPClass;
use std::ops::{Index, RangeFrom};

pub(crate) trait TryFromAmqpClass: Sized {
    fn try_from_class(class: AMQPClass) -> Result<Self>;
}

macro_rules! impl_try_from_class {
    ($type:ty, $class:path, $method:path) => {
        impl TryFromAmqpClass for $type {
            fn try_from_class(class: AMQPClass) -> Result<Self> {
                match class {
                    $class($method(val)) => Ok(val),
                    _ => FrameUnexpectedSnafu.fail(),
                }
            }
        }
    };
}

impl_try_from_class!(
    amq_protocol::protocol::connection::Start,
    AMQPClass::Connection,
    AmqpConnection::Start
);
impl_try_from_class!(
    amq_protocol::protocol::connection::Secure,
    AMQPClass::Connection,
    AmqpConnection::Secure
);
impl_try_from_class!(
    amq_protocol::protocol::connection::Tune,
    AMQPClass::Connection,
    AmqpConnection::Tune
);
impl_try_from_class!(
    amq_protocol::protocol::connection::OpenOk,
    AMQPClass::Connection,
    AmqpConnection::OpenOk
);
impl_try_from_class!(
    amq_protocol::protocol::connection::Close,
    AMQPClass::Connection,
    AmqpConnection::Close
);
impl_try_from_class!(
    amq_protocol::protocol::connection::CloseOk,
    AMQPClass::Connection,
    AmqpConnection::CloseOk
);

impl_try_from_class!(
    amq_protocol::protocol::channel::OpenOk,
    AMQPClass::Channel,
    AmqpChannel::OpenOk
);
impl_try_from_class!(
    amq_protocol::protocol::channel::CloseOk,
    AMQPClass::Channel,
    AmqpChannel::CloseOk
);

impl_try_from_class!(
    amq_protocol::protocol::basic::ConsumeOk,
    AMQPClass::Basic,
    AmqpBasic::ConsumeOk
);
impl_try_from_class!(
    amq_protocol::protocol::basic::CancelOk,
    AMQPClass::Basic,
    AmqpBasic::CancelOk
);
impl_try_from_class!(
    amq_protocol::protocol::basic::QosOk,
    AMQPClass::Basic,
    AmqpBasic::QosOk
);
impl_try_from_class!(
    amq_protocol::protocol::basic::RecoverOk,
    AMQPClass::Basic,
    AmqpBasic::RecoverOk
);

impl_try_from_class!(
    amq_protocol::protocol::confirm::SelectOk,
    AMQPClass::Confirm,
    AmqpConfirm::SelectOk
);

impl_try_from_class!(
    amq_protocol::protocol::queue::DeclareOk,
    AMQPClass::Queue,
    AmqpQueue::DeclareOk
);
impl_try_from_class!(
    amq_protocol::protocol::queue::BindOk,
    AMQPClass::Queue,
    AmqpQueue::BindOk
);
impl_try_from_class!(
    amq_protocol::protocol::queue::DeleteOk,
    AMQPClass::Queue,
    AmqpQueue::DeleteOk
);
impl_try_from_class!(
    amq_protocol::protocol::queue::PurgeOk,
    AMQPClass::Queue,
    AmqpQueue::PurgeOk
);
impl_try_from_class!(
    amq_protocol::protocol::queue::UnbindOk,
    AMQPClass::Queue,
    AmqpQueue::UnbindOk
);

impl_try_from_class!(
    amq_protocol::protocol::exchange::BindOk,
    AMQPClass::Exchange,
    AmqpExchange::BindOk
);
impl_try_from_class!(
    amq_protocol::protocol::exchange::DeclareOk,
    AMQPClass::Exchange,
    AmqpExchange::DeclareOk
);
impl_try_from_class!(
    amq_protocol::protocol::exchange::DeleteOk,
    AMQPClass::Exchange,
    AmqpExchange::DeleteOk
);
impl_try_from_class!(
    amq_protocol::protocol::exchange::UnbindOk,
    AMQPClass::Exchange,
    AmqpExchange::UnbindOk
);

pub(crate) trait TryFromAmqpFrame: Sized {
    fn try_from_frame(channel_id: u16, frame: AMQPFrame) -> Result<Self>;
}

impl<T: TryFromAmqpClass> TryFromAmqpFrame for T {
    fn try_from_frame(expected_id: u16, frame: AMQPFrame) -> Result<Self> {
        match frame {
            AMQPFrame::Method(channel_id, method) => {
                if expected_id == channel_id {
                    Self::try_from_class(method)
                } else {
                    FrameUnexpectedSnafu.fail()
                }
            }
            _ => FrameUnexpectedSnafu.fail(),
        }
    }
}

pub(crate) trait IntoAmqpClass {
    fn into_class(self) -> AMQPClass;
}

impl IntoAmqpClass for AmqpConnection {
    fn into_class(self) -> AMQPClass {
        AMQPClass::Connection(self)
    }
}

impl IntoAmqpClass for AmqpBasic {
    fn into_class(self) -> AMQPClass {
        AMQPClass::Basic(self)
    }
}

impl IntoAmqpClass for AmqpChannel {
    fn into_class(self) -> AMQPClass {
        AMQPClass::Channel(self)
    }
}

impl IntoAmqpClass for AmqpConfirm {
    fn into_class(self) -> AMQPClass {
        AMQPClass::Confirm(self)
    }
}

impl IntoAmqpClass for AmqpQueue {
    fn into_class(self) -> AMQPClass {
        AMQPClass::Queue(self)
    }
}

impl IntoAmqpClass for AmqpExchange {
    fn into_class(self) -> AMQPClass {
        AMQPClass::Exchange(self)
    }
}

#[derive(Debug)]
pub(crate) struct OutputBuffer(Vec<u8>);

impl OutputBuffer {
    pub(crate) fn with_protocol_header() -> OutputBuffer {
        OutputBuffer(b"AMQP\x00\x00\x09\x01".to_vec())
    }

    pub(crate) fn empty() -> OutputBuffer {
        OutputBuffer(Vec::new())
    }

    pub(crate) fn drain_into_new_buf(&mut self) -> OutputBuffer {
        let mut buf = OutputBuffer(Vec::with_capacity(self.len()));
        buf.0.append(&mut self.0);
        buf
    }

    pub(crate) fn push_heartbeat(&mut self, channel_id: u16) {
        // serializing heartbeat cannot fail; safe to unwrap.
        let frame = AMQPFrame::Heartbeat(channel_id);
        self.append_frame(frame);
    }

    // This can only fail if there is a bug in the serialization library; it is probably
    // safe to unwrap, but little cost to return a Result instead.
    pub(crate) fn push_method<M>(&mut self, channel_id: u16, method: M)
    where
        M: IntoAmqpClass,
    {
        let frame = AMQPFrame::Method(channel_id, method.into_class());
        self.append_frame(frame);
    }

    pub(crate) fn push_content_header(
        &mut self,
        channel_id: u16,
        class_id: u16,
        length: usize,
        properties: &AMQPProperties,
    ) {
        let header = Box::new(AMQPContentHeader {
            class_id,
            body_size: length as u64,
            properties: properties.to_owned(),
        });
        let frame = AMQPFrame::Header(channel_id, class_id, header);
        self.append_frame(frame);
    }

    pub(crate) fn push_content_body(&mut self, channel_id: u16, content: &[u8]) {
        let frame = AMQPFrame::Body(channel_id, content.into());
        self.append_frame(frame);
    }

    fn append_frame(&mut self, frame: AMQPFrame) {
        let serialize_fn = amq_protocol::frame::gen_frame(&frame);
        let (mut buf, _) = serialize_fn(Vec::<u8>::new().into()).unwrap().into_inner();
        self.0.append(&mut buf);
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.0.clear()
    }

    #[inline]
    pub(crate) fn drain_written(&mut self, n: usize) {
        self.0.drain(0..n);
    }

    #[inline]
    pub(crate) fn append(&mut self, mut other: OutputBuffer) {
        self.0.append(&mut other.0)
    }
}

impl Index<RangeFrom<usize>> for OutputBuffer {
    type Output = [u8];

    #[inline]
    fn index(&self, index: RangeFrom<usize>) -> &[u8] {
        &self.0[index]
    }
}

pub(super) struct SealableOutputBuffer {
    buf: OutputBuffer,
    sealed: bool,
}

impl SealableOutputBuffer {
    pub(super) fn new(buf: OutputBuffer) -> SealableOutputBuffer {
        SealableOutputBuffer { buf, sealed: false }
    }

    #[inline]
    pub(super) fn seal(&mut self) {
        self.sealed = true;
    }

    #[inline]
    pub(super) fn is_sealed(&self) -> bool {
        self.sealed
    }

    #[inline]
    pub(super) fn push_heartbeat(&mut self, channel_id: u16) {
        if !self.sealed {
            self.buf.push_heartbeat(channel_id);
        }
    }

    #[inline]
    pub(super) fn push_method<M>(&mut self, channel_id: u16, method: M)
    where
        M: IntoAmqpClass,
    {
        if !self.sealed {
            self.buf.push_method(channel_id, method)
        }
    }

    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub(super) fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.buf.clear()
    }

    #[inline]
    pub(super) fn drain_written(&mut self, n: usize) {
        self.buf.drain_written(n)
    }

    #[inline]
    pub(super) fn append(&mut self, other: OutputBuffer) {
        if !self.sealed {
            self.buf.append(other)
        }
    }
}

impl Index<RangeFrom<usize>> for SealableOutputBuffer {
    type Output = [u8];

    #[inline]
    fn index(&self, index: RangeFrom<usize>) -> &[u8] {
        &self.buf[index]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use amq_protocol::protocol::basic::{AMQPMethod, CancelOk, Publish};

    #[test]
    fn test_heartbeat() {
        let mut buf = OutputBuffer::empty();
        buf.push_heartbeat(3);
        let expected = [8, 0, 3, 0, 0, 0, 0, 206];
        assert_eq!(buf.0, &expected);
    }

    #[test]
    fn test_method() {
        let mut buf = OutputBuffer::empty();
        let method = AMQPMethod::CancelOk(CancelOk {
            consumer_tag: "tag".into(),
        });
        buf.push_method(3, method);
        let expected = [1, 0, 3, 0, 0, 0, 8, 0, 60, 0, 31, 3, 116, 97, 103, 206];
        assert_eq!(buf.0, &expected);
    }

    #[test]
    fn test_content_header() {
        let mut buf = OutputBuffer::empty();
        let properties = AMQPProperties::default();
        let class_id = Publish::default().get_amqp_class_id();
        buf.push_content_header(3, class_id, 0, &properties);
        let expected = [
            2, 0, 3, 0, 0, 0, 14, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 206,
        ];
        assert_eq!(buf.0, &expected);
    }

    #[test]
    fn test_content_body() {
        let mut buf = OutputBuffer::empty();
        buf.push_content_body(3, "test body".as_bytes());
        let expected = [
            3, 0, 3, 0, 0, 0, 9, 116, 101, 115, 116, 32, 98, 111, 100, 121, 206,
        ];
        assert_eq!(buf.0, &expected);
    }

    #[test]
    fn test_multi() {
        let mut buf = OutputBuffer::empty();
        let properties = AMQPProperties::default();
        let body = "test body";
        let class_id = Publish::default().get_amqp_class_id();
        buf.push_content_header(3, class_id, body.len(), &properties);
        buf.push_content_body(3, body.as_bytes());
        let expected = [
            2, 0, 3, 0, 0, 0, 14, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 206, 3, 0, 3, 0, 0, 0,
            9, 116, 101, 115, 116, 32, 98, 111, 100, 121, 206,
        ];
        assert_eq!(buf.0, &expected);
    }
}
