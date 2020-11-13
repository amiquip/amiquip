use crate::errors::*;
use amq_protocol::frame::generation::{
    gen_content_body_frame, gen_content_header_frame, gen_heartbeat_frame, gen_method_frame,
};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::AMQPProperties;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::confirm::AMQPMethod as AmqpConfirm;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::exchange::AMQPMethod as AmqpExchange;
use amq_protocol::protocol::queue::AMQPMethod as AmqpQueue;
use amq_protocol::protocol::AMQPClass;
use cookie_factory::GenError;
use std::ops::{Index, RangeFrom};
use std::result::Result as StdResult;

pub trait TryFromAmqpClass: Sized {
    fn try_from(class: AMQPClass) -> Result<Self>;
}

macro_rules! impl_try_from_class {
    ($type:ty, $class:path, $method:path) => {
        impl TryFromAmqpClass for $type {
            fn try_from(class: AMQPClass) -> Result<Self> {
                match class {
                    $class($method(val)) => Ok(val),
                    _ => FrameUnexpected.fail(),
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
    fn try_from(channel_id: u16, frame: AMQPFrame) -> Result<Self>;
}

impl<T: TryFromAmqpClass> TryFromAmqpFrame for T {
    fn try_from(expected_id: u16, frame: AMQPFrame) -> Result<Self> {
        match frame {
            AMQPFrame::Method(channel_id, method) => {
                if expected_id == channel_id {
                    Self::try_from(method)
                } else {
                    FrameUnexpected.fail()
                }
            }
            _ => FrameUnexpected.fail(),
        }
    }
}

pub trait IntoAmqpClass {
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
    pub fn with_protocol_header() -> OutputBuffer {
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

    pub fn push_heartbeat(&mut self) {
        // serializing heartbeat cannot fail; safe to unwrap.
        serialize(&mut self.0, |buf, pos| gen_heartbeat_frame((buf, pos)))
    }

    // This can only fail if there is a bug in the serialization library; it is probably
    // safe to unwrap, but little cost to return a Result instead.
    pub fn push_method<M>(&mut self, channel_id: u16, method: M)
    where
        M: IntoAmqpClass,
    {
        let class = method.into_class();
        serialize(&mut self.0, |buf, pos| {
            gen_method_frame((buf, pos), channel_id, &class)
        })
    }

    pub(crate) fn push_content_header(
        &mut self,
        channel_id: u16,
        class_id: u16,
        length: usize,
        properties: &AMQPProperties,
    ) {
        let length = length as u64;
        serialize(&mut self.0, |buf, pos| {
            gen_content_header_frame((buf, pos), channel_id, class_id, length, properties)
        })
    }

    pub(crate) fn push_content_body(&mut self, channel_id: u16, content: &[u8]) {
        serialize(&mut self.0, |buf, pos| {
            gen_content_body_frame((buf, pos), channel_id, content)
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn clear(&mut self) {
        self.0.clear()
    }

    #[inline]
    pub fn drain_written(&mut self, n: usize) {
        self.0.drain(0..n);
    }

    #[inline]
    pub fn append(&mut self, mut other: OutputBuffer) {
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
    pub(super) fn push_heartbeat(&mut self) {
        if !self.sealed {
            self.buf.push_heartbeat();
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

fn serialize<F: Fn(&mut [u8], usize) -> StdResult<(&mut [u8], usize), GenError>>(
    buf: &mut Vec<u8>,
    f: F,
) {
    let pos = buf.len();
    loop {
        let resize_to = match f(buf, pos) {
            Ok(_) => return,
            Err(GenError::BufferTooSmall(n)) => n,
            Err(err) => unreachable!("impossible serialization error: {:?}", err),
        };
        buf.resize(resize_to, 0);
    }
}
