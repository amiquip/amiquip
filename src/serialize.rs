use crate::{ErrorKind, Result};
use amq_protocol::frame::generation::{
    gen_content_body_frame, gen_content_header_frame, gen_heartbeat_frame, gen_method_frame,
};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::basic::AMQPProperties;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::AMQPClass;
use cookie_factory::GenError;
use std::ops::{Index, RangeFrom};
use std::result::Result as StdResult;

pub trait TryFromAmqpClass: Sized {
    fn try_from(class: AMQPClass) -> Result<Self>;
}

impl TryFromAmqpClass for amq_protocol::protocol::channel::CloseOk {
    fn try_from(class: AMQPClass) -> Result<Self> {
        match class {
            AMQPClass::Channel(AmqpChannel::CloseOk(close_ok)) => Ok(close_ok),
            class => Err(ErrorKind::BadRpcResponse(class))?,
        }
    }
}

impl TryFromAmqpClass for amq_protocol::protocol::channel::OpenOk {
    fn try_from(class: AMQPClass) -> Result<Self> {
        match class {
            AMQPClass::Channel(AmqpChannel::OpenOk(open_ok)) => Ok(open_ok),
            class => Err(ErrorKind::BadRpcResponse(class))?,
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

#[derive(Clone)]
pub struct OutputBuffer(Vec<u8>);

impl OutputBuffer {
    pub fn with_protocol_header() -> OutputBuffer {
        OutputBuffer(Vec::from("AMQP\x00\x00\x09\x01".as_bytes()))
    }

    pub fn empty() -> OutputBuffer {
        OutputBuffer(Vec::new())
    }

    pub fn with_method<M>(channel_id: u16, method: M) -> Result<OutputBuffer>
    where
        M: IntoAmqpClass,
    {
        let mut buf = OutputBuffer::empty();
        buf.push_method(channel_id, method)?;
        Ok(buf)
    }

    pub fn with_content_header(
        channel_id: u16,
        class_id: u16,
        length: usize,
        properties: &AMQPProperties,
    ) -> Result<OutputBuffer> {
        let length = length as u64;
        let mut buf = OutputBuffer::empty();
        serialize(&mut buf.0, |buf, pos| {
            gen_content_header_frame((buf, pos), channel_id, class_id, length, properties)
        })?;
        Ok(buf)
    }

    pub fn with_content_body(channel_id: u16, content: &[u8]) -> Result<OutputBuffer> {
        let mut buf = OutputBuffer::empty();
        serialize(&mut buf.0, |buf, pos| {
            gen_content_body_frame((buf, pos), channel_id, content)
        })?;
        Ok(buf)
    }

    pub fn push_heartbeat(&mut self) {
        // serializing heartbeat cannot fail; safe to unwrap.
        serialize(&mut self.0, |buf, pos| gen_heartbeat_frame((buf, pos))).unwrap();
    }

    // This can only fail if there is a bug in the serialization library; it is probably
    // safe to unwrap, but little cost to return a Result instead.
    pub fn push_method<M>(&mut self, channel_id: u16, method: M) -> Result<()>
    where
        M: IntoAmqpClass,
    {
        let class = method.into_class();
        serialize(&mut self.0, |buf, pos| {
            gen_method_frame((buf, pos), channel_id, &class)
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

fn serialize<F: Fn(&mut [u8], usize) -> StdResult<(&mut [u8], usize), GenError>>(
    buf: &mut Vec<u8>,
    f: F,
) -> Result<()> {
    let pos = buf.len();
    loop {
        let resize_to = match f(buf, pos) {
            Ok(_) => return Ok(()),
            Err(GenError::BufferTooSmall(n)) => n,
            Err(_) => return Err(ErrorKind::InternalSerializationError)?,
        };
        buf.resize(resize_to, 0);
    }
}
