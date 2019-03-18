use crate::{ErrorKind, Result};
use amq_protocol::frame::generation::{gen_heartbeat_frame, gen_method_frame};
use amq_protocol::protocol::basic::AMQPMethod as AmqpBasic;
use amq_protocol::protocol::channel::AMQPMethod as AmqpChannel;
use amq_protocol::protocol::connection::AMQPMethod as AmqpConnection;
use amq_protocol::protocol::AMQPClass;
use cookie_factory::GenError;
use std::ops::{Index, RangeFrom};
use std::result::Result as StdResult;

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

pub(crate) struct OutputBuffer(Vec<u8>);

impl OutputBuffer {
    pub(crate) fn new() -> OutputBuffer {
        OutputBuffer(Vec::from("AMQP\x00\x00\x09\x01".as_bytes()))
    }

    pub(crate) fn push_heartbeat(&mut self) {
        // serializing heartbeat cannot fail; safe to unwrap.
        serialize(&mut self.0, |buf, pos| gen_heartbeat_frame((buf, pos))).unwrap();
    }

    // This can only fail if there is a bug in the serialization library; it is probably
    // safe to unwrap, but little cost to return a Result instead.
    pub(crate) fn push_method<M>(&mut self, channel_id: u16, method: M) -> Result<()>
    where
        M: IntoAmqpClass,
    {
        let class = method.into_class();
        serialize(&mut self.0, |buf, pos| {
            gen_method_frame((buf, pos), channel_id, &class)
        })
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
}

impl Index<RangeFrom<usize>> for OutputBuffer {
    type Output = [u8];

    #[inline]
    fn index(&self, index: RangeFrom<usize>) -> &[u8] {
        &self.0[index]
    }
}

pub(crate) fn serialize_heartbeat(buf: &mut Vec<u8>) -> Result<()> {
    serialize(buf, |buf, pos| gen_heartbeat_frame((buf, pos)))
}

pub(crate) fn serialize_method<M: IntoAmqpClass>(
    channel_id: u16,
    method: M,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let class = method.into_class();
    serialize(buf, |buf, pos| {
        gen_method_frame((buf, pos), channel_id, &class)
    })
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
