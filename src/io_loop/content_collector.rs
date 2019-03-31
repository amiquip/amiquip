use crate::{AmqpProperties, Delivery, ErrorKind, Get, Result, Return};
use amq_protocol::frame::AMQPContentHeader;
use amq_protocol::protocol::basic::Deliver;
use amq_protocol::protocol::basic::GetOk as AmqpGetOk;
use amq_protocol::protocol::basic::Return as AmqpReturn;

pub(super) struct ContentCollector {
    channel_id: u16,
    kind: Option<Kind>,
}

pub(super) enum CollectorResult {
    Delivery((String, Delivery)),
    Return(Return),
    Get(Get),
}

impl ContentCollector {
    pub(super) fn new(channel_id: u16) -> ContentCollector {
        ContentCollector {
            channel_id,
            kind: None,
        }
    }

    pub(super) fn collect_deliver(&mut self, deliver: Deliver) -> Result<()> {
        match self.kind.take() {
            None => {
                self.kind = Some(Kind::Delivery(State::Start(deliver)));
                Ok(())
            }
            Some(_) => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    pub(super) fn collect_return(&mut self, return_: AmqpReturn) -> Result<()> {
        match self.kind.take() {
            None => {
                self.kind = Some(Kind::Return(State::Start(return_)));
                Ok(())
            }
            Some(_) => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    pub(super) fn collect_get(&mut self, get_ok: AmqpGetOk) -> Result<()> {
        match self.kind.take() {
            None => {
                self.kind = Some(Kind::Get(State::Start(get_ok)));
                Ok(())
            }
            Some(_) => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    pub(super) fn collect_header(
        &mut self,
        header: AMQPContentHeader,
    ) -> Result<Option<CollectorResult>> {
        match self.kind.take() {
            Some(Kind::Delivery(state)) => match state.collect_header(self.channel_id, header)? {
                Content::Done((tag, delivery)) => {
                    self.kind = None;
                    Ok(Some(CollectorResult::Delivery((tag, delivery))))
                }
                Content::NeedMore(state) => {
                    self.kind = Some(Kind::Delivery(state));
                    Ok(None)
                }
            },
            Some(Kind::Return(state)) => match state.collect_header(self.channel_id, header)? {
                Content::Done(return_) => {
                    self.kind = None;
                    Ok(Some(CollectorResult::Return(return_)))
                }
                Content::NeedMore(state) => {
                    self.kind = Some(Kind::Return(state));
                    Ok(None)
                }
            },
            Some(Kind::Get(state)) => match state.collect_header(self.channel_id, header)? {
                Content::Done(get) => {
                    self.kind = None;
                    Ok(Some(CollectorResult::Get(get)))
                }
                Content::NeedMore(state) => {
                    self.kind = Some(Kind::Get(state));
                    Ok(None)
                }
            },
            None => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    pub(super) fn collect_body(&mut self, body: Vec<u8>) -> Result<Option<CollectorResult>> {
        match self.kind.take() {
            Some(Kind::Delivery(state)) => match state.collect_body(self.channel_id, body)? {
                Content::Done((tag, delivery)) => {
                    self.kind = None;
                    Ok(Some(CollectorResult::Delivery((tag, delivery))))
                }
                Content::NeedMore(state) => {
                    self.kind = Some(Kind::Delivery(state));
                    Ok(None)
                }
            },
            Some(Kind::Return(state)) => match state.collect_body(self.channel_id, body)? {
                Content::Done(return_) => {
                    self.kind = None;
                    Ok(Some(CollectorResult::Return(return_)))
                }
                Content::NeedMore(state) => {
                    self.kind = Some(Kind::Return(state));
                    Ok(None)
                }
            },
            Some(Kind::Get(state)) => match state.collect_body(self.channel_id, body)? {
                Content::Done(get) => {
                    self.kind = None;
                    Ok(Some(CollectorResult::Get(get)))
                }
                Content::NeedMore(state) => {
                    self.kind = Some(Kind::Get(state));
                    Ok(None)
                }
            },
            None => Err(ErrorKind::FrameUnexpected)?,
        }
    }
}

enum Kind {
    Delivery(State<Delivery>),
    Return(State<Return>),
    Get(State<Get>),
}

trait ContentType {
    type Start;
    type Finish;

    fn new(
        channel_id: u16,
        start: Self::Start,
        buf: Vec<u8>,
        properties: AmqpProperties,
    ) -> Self::Finish;
}

impl ContentType for Delivery {
    type Start = Deliver;
    type Finish = (String, Delivery);

    fn new(
        channel_id: u16,
        start: Self::Start,
        buf: Vec<u8>,
        properties: AmqpProperties,
    ) -> Self::Finish {
        Delivery::new(channel_id, start, buf, properties)
    }
}

impl ContentType for Return {
    type Start = AmqpReturn;
    type Finish = Return;

    fn new(
        _channel_id: u16,
        start: Self::Start,
        buf: Vec<u8>,
        properties: AmqpProperties,
    ) -> Self::Finish {
        Return::new(start, buf, properties)
    }
}

impl ContentType for Get {
    type Start = AmqpGetOk;
    type Finish = Get;

    fn new(
        channel_id: u16,
        get_ok: AmqpGetOk,
        buf: Vec<u8>,
        properties: AmqpProperties,
    ) -> Self::Finish {
        let message_count = get_ok.message_count;
        let delivery = Delivery::new_get_ok(channel_id, get_ok, buf, properties);
        Get {
            delivery,
            message_count,
        }
    }
}

enum Content<T: ContentType> {
    Done(T::Finish),
    NeedMore(State<T>),
}

// Clippy warns about State::Body being much larger than the other variant, but we
// expect almost all instances of State to transition to Body.
#[allow(clippy::large_enum_variant)]
enum State<T: ContentType> {
    Start(T::Start),
    Body(T::Start, AMQPContentHeader, Vec<u8>),
}

impl<T: ContentType> State<T> {
    fn collect_header(self, channel_id: u16, header: AMQPContentHeader) -> Result<Content<T>> {
        match self {
            State::Start(start) => {
                if header.body_size == 0 {
                    Ok(Content::Done(T::new(
                        channel_id,
                        start,
                        Vec::new(),
                        header.properties,
                    )))
                } else {
                    let buf = Vec::with_capacity(header.body_size as usize);
                    Ok(Content::NeedMore(State::Body(start, header, buf)))
                }
            }
            State::Body(_, _, _) => Err(ErrorKind::FrameUnexpected)?,
        }
    }

    fn collect_body(self, channel_id: u16, mut body: Vec<u8>) -> Result<Content<T>> {
        match self {
            State::Body(start, header, mut buf) => {
                let body_size = header.body_size as usize;
                buf.append(&mut body);
                if buf.len() == body_size {
                    Ok(Content::Done(T::new(
                        channel_id,
                        start,
                        buf,
                        header.properties,
                    )))
                } else if buf.len() < body_size {
                    Ok(Content::NeedMore(State::Body(start, header, buf)))
                } else {
                    Err(ErrorKind::FrameUnexpected)?
                }
            }
            State::Start(_) => Err(ErrorKind::FrameUnexpected)?,
        }
    }
}
