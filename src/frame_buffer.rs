use crate::{ErrorKind, Result};
use amq_protocol::frame::{parse_frame, AMQPFrame};
use amq_protocol::types::parsing::parse_long_uint;
use bytes::Buf;
use failure::Fail;
use input_buffer::{InputBuffer, MIN_READ};
use log::trace;
use std::io;
use std::marker::PhantomData;

pub struct FrameBuffer(Inner<AmqpFrameKind>);

impl FrameBuffer {
    pub fn new() -> FrameBuffer {
        FrameBuffer(Inner::new())
    }

    pub fn read_from<S, F>(&mut self, stream: &mut S, handler: F) -> Result<usize>
    where
        S: io::Read,
        F: FnMut(AMQPFrame) -> Result<()>,
    {
        self.0.read_from(stream, handler)
    }
}

// Dep. injection helper primarily for unit testing.
trait FrameKind {
    type Frame;

    // Should return None if not enough data is available to know the size of the next
    // frame, or Some(n) if the next frame requires n bytes.
    fn parse_size(buf: &[u8]) -> Option<usize>;

    // Attempt to parse a frame. Will only be called if parse_size() already returned
    // Some(n), and buf will have length exactly n.
    fn parse_frame(buf: &[u8]) -> Result<Self::Frame>;
}

// Standard FrameKind - parses AMQP frames.
enum AmqpFrameKind {}

impl AmqpFrameKind {
    // position (from start of frame) where the "size of frame" bytes are located
    const AMQP_FRAME_SIZE_POS: std::ops::Range<usize> = 3..7;
}

impl FrameKind for AmqpFrameKind {
    type Frame = AMQPFrame;

    fn parse_size(buf: &[u8]) -> Option<usize> {
        if buf.len() < Self::AMQP_FRAME_SIZE_POS.end {
            None
        } else {
            // Parsing a u32 from 4 bytes can't fail; safe to unwrap.
            let (_, size) = parse_long_uint(&buf[Self::AMQP_FRAME_SIZE_POS]).unwrap();

            // actual frame size is the extracted size + 8 bytes (7 byte header and
            // single byte frame-end
            Some(size as usize + 8)
        }
    }

    fn parse_frame(buf: &[u8]) -> Result<AMQPFrame> {
        // parse is only successful if there were no errors _and_ it consumed
        // all of `buf` (Inner calls us with exactly the size of `buf` we said
        // we need from parse_size()).
        if let Ok((rest, frame)) = parse_frame(buf) {
            if rest.is_empty() {
                return Ok(frame);
            }
        }
        Err(ErrorKind::ReceivedMalformed)?
    }
}

struct Inner<Kind: FrameKind> {
    buf: InputBuffer,
    phantom: PhantomData<Kind>,
}

impl<Kind: FrameKind> Inner<Kind> {
    fn new() -> Inner<Kind> {
        Inner {
            buf: InputBuffer::new(),
            phantom: PhantomData,
        }
    }

    fn read_from<S, F>(&mut self, stream: &mut S, mut handler: F) -> Result<usize>
    where
        S: io::Read,
        F: FnMut(Kind::Frame) -> Result<()>,
    {
        let mut bytes_read = 0;

        loop {
            let bytes = self.buf.bytes();
            let frame_size = Kind::parse_size(bytes);
            let mut reserve = MIN_READ;

            // if we already have enough data buffered to read a frame, do that before
            // trying to read from the stream.
            if let Some(frame_size) = frame_size {
                if bytes.len() >= frame_size {
                    let frame = Kind::parse_frame(&bytes[..frame_size])?;
                    handler(frame)?;
                    self.buf.advance(frame_size);
                    continue;
                } else {
                    // not enough data, but we know how much we need; try to read that
                    // much from the stream if it's larger than MIN_READ
                    reserve = usize::max(MIN_READ, frame_size);
                }
            }

            // need to read more data from the stream to get to a frame
            match self.buf.prepare_reserve(reserve).read_from(stream) {
                Ok(0) => return Err(ErrorKind::UnexpectedSocketClose)?,
                Ok(n) => {
                    trace!("read {} bytes", n);
                    bytes_read += n;
                }
                Err(err) => match err.kind() {
                    io::ErrorKind::WouldBlock => return Ok(bytes_read),
                    _ => return Err(err.context(ErrorKind::Io))?,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ErrorKind, FrameKind, Inner, Result};
    use crate::Error;
    use mockstream::FailingMockStream;
    use std::io::{self, Cursor, Read};

    struct FakeFrameKind {}

    impl FrameKind for FakeFrameKind {
        type Frame = Vec<u8>;

        fn parse_size(buf: &[u8]) -> Option<usize> {
            if buf.len() >= 2 {
                Some(buf[1] as usize)
            } else {
                None
            }
        }

        fn parse_frame(buf: &[u8]) -> Result<Self::Frame> {
            assert!(buf.len() == buf[1] as usize);
            if buf.len() == 6 && &buf[2..] == b"fail" {
                Err(ErrorKind::ReceivedMalformed)?
            } else {
                Ok(Vec::from(buf))
            }
        }
    }

    fn make_buffer() -> Inner<FakeFrameKind> {
        Inner::new()
    }

    fn would_block() -> FailingMockStream {
        FailingMockStream::new(io::ErrorKind::WouldBlock, "", 1)
    }

    #[test]
    fn full_frame_available() {
        let frame0 = b"a\x04aa";
        let mut c = Cursor::new(frame0).chain(would_block());

        let mut got = None;
        let mut buf = make_buffer();
        let n = buf
            .read_from(&mut c, |f| {
                got = Some(f);
                Ok(())
            })
            .unwrap();

        assert_eq!(n, 4);
        assert_eq!(got, Some(Vec::from(&frame0[..])));
    }

    #[test]
    fn two_full_frames_available() {
        let frame0 = b"a\x04aa";
        let frame1 = b"b\x04bb";
        let mut c = Cursor::new(frame0)
            .chain(Cursor::new(frame1))
            .chain(would_block());

        let mut got = Vec::new();
        let mut buf = make_buffer();
        let n = buf.read_from(&mut c, |f| Ok(got.push(f))).unwrap();

        assert_eq!(n, 8);
        assert_eq!(got, vec![Vec::from(&frame0[..]), Vec::from(&frame1[..])]);
    }

    #[test]
    fn partial_first_frame() {
        let mut c = Cursor::new(b"a\x04")
            .chain(would_block())
            .chain(Cursor::new(b"aa"))
            .chain(would_block());

        let mut got = None;
        let mut buf = make_buffer();
        let n = buf
            .read_from(&mut c, |f| {
                got = Some(f);
                Ok(())
            })
            .unwrap();
        assert_eq!(n, 2);
        assert!(got.is_none());

        let n = buf
            .read_from(&mut c, |f| {
                got = Some(f);
                Ok(())
            })
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(got, Some(Vec::from("a\x04aa".as_bytes())));
    }

    #[test]
    fn split_frames() {
        let mut c = Cursor::new(b"a\x04")
            .chain(would_block())
            .chain(Cursor::new(b"aab\x04b"))
            .chain(would_block())
            .chain(Cursor::new(b"bc\x04"))
            .chain(would_block());

        let mut got = Vec::new();
        let mut buf = make_buffer();
        let n = buf.read_from(&mut c, |f| Ok(got.push(f))).unwrap();
        assert_eq!(n, 2);
        assert!(got.is_empty());

        let n = buf.read_from(&mut c, |f| Ok(got.push(f))).unwrap();
        assert_eq!(n, 5);
        assert_eq!(got, vec![Vec::from("a\x04aa".as_bytes())]);

        let n = buf.read_from(&mut c, |f| Ok(got.push(f))).unwrap();
        assert_eq!(n, 3);
        assert_eq!(
            got,
            vec![
                Vec::from("a\x04aa".as_bytes()),
                Vec::from("b\x04bb".as_bytes())
            ]
        );
    }

    #[test]
    fn parse_fail() {
        let mut c = Cursor::new(b"x\x06fail").chain(would_block());

        let mut buf = make_buffer();
        let res = buf.read_from(&mut c, |_| panic!("should not be called"));
        assert!(res.is_err());
        assert_eq!(*res.unwrap_err().kind(), ErrorKind::ReceivedMalformed);
    }

    #[test]
    fn callback_fail() {
        let mut c = Cursor::new(b"a\x04aa").chain(would_block());

        // use __Nonexhaustive as "some non-parsing, non-I/O error"
        let mut buf = make_buffer();
        let res = buf.read_from(&mut c, |_| Err(Error::from(ErrorKind::__Nonexhaustive)));
        assert!(res.is_err());
        assert_eq!(*res.unwrap_err().kind(), ErrorKind::__Nonexhaustive);
    }

    #[test]
    fn eof_fail() {
        let mut c = Cursor::new(b"a\x04a");

        let mut buf = make_buffer();
        let res = buf.read_from(&mut c, |_| panic!("should not be called"));
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::UnexpectedSocketClose);
    }

    #[test]
    fn io_fail() {
        let mut c = Cursor::new(b"a\x04a").chain(FailingMockStream::new(
            io::ErrorKind::ConnectionReset,
            "",
            1,
        ));

        let mut buf = make_buffer();
        let res = buf.read_from(&mut c, |_| panic!("should not be called"));
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::Io);
    }
}
