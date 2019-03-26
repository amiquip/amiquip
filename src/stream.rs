use mio::net::TcpStream;
use mio::Evented;
use std::io::{Read, Write};

pub trait IoStream: Read + Write + Evented + Send + 'static {}

impl IoStream for TcpStream {}
