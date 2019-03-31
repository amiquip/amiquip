//! amiquip is a RabbitMQ client written in pure Rust.
//!
//! amiquip supports most features of the AMQP spec and some RabbitMQ extensions (see a list of
//! [currently unsupported features](#unsupported-features) below). It aims to be robust: problems
//! on a channel or connection should lead to a relevant [error](enum.ErrorKind.html) being raised.
//! Most errors, however, do result in effectively killing the channel or connection on which they
//! occur.
//!
//! TLS support is optionally available via the [native-tls](https://crates.io/crates/native-tls)
//! crate. To enable TLS support, turn on the `native-tls` feature of amiquip; e.g.,
//!
//! ```toml
//! # Cargo.toml
//! [dependencies]
//! amiquip = { version = "0.1", features = ["native-tls"] }
//! ```
//!
//! # Examples
//!
//! A "hello world" publisher:
//!
//! ```rust,no_run
//! use amiquip::{Connection, Exchange, Publish, Result};
//!
//! fn main() -> Result<()> {
//!     // Open connection.
//!     let mut connection = Connection::open("amqp://guest:guest@localhost:5672")?;
//!
//!     // Open a channel - None says let the library choose the channel ID.
//!     let channel = connection.open_channel(None)?;
//!
//!     // Get a handle to the direct exchange on our channel.
//!     let exchange = Exchange::direct(&channel);
//!
//!     // Publish a message to the "hello" queue.
//!     exchange.publish(Publish::new("hello there".as_bytes(), "hello"))?;
//!
//!     connection.close()
//! }
//! ```
//!
//! A corresponding "hello world" consumer:
//!
//! ```rust,no_run
//! // Port of https://www.rabbitmq.com/tutorials/tutorial-one-python.html. Run this
//! // in one shell, and run the hello_world_publish example in another.
//! use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions, Result};
//!
//! fn main() -> Result<()> {
//!     // Open connection.
//!     let mut connection = Connection::open("amqp://guest:guest@localhost:5672")?;
//!
//!     // Open a channel - None says let the library choose the channel ID.
//!     let channel = connection.open_channel(None)?;
//!
//!     // Declare the "hello" queue.
//!     let queue = channel.queue_declare("hello", QueueDeclareOptions::default())?;
//!
//!     // Start a consumer.
//!     let consumer = queue.consume(ConsumerOptions::default())?;
//!     println!("Waiting for messages. Press Ctrl-C to exit.");
//!
//!     for (i, message) in consumer.receiver().iter().enumerate() {
//!         match message {
//!             ConsumerMessage::Delivery(delivery) => {
//!                 let body = String::from_utf8_lossy(&delivery.body);
//!                 println!("({:>3}) Received [{}]", i, body);
//!                 consumer.ack(delivery)?;
//!             }
//!             other => {
//!                 println!("Consumer ended: {:?}", other);
//!                 break;
//!             }
//!         }
//!     }
//!
//!     connection.close()
//! }
//! ```
//!
//! Both of these examples are ports of the [RabbitMQ Hello World
//! tutorial](https://www.rabbitmq.com/tutorials/tutorial-one-python.html). Additional examples,
//! including ports of the other tutorials from that series, [are also
//! available](https://github.com/jgallagher/amiquip/tree/master/examples).
//!
//! # Design Details
//!
//! When a [connection](struct.Connection.html) is opened, a thread is created to manage all reads
//! and writes on the socket. Other documentation and code refers to this as the "I/O thread". Each
//! connection has exactly one I/O thread. The I/O thread uses [mio](https://crates.io/crates/mio)
//! to drive nonblocking connection. The connection handle and other related handles (particularly
//! [channels](struct.Channel.html)) communicate with the I/O thread via [mio sync
//! channels](https://crates.io/crates/mio_extras) (to the I/O thread) and [crossbeam
//! channels](https://crates.io/crates/crossbeam-channel) (from the I/O thread).
//!
//! Heartbeats are entirely managed by the I/O thread; if heartbeats are enabled and the I/O thread
//! fails to receive communication from the server for too long, it will close the connection.
//!
//! amiquip uses the [log](https://crates.io/crates/log) crate internally. At the `trace` log
//! level, amiquip is quite noisy, but this may be valuable in debugging connection problems.
//!
//! ## Thread Support
//!
//! A [`Connection`](struct.Connection.html) is effectively bound to a single thread (it
//! technically implements both `Send` and `Sync`, but most relevant methods take `&mut self`). A
//! connection can open many [`Channel`](struct.Channel.html)s; a channel can only be used by a
//! single thread (it implements `Send` but not `Sync`). There is no tie between a connection and
//! its channels at the type system level; if the connection is closed (either intentionally or
//! because of an error), all the channels that it opened will end up returning errors shortly
//! thereafter. See the discussion on [`Connection::close`](struct.Connection.html#method.close)
//! for a bit more information about that.
//!
//! A channel is able to produce other handles ([queues](struct.Queue.html),
//! [exchanges](struct.Exchange.html), and [consumers](struct.Consumer.html)). These are mostly
//! thin convenience wrappers around the channel, and they do hold a reference back to the channel
//! that created them. This means if you want to use the connection to open a channel on one thread
//! then move it to another thread to do work, you will need to declare queues, exchanges, and
//! consumers from the thread where work will be done; e.g.,
//!
//! ```rust
//! use amiquip::{Connection, QueueDeclareOptions, ConsumerOptions, Result};
//! use std::thread;
//!
//! fn run_connection(mut connection: Connection) -> Result<()> {
//!     let channel = connection.open_channel(None)?;
//!
//!     // Declaring the queue outside the thread spawn will fail, as it cannot
//!     // be moved into the thread. Instead, wait to declare until inside the new thread.
//!
//!     // Would fail:
//!     // let queue = channel.queue_declare("hello", QueueDeclareOptions::default())?;
//!     thread::spawn(move || -> Result<()> {
//!         // Instead, declare once the channel is moved into this thread.
//!         let queue = channel.queue_declare("hello", QueueDeclareOptions::default())?;
//!         let consumer = queue.consume(ConsumerOptions::default())?;
//!         for message in consumer.receiver().iter() {
//!             // do something with message...
//!             # let _ = message;
//!         }
//!         Ok(())
//!     });
//!
//!     // do something to keep the connection open; if we drop the connection here,
//!     // it will be closed, killing the channel that we just moved into a new thread
//!     # Ok(())
//! }
//! ```
//!
//! # Unsupported Features
//!
//! * Connection recovery. If something goes wrong with a connection, it will be torn down, and
//! errors will be returned from calls on the connection and any other handles (channels,
//! consumers, etc.). A connection recovery strategy could be implemented on top of amiquip.
//! * Channel-level flow control. RabbitMQ, as of version 3.7.14 in March 2019, [does not
//! support](https://www.rabbitmq.com/specification.html#rules) clients requesting channel flow
//! control, and it does not send channel flow control messages to clients (using TCP backpressure
//! instead).
//! * Setting up a [`Consumer`](struct.Consumer.html) with a user-provided consumer tag. If this is
//! something you need, please [file an issue](https://github.com/jgallagher/amiquip/issues).
//! * `nowait` variants of [`Queue::consume`](struct.Queue.html#method.consume) and
//! [`Consumer::cancel`](struct.Consumer.html#method.cancel). It is unlikely support for these will
//! be added, as the synchronous versions are used to set up internal channels for consumer
//! messages.
//! * `nowait` variant of [`Channel::recover`](struct.Channel.html#method.recover). The
//! asynchronous version of `recover` is marked as deprecated in RabbitMQ's AMQP reference.

mod auth;
mod channel;
mod connection;
mod connection_options;
mod consumer;
mod delivery;
mod errors;
mod exchange;
mod frame_buffer;
mod get;
mod heartbeats;
mod io_loop;
mod queue;
mod return_;
mod serialize;
mod stream;

pub use auth::{Auth, Sasl};
pub use channel::Channel;
pub use connection::{Connection, ConnectionBlockedNotification, ConnectionTuning};
pub use connection_options::ConnectionOptions;
pub use consumer::{Consumer, ConsumerMessage, ConsumerOptions};
pub use delivery::Delivery;
pub use errors::{Error, ErrorKind, Result};
pub use exchange::{Exchange, ExchangeDeclareOptions, ExchangeType, Publish};
pub use get::Get;
pub use queue::{Queue, QueueDeclareOptions, QueueDeleteOptions};
pub use return_::Return;
pub use stream::IoStream;

#[cfg(feature = "native-tls")]
pub use stream::TlsConnector;

pub use amq_protocol::protocol::basic::AMQPProperties as AmqpProperties;
pub use amq_protocol::types::AMQPValue as AmqpValue;
pub use amq_protocol::types::FieldTable;

#[allow(dead_code)]
mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
