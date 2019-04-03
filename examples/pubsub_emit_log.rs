// Port of https://www.rabbitmq.com/tutorials/tutorial-three-python.html. Start one
// or more pubsub_receive_logs examples in other shells, then run this example.
use amiquip::{Connection, ExchangeDeclareOptions, ExchangeType, Publish, Result};
use std::env;

fn main() -> Result<()> {
    env_logger::init();

    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    // Declare the fanout exchange we will publish to.
    let exchange = channel.exchange_declare(
        ExchangeType::Fanout,
        "logs",
        ExchangeDeclareOptions::default(),
    )?;

    // Publish a message to the logs queue.
    let mut message = env::args().skip(1).collect::<Vec<_>>().join(" ");
    if message.is_empty() {
        message = "info: Hello world!".to_string();
    }

    exchange.publish(Publish::new(message.as_bytes(), ""))?;
    println!("Sent [{}]", message);

    connection.close()
}
