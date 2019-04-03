// Port of https://www.rabbitmq.com/tutorials/tutorial-five-python.html. Start the
// topics_receive_logs example in one shell, then run this example in another.
use amiquip::{Connection, ExchangeDeclareOptions, ExchangeType, Publish, Result};
use std::env;

fn main() -> Result<()> {
    env_logger::init();

    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    // Declare the exchange we will publish to.
    let exchange = channel.exchange_declare(
        ExchangeType::Topic,
        "topic_logs",
        ExchangeDeclareOptions::default(),
    )?;

    let mut args = env::args().skip(1);
    let routing_key = args.next().unwrap_or_else(|| "anonymous.info".to_string());

    let mut message = args.collect::<Vec<_>>().join(" ");
    if message.is_empty() {
        message = "Hello world!".to_string();
    }

    exchange.publish(Publish::new(message.as_bytes(), routing_key.clone()))?;
    println!("Sent {}:{}", routing_key, message);

    connection.close()
}
