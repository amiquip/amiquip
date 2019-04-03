// Port of https://www.rabbitmq.com/tutorials/tutorial-four-python.html. Start the
// routing_receive_logs_direct example in another shell, then run this example.
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
        ExchangeType::Direct,
        "direct_logs",
        ExchangeDeclareOptions::default(),
    )?;

    let mut args = env::args().skip(1);
    let severity = args.next().unwrap_or_else(|| "info".to_string());

    let mut message = args.collect::<Vec<_>>().join(" ");
    if message.is_empty() {
        message = "Hello world!".to_string();
    }

    exchange.publish(Publish::new(message.as_bytes(), severity.clone()))?;
    println!("Sent {}:{}", severity, message);

    connection.close()
}
