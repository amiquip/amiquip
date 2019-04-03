// Port of https://www.rabbitmq.com/tutorials/tutorial-six-python.html. Start this
// example in one shell, then the rpc_client example in another.
use amiquip::{
    AmqpProperties, Connection, ConsumerMessage, ConsumerOptions, Exchange, Publish,
    QueueDeclareOptions, Result,
};

fn fib(n: u64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        n => fib(n - 1) + fib(n - 2),
    }
}

fn main() -> Result<()> {
    env_logger::init();

    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    // Get a handle to the default direct exchange.
    let exchange = Exchange::direct(&channel);

    // Declare the queue that will receive RPC requests.
    let queue = channel.queue_declare("rpc_queue", QueueDeclareOptions::default())?;

    // Start a consumer.
    let consumer = queue.consume(ConsumerOptions::default())?;
    println!("Awaiting RPC requests");

    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                println!("({:>3}) fib({})", i, body);

                let (reply_to, corr_id) = match (
                    delivery.properties.reply_to(),
                    delivery.properties.correlation_id(),
                ) {
                    (Some(r), Some(c)) => (r.clone(), c.clone()),
                    _ => {
                        println!("received delivery without reply_to or correlation_id");
                        consumer.ack(delivery)?;
                        continue;
                    }
                };

                let response = match body.parse() {
                    Ok(n) => format!("{}", fib(n)),
                    Err(_) => "invalid input".to_string(),
                };

                exchange.publish(Publish::with_properties(
                    response.as_bytes(),
                    reply_to,
                    AmqpProperties::default().with_correlation_id(corr_id),
                ))?;
                consumer.ack(delivery)?;
            }
            other => {
                println!("Consumer ended: {:?}", other);
                break;
            }
        }
    }

    connection.close()
}
