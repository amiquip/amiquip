// Port of https://www.rabbitmq.com/tutorials/tutorial-two-python.html. Start one
// or more work_queues_worker examples in other shells, then run this example.
use amiquip::{AmqpProperties, Connection, Exchange, Publish, QueueDeclareOptions, Result};
use std::env;

const TASK_QUEUE: &'static str = "task_queue";

fn main() -> Result<()> {
    env_logger::init();

    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    // Declare the durable queue we are going to publish to, but discard the handle;
    // we aren't going to do anything with it.
    let _ = channel.queue_declare(
        TASK_QUEUE,
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
    )?;

    // Get a handle to the direct exchange on our channel.
    let exchange = Exchange::direct(&channel);

    // Publish a message to the "hello" queue.
    let mut message = env::args().skip(1).collect::<Vec<_>>().join(" ");
    if message.is_empty() {
        message = "Hello world.".to_string();
    }

    exchange.publish(Publish::with_properties(
        message.as_bytes(),
        TASK_QUEUE,
        // delivery_mode 2 makes message persistent
        AmqpProperties::default().with_delivery_mode(2),
    ))?;
    println!("Sent message [{}]", message);

    connection.close()
}
