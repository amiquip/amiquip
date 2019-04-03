// Port of https://www.rabbitmq.com/tutorials/tutorial-two-python.html. Start one
// or more of this example in shells, then run the work_queues_new_task example
// in another.
use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions, Result};
use std::thread;
use std::time::Duration;

const TASK_QUEUE: &'static str = "task_queue";

fn main() -> Result<()> {
    env_logger::init();

    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    // Declare the durable queue we will consume from.
    let queue = channel.queue_declare(
        TASK_QUEUE,
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
    )?;

    // Set QOS to only send us 1 message at a time.
    channel.qos(0, 1, false)?;

    // Start a consumer.
    let consumer = queue.consume(ConsumerOptions::default())?;
    println!("Waiting for messages. Press Ctrl-C to exit.");

    for (i, message) in consumer.receiver().iter().enumerate() {
        match message {
            ConsumerMessage::Delivery(delivery) => {
                let body = String::from_utf8_lossy(&delivery.body);
                println!("({:>3}) Received [{}]", i, body);

                // Sleep for n seconds, where n is the number of '.' chars in the body,
                // before we ack the message.
                let dits = delivery.body.iter().filter(|&&b| b == b'.').count();
                thread::sleep(Duration::from_secs(dits as u64));
                println!("({:>3}) ... done sleeping", i);

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
