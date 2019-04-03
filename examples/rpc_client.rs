// Port of https://www.rabbitmq.com/tutorials/tutorial-six-python.html. Start the
// rpc_server example in one shell, then run this example in another.
use amiquip::{
    AmqpProperties, Channel, Connection, Consumer, ConsumerMessage, ConsumerOptions, Exchange,
    Publish, Queue, QueueDeclareOptions, Result,
};
use uuid::Uuid;

struct FibonacciRpcClient<'a> {
    exchange: Exchange<'a>,
    queue: Queue<'a>,
    consumer: Consumer<'a>,
}

impl<'a> FibonacciRpcClient<'a> {
    fn new(channel: &Channel) -> Result<FibonacciRpcClient> {
        let exchange = Exchange::direct(&channel);

        let queue = channel.queue_declare(
            "",
            QueueDeclareOptions {
                exclusive: true,
                ..QueueDeclareOptions::default()
            },
        )?;
        let consumer = queue.consume(ConsumerOptions {
            no_ack: true,
            ..ConsumerOptions::default()
        })?;

        Ok(FibonacciRpcClient {
            exchange,
            queue,
            consumer,
        })
    }

    fn call(&self, n: u64) -> Result<String> {
        let correlation_id = format!("{}", Uuid::new_v4());
        self.exchange.publish(Publish::with_properties(
            format!("{}", n).as_bytes(),
            "rpc_queue",
            AmqpProperties::default()
                .with_reply_to(self.queue.name().to_string())
                .with_correlation_id(correlation_id.clone()),
        ))?;
        for message in self.consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    if delivery.properties.correlation_id().as_ref() == Some(&correlation_id) {
                        return Ok(String::from_utf8_lossy(&delivery.body).into());
                    }
                }
                other => {
                    println!("Consumer ended: {:?}", other);
                    break;
                }
            }
        }
        // This should really be an Err(..), but we don't want to go through the trouble
        // of defining a new error type for this example.
        Ok("ERROR: server failed to respond to RPC call".to_string())
    }
}

fn main() -> Result<()> {
    // Open connection.
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;

    let rpc_client = FibonacciRpcClient::new(&channel)?;

    println!("Requesting fib(30)");
    let result = rpc_client.call(30)?;
    println!("Got {}", result);

    connection.close()
}
