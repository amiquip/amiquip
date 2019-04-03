use amiquip::{
    Channel, ConfirmSmoother, Connection, Exchange, Publish, QueueDeclareOptions, Result,
};

fn publish_messages_with_confirmation(
    channel: &Channel,

    // queue to publish to
    target_queue: &str,

    // number of messages to publish before waiting for confirms
    group_size: usize,

    // number of groups to send before returning
    num_groups: usize,
) -> Result<()> {
    let exchange = Exchange::direct(channel);

    // register a pub confirm listener before putting the channel into confirm mode
    let confirm_listener = channel.listen_for_publisher_confirms()?;

    // put channel in confirm mode
    channel.enable_publisher_confirms()?;

    // create a confirm smoother so we can process perfectly sequential confirmations
    let mut confirm_smoother = ConfirmSmoother::new();

    for i in 0..num_groups {
        // publish group_size messages
        println!("publishing {} messages (group {})", group_size, i);
        for _ in 0..group_size {
            exchange.publish(Publish::new(b"hello", target_queue))?;
        }

        // wait for confirmation from the server for those 10 messages
        let mut confirmed = 0;
        while confirmed < group_size {
            let confirm = match confirm_listener.recv() {
                Ok(confirm) => confirm,
                Err(_) => {
                    // sender is gone - something has gone wrong with the channel;
                    // we'll see that error when we try to publish again or close it
                    break;
                }
            };
            println!("got raw confirm {:?} from server", confirm);
            for confirm in confirm_smoother.process(confirm) {
                println!("got smoothed confirm {:?}", confirm);
                confirmed += 1;
            }
        }
    }

    Ok(())
}

// Use technique discussed on Connection::close to capture connection errors
fn use_connection(connection: &mut Connection) -> Result<()> {
    let channel = connection.open_channel(None)?;

    // create an anonymous queue for us to publish to; we're not going to consume
    // from this, but we want it to be deleted once the example is done.
    let queue = channel.queue_declare(
        "",
        QueueDeclareOptions {
            exclusive: true,
            ..QueueDeclareOptions::default()
        },
    )?;

    // publish 3 groups of 10 messages, waiting for confirms between each group
    publish_messages_with_confirmation(&channel, queue.name(), 10, 3)?;

    channel.close()
}

fn main() -> Result<()> {
    let mut connection = Connection::insecure_open("amqp://guest:guest@localhost:5672")?;

    let result = use_connection(&mut connection);

    connection.close()?;

    result
}
