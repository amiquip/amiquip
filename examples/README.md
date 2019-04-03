# Examples

This directory contains several toy examples for amiquip:

* [RabbitMQ Tutorial Examples](#rabbitmq-tutorial-examples) - ports of RabbitMQ's introductory tutorials
* [Publisher Confirm Example](#publisher-confirm-example) - example of enabling and using publisher confirms

# RabbitMQ Tutorial Examples

This directory contains ports of [6 RabbitMQ Python
tutorials](https://www.rabbitmq.com/getstarted.html). Each of them is a pair of
programs that should be run in different shells. Examples of running each is
below.

## Hello World

Run the consumer in one shell:

```
> cargo run --example hello_world_consume
```

And publish messages to it in another shell:

```
> cargo run --example hello_world_publish
```

## Work Queues

Start a worker in one shell (or multiple workers, to make things more interesting).

```
> cargo run --example work_queues_worker
```

Send "tasks" to the workers. In this example, a task is just a string, but the
worker will sleep for a number of seconds equal to the number of `.` characters
in the string.

```
> cargo run --example work_queues_new_task -- Short task.
Sent message [Short task.]
> cargo run --example work_queues_new_task -- Medium task...
Sent message [Medium task...]
> cargo run --example work_queues_new_task -- Long task..........
Send message [Long task..........]
```

You should see the tasks go to the first available worker, or bounce back and
forth if both workers are available.

## Publish / Subscribe

Start the log receiver in one shell.

```
> cargo run --example pubsub_receive_logs
```

Run the log emitter in another shell.

```
> cargo run --example pubsub_emit_log -- This is a message.
Sent [This is a message.]
```

## Routing

Start the log receiver in one shell, specifying the log levels you want to receive.

```
> cargo run --example routing_receive_logs_direct -- info warning
created exclusive queue amq.gen-aEt2-fCvZ64E487hKpAuGg
Waiting for logs. Press Ctrl-C to exit.
```

With that running, in another shell publish a log message.

```
> cargo run --example routing_emit_log_direct -- info This is a message.
Sent info:This is a message.
```

If the log level (the first arg) matches one of the log levels you used to
start the receiver, you should see the log message in the receiver shell.

## Topics

Start the log receiver in one shell, specifying topical binding keys.

```
# Receive all logs
> cargo run --example topics_receive_logs -- '#'

# Receive all logs from the facility "kern"
> cargo run --example topics_receive_logs -- 'kern.*'

# Receive all critical logs
> cargo run --example topics_receive_logs -- '*.critical'

# Receive all kern or critical logs
> cargo run --example topics_receive_logs -- 'kern.*' '*.critical'
```

To emit a log with routing key `kern.critical`:

```
> cargo  run --example topics_emit_log -- kern.critical "A critical kernel error"
Sent kern.critical:A critical kernel error
```

## RPC

Start the RPC server in one shell.

```
> cargo run --example rpc_server
```

Run the client in another shell.

```
> cargo run --example rpc_client
Requesting fib(30)
Got 832040
```

# Publisher Confirm Example

The `publisher_confirms` example attempts to publish 30 messages to a temporary queue, pausing after each 10 to wait for the server to confirm receipt of those 10 messages. Example output:

```
> cargo run --example publisher_confirms
publishing 10 messages (group 0)
got raw confirm Ack(ConfirmPayload { delivery_tag: 5, multiple: true }) from server
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 1, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 2, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 3, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 4, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 5, multiple: false })
got raw confirm Ack(ConfirmPayload { delivery_tag: 10, multiple: true }) from server
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 6, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 7, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 8, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 9, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 10, multiple: false })
publishing 10 messages (group 1)
got raw confirm Ack(ConfirmPayload { delivery_tag: 15, multiple: true }) from server
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 11, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 12, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 13, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 14, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 15, multiple: false })
got raw confirm Ack(ConfirmPayload { delivery_tag: 20, multiple: true }) from server
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 16, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 17, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 18, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 19, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 20, multiple: false })
publishing 10 messages (group 2)
got raw confirm Ack(ConfirmPayload { delivery_tag: 25, multiple: true }) from server
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 21, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 22, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 23, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 24, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 25, multiple: false })
got raw confirm Ack(ConfirmPayload { delivery_tag: 30, multiple: true }) from server
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 26, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 27, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 28, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 29, multiple: false })
got smoothed confirm Ack(ConfirmPayload { delivery_tag: 30, multiple: false })
```

You should see the same "got smoothed confirm" messages, but you may see different "got raw confirm" messages. The "raw confirm" logs are the exact messages coming from the server; the "smoothed confirm" logs are from passing those raw messages through a `ConfirmSmoother` to get singular, sequential confirmation messages. In the above run, the server sent a confirmation every 5 messages.
