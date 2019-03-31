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
