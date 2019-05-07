# Version 0.2.2 (2019-05-07)

* Modify how we register the mio socket to avoid getting spurious wakeups before
  the socket is actually connected on Windows (hopefully fixes #8).

# Version 0.2.1 (2019-04-09)

* Fix bug when publishing a message with length 0 (or exactly equal to a
  multiple of the connection's `frame_max`).
* Fix `Channel::exchange_declare_nowait` and `Channel::exchange_declare_passive`
  (the implementations of these two methods were backwards).

# Version 0.2 (2019-04-03)

* Added support for publisher confirms
* Added `ConnectionOptions::information` for specifying the informational string amiquip reports to the server upon connection
* Made `native-tls` a default feature.
* Reworked the `Connection::open*` family of methods into `Connection::open*` which require TLS, and `Connection::insecure_open*` which allow unencrypted connections.

# Version 0.1.1 (2019-03-31)

* Initial release
