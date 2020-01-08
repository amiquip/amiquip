# Version 0.3.3 (2020-01-07)

* Add documentation examples of using the `arguments` fields of
  `QueueDeclareOptions` and `ConsumerOptions`.

# Version 0.3.2 (2019-08-23)

* Restore `#[doc(hidden)]` attribute with upgrade to snafu 0.4.4.
* Fix `basic_publish`'s immediate/mandatory mixup.

# Version 0.3.1 (2019-08-06)

* Remove `#[doc(hidden)]` attribute to work around https://github.com/shepmaster/snafu/issues/139.

# Version 0.3 (2019-07-14)

* Internally, `Error` is now created via `snafu` instead of `failure`. This leads to three breaking changes:
  * `Error` no longer implements `Clone` or `PartialEq`, but _does_ implement `std::error::Error`.
  * The `ErrorKind` helper enum no longer exists; `Error` is itself an enum.
  * The definition of most error cases has changes (and there are considerably more of them).
* Add missing `Debug`, `Clone`, `Copy`, and/or `Default` derivations for `ExchangeType`, `Publish`, and `QueueDeleteOption`.

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
