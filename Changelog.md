# Version UPCOMING

* Added support for publisher confirms
* Added `ConnectionOptions::information` for specifying the informational string amiquip reports to the server upon connection
* Made `native-tls` a default feature.
* Reworked the `Connection::open*` family of methods into `Connection::open*` which require TLS, and `Connection::insecure_open*` which allow unencrypted connections.

# Version 0.1.1 (2019-03-31)

* Initial release
