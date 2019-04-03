# amiquip

[![Travis Build Status](https://api.travis-ci.org/jgallagher/amiquip.svg?branch=master)](https://travis-ci.org/jgallagher/amiquip)
[![dependency status](https://deps.rs/repo/github/jgallagher/amiquip/status.svg)](https://deps.rs/repo/github/jgallagher/amiquip)
[![Latest Version](https://img.shields.io/crates/v/amiquip.svg)](https://crates.io/crates/amiquip)
[![Docs](https://docs.rs/amiquip/badge.svg)](https://docs.rs/amiquip)

amiquip is a RabbitMQ client written in pure Rust.

# Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
amiquip = "0.2"
```

For usage, see the [documentation](https://docs.rs/amiquip/) and
[examples](https://github.com/jgallagher/amiquip/tree/master/examples).

## TLS Support

By default, amiquip enables TLS support via the
[native-tls](https://crates.io/crates/native-tls) crate. You can disable
support for TLS by turning off default features:

```toml
[dependencies]
amiquip = { version = "0.2", default-features = false }
```

If you disable TLS support, the methods `Connection::open`,
`Connection::open_tuned`, and `Connection::open_tls_stream` will no longer be
available, as all three only allow secure connections. The methods
`Connection::insecure_open`, `Connection::insecure_open_tuned`, and
`Connection::insecure_open_stream` will still be available; these methods
support unencrypted connections.

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in amiquip by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
