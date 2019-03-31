# amiquip

<!-- Uncomment once these exist.
[![Travis Build Status](https://api.travis-ci.org/jgallagher/amiquip.svg?branch=master)](https://travis-ci.org/jgallagher/amiquip)
[![dependency status](https://deps.rs/repo/github/jgallagher/amiquip/status.svg)](https://deps.rs/repo/github/jgallagher/amiquip)
[![Latest Version](https://img.shields.io/crates/v/amiquip.svg)](https://crates.io/crates/amiquip)
[![Docs](https://docs.rs/amiquip/badge.svg)](https://docs.rs/amiquip)
-->

amiquip is a RabbitMQ client written in pure Rust.

# Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
amiquip = "0.1"
```

For usage, see the [documentation](#TODO-link-to-docs-rs) and
[examples](https://github.com/jgallagher/amiquip/tree/master/examples).

## Feature `native-tls`

By default, amiquip cannot use a TLS connection. To enable TLS connections, use
the `native-tls` feature:

```toml
[dependencies]
amiquip = { version = "0.1", features = ["native_tls"] }
```

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
