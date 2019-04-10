use crate::{Channel, Connection};
use std::env;
use std::sync::Once;

mod exchange;

static PRINT_WARNING: Once = Once::new();

fn with_test_url<F: FnOnce(&str)>(f: F) {
    match env::var("AMIQUIP_TEST_URL") {
        Ok(url) => f(&url),
        Err(env::VarError::NotPresent) => PRINT_WARNING.call_once(|| {
            println!("AMIQUIP_TEST_URL not defined - skipping integration tests");
        }),
        Err(env::VarError::NotUnicode(_)) => {
            panic!("AMIQUIP_TEST_URL exists but is not valid unicode")
        }
    }
}

fn with_conn<F: FnOnce(&mut Connection)>(f: F) {
    with_test_url(|url| {
        let mut conn = Connection::insecure_open(url).unwrap();
        f(&mut conn);
        conn.close().unwrap();
    })
}

fn with_chan<F: FnOnce(&Channel)>(f: F) {
    with_conn(|conn| {
        let chan = conn.open_channel(None).unwrap();
        f(&chan)
    })
}
