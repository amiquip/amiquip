use super::with_chan;
use crate::Publish;

#[test]
fn test_publish_empty() {
    with_chan(|chan| {
        for _ in 0..2 {
            chan.basic_publish("", Publish::new(&[], "does.not.exist"))
                .unwrap();
        }
    })
}
