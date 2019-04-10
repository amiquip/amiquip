use super::with_chan;
use crate::{ExchangeDeclareOptions, ExchangeType, Publish};

#[test]
fn test_publish_empty() {
    with_chan(|chan| {
        for _ in 0..2 {
            chan.basic_publish("", Publish::new(&[], "does.not.exist"))
                .unwrap();
        }
    })
}

#[test]
fn test_declare() {
    let name = "amiquip-test-declare";

    with_chan(|chan| {
        let ex1 = chan
            .exchange_declare(
                ExchangeType::Direct,
                name,
                ExchangeDeclareOptions::default(),
            )
            .unwrap();
        ex1.publish(Publish::new(&[], "does.not.exist")).unwrap();

        let ex2 = chan
            .exchange_declare_nowait(
                ExchangeType::Direct,
                name,
                ExchangeDeclareOptions::default(),
            )
            .unwrap();
        ex2.publish(Publish::new(&[], "does.not.exist")).unwrap();

        let ex3 = chan.exchange_declare_passive(name).unwrap();
        ex3.publish(Publish::new(&[], "does.not.exist")).unwrap();

        assert_eq!(ex1.name(), ex2.name());
        assert_eq!(ex1.name(), ex3.name());
    })
}
