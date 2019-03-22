use crate::heartbeats::Heartbeat;
use log::trace;
use mio_extras::timer::Timer;
use std::time::Duration;

pub(super) use crate::heartbeats::HeartbeatState;

const MAX_MISSED_SERVER_HEARTBEATS: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum HeartbeatKind {
    Rx,
    Tx,
}

struct RxTxHeartbeat {
    rx: Heartbeat<HeartbeatKind>,
    tx: Heartbeat<HeartbeatKind>,
}

impl RxTxHeartbeat {
    fn new(timer: &mut Timer<HeartbeatKind>, interval: Duration) -> RxTxHeartbeat {
        let rx = Heartbeat::start(
            HeartbeatKind::Rx,
            MAX_MISSED_SERVER_HEARTBEATS * interval,
            timer,
        );
        let tx = Heartbeat::start(HeartbeatKind::Tx, interval, timer);
        RxTxHeartbeat { rx, tx }
    }
}

#[derive(Default)]
pub(super) struct HeartbeatTimers {
    pub(super) timer: Timer<HeartbeatKind>,
    heartbeats: Option<RxTxHeartbeat>,
}

impl HeartbeatTimers {
    pub(super) fn record_rx_activity(&mut self) {
        if let Some(hb) = &mut self.heartbeats {
            trace!("recording activity for rx heartbeat");
            hb.rx.record_activity();
        }
    }

    pub(super) fn record_tx_activity(&mut self) {
        if let Some(hb) = &mut self.heartbeats {
            trace!("recording activity for tx heartbeat");
            hb.tx.record_activity();
        }
    }

    pub(super) fn start(&mut self, interval: Duration) {
        assert!(
            self.heartbeats.is_none(),
            "heartbeat timer started multiple times"
        );
        self.heartbeats = Some(RxTxHeartbeat::new(&mut self.timer, interval));
    }

    pub(super) fn fire_rx(&mut self) -> HeartbeatState {
        self.heartbeats
            .as_mut()
            .expect("fire_rx called on empty heartbeats")
            .rx
            .fire(&mut self.timer)
    }

    pub(super) fn fire_tx(&mut self) -> HeartbeatState {
        self.heartbeats
            .as_mut()
            .expect("fire_tx called on empty heartbeats")
            .tx
            .fire(&mut self.timer)
    }
}
