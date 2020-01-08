use log::trace;
use mio_extras::timer::{Timeout, Timer};
use std::fmt::Debug;
use std::time::{Duration, Instant};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum HeartbeatState {
    StillRunning,
    Expired,
}

#[derive(Debug)]
pub struct Heartbeat<T: Copy + Debug> {
    val: T,
    last: Instant,
    timeout: Timeout,
    interval: Duration,
}

impl<T: Copy + Debug> Heartbeat<T> {
    pub fn start(val: T, interval: Duration, timer: &mut Timer<T>) -> Heartbeat<T> {
        assert!(
            interval > Duration::from_millis(0),
            "timer interval cannot be 0"
        );
        let last = Instant::now();
        let timeout = timer.set_timeout(interval, val);
        Heartbeat {
            val,
            last,
            timeout,
            interval,
        }
    }

    pub fn record_activity(&mut self) {
        self.last = Instant::now();
    }

    pub fn fire(&mut self, timer: &mut Timer<T>) -> HeartbeatState {
        timer.cancel_timeout(&self.timeout);

        // see if the heartbeat timer has expired (in which case we'll restart for
        // the full interval) or if there have been intervening record_activity()
        // calls for activity (in which case we'll restart for the remaining time).
        //
        // We'll add a bit of fudge in the comparison to handle imprecise wakeups;
        // during unit tests sometimes we wake up <1ms before expiration, but we
        // want to count that as expired anyway. AMQP heartbeats are scaled in
        // seconds, so a few ms is harmless.
        let elapsed = self.last.elapsed();
        let (when, state) = if self.interval <= elapsed + Duration::from_millis(5) {
            (self.interval, HeartbeatState::Expired)
        } else {
            (self.interval - elapsed, HeartbeatState::StillRunning)
        };

        trace!(
            "setting new heartbeat timer {:?} for {:?} (interval = {:?}, elapsed = {:?})",
            self.val,
            when,
            self.interval,
            elapsed
        );
        self.timeout = timer.set_timeout(when, self.val);
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mio::{Events, Poll, PollOpt, Ready, Token};
    use mio_extras::timer::Builder;

    struct Harness {
        poll: Poll,
        events: Events,
        timer: Timer<u32>,
    }

    impl Harness {
        const TOKEN: Token = Token(0);

        fn new() -> Harness {
            let poll = Poll::new().unwrap();
            let events = Events::with_capacity(16);
            let timer = Builder::default().tick_duration(millis(10)).build();
            poll.register(&timer, Self::TOKEN, Ready::readable(), PollOpt::edge())
                .unwrap();
            Harness {
                poll,
                events,
                timer,
            }
        }

        fn poll(&mut self, timeout: Duration) {
            self.poll.poll(&mut self.events, Some(timeout)).unwrap();
        }

        fn poll_until_fire(&mut self, h: &mut Heartbeat<u32>) -> HeartbeatState {
            loop {
                self.poll.poll(&mut self.events, None).unwrap();
                for ev in &self.events {
                    assert_eq!(ev.token(), Self::TOKEN);
                    if self.timer.poll().is_some() {
                        return h.fire(&mut self.timer);
                    }
                }
            }
        }
    }

    fn millis(u: u64) -> Duration {
        Duration::from_millis(u)
    }

    fn assert_duration_is_about(one: Duration, two: Duration) {
        // NOTE: assumes two is >= 50ms, or will panic on the subtraction.
        // Fine for all our tests which are 100s of ms test durations
        assert!(one > two - millis(50));
        assert!(one < two + millis(50));
    }

    #[test]
    fn fire_after_expiration() {
        let mut t = Harness::new();
        let mut h = Heartbeat::start(0, millis(400), &mut t.timer);
        let start = Instant::now();

        let state = t.poll_until_fire(&mut h);

        assert_duration_is_about(start.elapsed(), millis(400));
        assert_eq!(state, HeartbeatState::Expired);
    }

    #[test]
    fn fire_after_activity() {
        let mut t = Harness::new();
        let mut h = Heartbeat::start(0, millis(400), &mut t.timer);
        let start = Instant::now();

        // timer shouldn't fire yet
        t.poll(millis(200));
        assert_duration_is_about(start.elapsed(), millis(200));
        assert!(t.events.is_empty());
        h.record_activity();

        // timer should fire, but should be set back to "still running"
        let state = t.poll_until_fire(&mut h);
        assert_duration_is_about(start.elapsed(), millis(400));
        assert_eq!(state, HeartbeatState::StillRunning);

        // timer should fire again and expire in just ~200ms
        let state = t.poll_until_fire(&mut h);
        assert_duration_is_about(start.elapsed(), millis(600));
        assert_eq!(state, HeartbeatState::Expired);
    }
}
