use std::collections::HashMap;

/// Payload for a publisher confirmation message (either an [ack](enum.Confirm.html#variant.Ack) or
/// a [nack](enum.Confirm.html#variant.Nack)) from the server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConfirmPayload {
    /// The tag from the server. Tags are sequentially increasing integers beginning with
    /// 1 (once publisher confirms [are
    ///   enabled](struct.Channel.html#method.enable_publisher_confirms) on the channel.
    pub delivery_tag: u64,

    /// If true, the confirmation applies to all previously-unconfirmed messages with delivery tags
    /// less than or equal to this payload's [`delivery_tag`](#structfield.delivery_tag).
    pub multiple: bool,
}

/// A publisher confirmation message from the server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Confirm {
    /// Acknowledgment that the server has received the message(s) described by the associated
    /// payload. Note that acks do not necessarily imply that the messages have been handled by a
    /// consumer, merely that they have been received by the server.
    Ack(ConfirmPayload),

    /// Notification that the message(s) described by the associated payload have been rejected.
    Nack(ConfirmPayload),
}

/// Helper to smooth out of order and/or `multiple: true` publisher confirmation messages.
///
/// If publisher confirms are enabled, the server may confirm messages out of order and/or may
/// confirm multiple messages with a single [`Confirm`](enum.Confirm.html). `ConfirmSmoother`
/// exists to "smooth" server messages out into an always-increasing-by-one sequence of
/// confirmation messages.
///
/// # Example
///
/// ```rust
/// use amiquip::{Confirm, ConfirmSmoother};
/// use crossbeam_channel::Receiver;
///
/// // assume we've published n messages and want to wait for them to be confirmed
/// fn wait_for_publisher_confirms(n: usize, receiver: &Receiver<Confirm>) {
///     // NOTE: a new smoother assumes we will be receiving messages starting with
///     // delivery_tag = 1, so this method is only valid if called after the first n
///     // publishes on a channel. We could take a &mut ConfirmSmoother to be called
///     // multiple times in succession on the same channel.
///     let mut smoother = ConfirmSmoother::new();
///     let mut acked = 0;
///     let mut nacked = 0;
///     while acked + nacked < n {
///         // get a confirmation from the server; this may be out of order or a confirm
///         // for multiple messages in one payload
///         let raw_confirm = match receiver.recv() {
///             Ok(raw_confirm) => raw_confirm,
///             Err(_) => {
///                 // the I/O thread dropped the sending side; either an error has occurred
///                 // or another thread of ours closed the connection; either way we'll
///                 // stop waiting
///                 return;
///             }
///         };
///
///         // feed the raw confirm into the smoother. Two notes:
///         //   1. We must run the returned iterator to the end or risk missing confirms.
///         //   2. The iterator may produce 0, 1, or multiple independent confirmations.
///         //      They will all have multiple: false.
///         for confirm in smoother.process(raw_confirm) {
///             match confirm {
///                 Confirm::Ack(_) => {
///                     acked += 1;
///                 }
///                 Confirm::Nack(_) => {
///                     // server rejected message; need to do something else to
///                     // track which messages were rejected
///                     nacked += 1;
///                 }
///             }
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ConfirmSmoother {
    expected: u64,
    out_of_order: HashMap<u64, Confirm>,
}

impl Default for ConfirmSmoother {
    fn default() -> ConfirmSmoother {
        ConfirmSmoother::new()
    }
}

impl ConfirmSmoother {
    /// Create a new `ConfirmSmoother`. It expects the next (in absolute order) delivery tag
    /// received from the server to be `1`.
    pub fn new() -> ConfirmSmoother {
        ConfirmSmoother::with_expected_delivery_tag(1)
    }

    /// Create a new `ConfirmSmoother`. It expects the next (in absolute order) delivery tag
    /// received from the server to be `expected`.
    pub fn with_expected_delivery_tag(expected: u64) -> ConfirmSmoother {
        ConfirmSmoother {
            expected,
            out_of_order: HashMap::new(),
        }
    }

    /// Process a confirmation message from the server. Returns an iterator; each item returned by
    /// the iterator will be a single (i.e., `multiple: false`) [`Confirm`](enum.Confirm.html). You
    /// _must_ run the iterator to its completion or risk missing confirmations; future calls to
    /// `process` will not return an iterator that will repeat confirms that would have been
    /// returned by a previously returned iterator (even if that earlier iterator was dropped
    /// before it ran to completion).
    ///
    /// The returned iterator may have 0 items (if `confirm` is a non-multiple confirmation that is
    /// later than the next expected delivery tag), 1 item (if `confirm` exactly matches our next
    /// expected delivery tag and we had not previously seen the next tag), or multiple items (if
    /// `confirm` is a `multiple: true` confirmation or we've previously seen out-of-order tags
    /// that are next sequentially after `confirm`'s tag).
    pub fn process<'a>(&'a mut self, confirm: Confirm) -> impl Iterator<Item = Confirm> + 'a {
        match confirm {
            Confirm::Ack(inner) => self.new_iter(inner, Confirm::Ack),
            Confirm::Nack(inner) => self.new_iter(inner, Confirm::Nack),
        }
    }

    fn new_iter<'a>(
        &'a mut self,
        payload: ConfirmPayload,
        to_confirm: fn(ConfirmPayload) -> Confirm,
    ) -> impl Iterator<Item = Confirm> + 'a {
        Iter {
            parent: self,
            payload,
            next: None,
            to_confirm: move |tag| {
                to_confirm(ConfirmPayload {
                    delivery_tag: tag,
                    multiple: false,
                })
            },
            done: false,
        }
    }
}

struct Iter<'a, F: Fn(u64) -> Confirm> {
    parent: &'a mut ConfirmSmoother,
    payload: ConfirmPayload,
    next: Option<Confirm>,
    to_confirm: F,
    done: bool,
}

impl<'a, F> Drop for Iter<'a, F>
where
    F: Fn(u64) -> Confirm,
{
    fn drop(&mut self) {
        while !self.done {
            let _ = self.next();
        }
    }
}

impl<'a, F> Iterator for Iter<'a, F>
where
    F: Fn(u64) -> Confirm,
{
    type Item = Confirm;

    fn next(&mut self) -> Option<Confirm> {
        if self.done {
            return None;
        }

        let payload = self.payload;

        if payload.delivery_tag == self.parent.expected {
            // exact match - we'll return this tag, and set next to an out-of-order
            // entry for the next tag if we had one
            self.parent.expected += 1;
            self.next = self.parent.out_of_order.remove(&self.parent.expected);
            return Some((self.to_confirm)(payload.delivery_tag));
        }

        if payload.delivery_tag > self.parent.expected {
            // tag is in the future; if it's "multiple", keep sending all tags in
            // between where we are now and payload.delivery_tag
            if payload.multiple {
                let ret = (self.to_confirm)(self.parent.expected);
                self.parent.expected += 1;
                return Some(ret);
            } else {
                // if it's _not_ multiple, stash it away in out_of_order
                self.parent.out_of_order.insert(
                    payload.delivery_tag,
                    (self.to_confirm)(payload.delivery_tag),
                );
                self.done = true;
                return None;
            }
        }

        match self.next.take() {
            Some(next) => {
                // self.next is Some() only if a previous call to next() hit the tag==expected
                // case _and_ out_of_order held a confirm for the next expected tag
                self.parent.expected += 1;
                self.next = self.parent.out_of_order.remove(&self.parent.expected);
                Some(next)
            }
            None => {
                self.done = true;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single(delivery_tag: u64, f: fn(ConfirmPayload) -> Confirm) -> Confirm {
        f(ConfirmPayload {
            delivery_tag,
            multiple: false,
        })
    }
    fn multiple(delivery_tag: u64, f: fn(ConfirmPayload) -> Confirm) -> Confirm {
        f(ConfirmPayload {
            delivery_tag,
            multiple: true,
        })
    }

    #[test]
    fn simple_single() {
        let mut flat = ConfirmSmoother::new();
        let one = flat.process(single(1, Confirm::Ack));
        let expected = vec![single(1, Confirm::Ack)];
        assert_eq!(expected, one.collect::<Vec<_>>());
    }

    #[test]
    fn simple_multiple() {
        let mut flat = ConfirmSmoother::new();
        let three = flat.process(multiple(3, Confirm::Ack));
        let expected = (1..=3).map(|i| single(i, Confirm::Ack)).collect::<Vec<_>>();
        assert_eq!(expected, three.collect::<Vec<_>>());
    }

    #[test]
    fn single_then_single() {
        let mut flat = ConfirmSmoother::new();
        let empty = flat.process(single(3, Confirm::Ack));
        assert_eq!(empty.count(), 0);
        let empty = flat.process(single(2, Confirm::Ack));
        assert_eq!(empty.count(), 0);
        let two = flat.process(single(1, Confirm::Nack));
        let expected = vec![
            single(1, Confirm::Nack),
            single(2, Confirm::Ack),
            single(3, Confirm::Ack),
        ];
        assert_eq!(expected, two.collect::<Vec<_>>());
    }

    #[test]
    fn redelivery() {
        let mut flat = ConfirmSmoother::new();
        let two = flat.process(multiple(2, Confirm::Ack));
        assert_eq!(two.count(), 2);

        // getting another confirm for 1 or 2 should do nothing since flattener already
        // dispatched a confirm for 1 and 2
        let empty = flat.process(single(2, Confirm::Ack));
        assert_eq!(empty.count(), 0);
        let empty = flat.process(single(1, Confirm::Nack));
        assert_eq!(empty.count(), 0);
    }

    #[test]
    fn single_single_multiple_single() {
        let mut flat = ConfirmSmoother::new();
        let empty = flat.process(single(5, Confirm::Nack));
        assert_eq!(empty.count(), 0);
        let empty = flat.process(single(3, Confirm::Nack));
        assert_eq!(empty.count(), 0);
        let three = flat.process(multiple(2, Confirm::Ack));
        assert_eq!(
            vec![
                single(1, Confirm::Ack),
                single(2, Confirm::Ack),
                single(3, Confirm::Nack)
            ],
            three.collect::<Vec<_>>()
        );
        let two = flat.process(single(4, Confirm::Ack));
        assert_eq!(
            vec![single(4, Confirm::Ack), single(5, Confirm::Nack),],
            two.collect::<Vec<_>>()
        );
    }

    #[test]
    fn drop_without_running_iter_to_completion() {
        let mut flat = ConfirmSmoother::new();
        let _ = flat.process(multiple(2, Confirm::Ack));
        let one = flat.process(single(3, Confirm::Ack));
        let expected = vec![single(3, Confirm::Ack)];
        assert_eq!(expected, one.collect::<Vec<_>>());
    }
}
