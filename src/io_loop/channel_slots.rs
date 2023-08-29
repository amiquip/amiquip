use crate::errors::*;
use indexmap::IndexSet;
use snafu::OptionExt;
use std::collections::hash_map::{Drain, Entry, HashMap};

pub(crate) struct ChannelSlots<T> {
    slots: HashMap<u16, T>,
    freed_channel_ids: IndexSet<u16>,
    next_channel_id: u16,
    channel_max: u16,
}

impl<T> ChannelSlots<T> {
    pub(crate) fn new() -> ChannelSlots<T> {
        ChannelSlots {
            slots: HashMap::new(),
            freed_channel_ids: IndexSet::new(),
            next_channel_id: 1,
            channel_max: 0,
        }
    }

    pub(crate) fn drain(&mut self) -> Drain<u16, T> {
        for (id, _) in self.slots.iter() {
            self.freed_channel_ids.insert(*id);
        }
        self.slots.drain()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&u16, &T)> {
        self.slots.iter()
    }

    pub(crate) fn set_channel_max(&mut self, channel_max: u16) {
        assert!(
            self.slots.is_empty() && self.freed_channel_ids.is_empty(),
            "channel_max should not be set after channels have been opened"
        );
        self.channel_max = channel_max;
    }

    pub(crate) fn get(&self, channel_id: u16) -> Option<&T> {
        self.slots.get(&channel_id)
    }

    pub(crate) fn get_mut(&mut self, channel_id: u16) -> Option<&mut T> {
        self.slots.get_mut(&channel_id)
    }

    pub(crate) fn insert<F, U>(&mut self, channel_id: Option<u16>, make_entry: F) -> Result<U>
    where
        F: FnOnce(u16) -> Result<(T, U)>,
    {
        let channel_id = match channel_id {
            Some(id) => id,
            None => return self.insert_unused_channel_id(make_entry),
        };
        if channel_id > self.channel_max {
            return UnavailableChannelIdSnafu { channel_id }.fail();
        }
        match self.slots.entry(channel_id) {
            Entry::Occupied(_) => UnavailableChannelIdSnafu { channel_id }.fail(),
            Entry::Vacant(entry) => {
                let (t, u) = make_entry(channel_id)?;
                entry.insert(t);
                Ok(u)
            }
        }
    }

    pub(crate) fn remove(&mut self, channel_id: u16) -> Option<T> {
        let entry = self.slots.remove(&channel_id)?;
        self.freed_channel_ids.insert(channel_id);
        Some(entry)
    }

    fn insert_unused_channel_id<F, U>(&mut self, make_entry: F) -> Result<U>
    where
        F: FnOnce(u16) -> Result<(T, U)>,
    {
        // First try to grab the next available channel ID we're aware of; this
        // could fail if a user requested a channel ID greater than the ones we've
        // handed out from within this function, so keep looking.
        while self.next_channel_id <= self.channel_max {
            let channel_id = self.next_channel_id;
            self.next_channel_id += 1;
            match self.slots.entry(channel_id) {
                Entry::Occupied(_) => continue,
                Entry::Vacant(entry) => {
                    let (t, u) = make_entry(channel_id)?;
                    entry.insert(t);
                    return Ok(u);
                }
            }
        }

        // At the end of our rope for simple channel allocation; fall back to finding
        // one that has been previously freed.
        let channel_id = self
            .freed_channel_ids
            .pop()
            .context(ExhaustedChannelIdsSnafu)?;
        match self.slots.entry(channel_id) {
            Entry::Occupied(_) => unreachable!("free channel id cannot be occupied"),
            Entry::Vacant(entry) => {
                let (t, u) = make_entry(channel_id)?;
                entry.insert(t);
                Ok(u)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id<T>(x: T) -> Result<(T, ())> {
        Ok((x, ()))
    }

    fn with_channel_max(channel_max: u16) -> ChannelSlots<u16> {
        let mut cs = ChannelSlots::new();
        cs.set_channel_max(channel_max);
        cs
    }

    #[test]
    #[should_panic]
    fn set_channel_max_after_insert_panics() {
        let mut cs = with_channel_max(4);
        if cs.insert(Some(1), id).is_err() {
            return;
        }
        cs.set_channel_max(4);
    }

    #[test]
    #[should_panic]
    fn set_channel_max_after_insert_and_remove_panics() {
        let mut cs = with_channel_max(4);
        if cs.insert(Some(1), id).is_err() {
            return;
        }
        if cs.remove(1).is_none() {
            return;
        }
        cs.set_channel_max(4);
    }

    #[test]
    fn insert_channel_above_max_fails() {
        let mut cs = with_channel_max(4);
        let res = cs.insert(Some(5), id);
        match res.unwrap_err() {
            Error::UnavailableChannelId { channel_id } if channel_id == 5 => (),
            err => panic!("unexpected error {}", err),
        }
    }

    #[test]
    fn insert_taken_id_fails() {
        let mut cs = with_channel_max(4);
        cs.insert(Some(1), id).unwrap();
        let res = cs.insert(Some(1), id);
        match res.unwrap_err() {
            Error::UnavailableChannelId { channel_id } if channel_id == 1 => (),
            err => panic!("unexpected error {}", err),
        }
    }

    #[test]
    fn insert_finds_never_used_ids() {
        let mut cs = with_channel_max(4);
        cs.insert(Some(1), id).unwrap();
        cs.insert(Some(2), id).unwrap();
        assert_eq!(cs.next_channel_id, 1); // still doesn't know about the manually-chosen ids..

        // should pick 3 (next available)
        cs.insert(None, id).unwrap();
        assert!(cs.get(3).is_some());
        assert_eq!(cs.next_channel_id, 4);
    }

    #[test]
    fn insert_finds_freed_ids() {
        let mut cs = with_channel_max(4);
        for i in 1..=4 {
            cs.insert(Some(i), id).unwrap();
        }
        assert!(cs.remove(2).is_some());
        assert!(cs.get(2).is_none());
        cs.insert(None, id).unwrap();
        assert!(cs.get(2).is_some());
    }

    #[test]
    fn insert_fails_if_all_available_ids_taken() {
        let mut cs = with_channel_max(4);
        for i in 1..=4 {
            cs.insert(Some(i), id).unwrap();
        }
        match cs.insert(None, id).unwrap_err() {
            Error::ExhaustedChannelIds => (),
            err => panic!("unexpected error {}", err),
        }
    }
}
