use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct NotificationListeners<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T: Clone> NotificationListeners<T> {
    pub(crate) fn new() -> Self {
        NotificationListeners {
            inner: Arc::new(Mutex::new(Inner::new()))
        }
    }

    pub(crate) fn register_listener(&self) -> NotificationListener<T> {
        let (id, rx) = self.inner.lock().unwrap().add_listener();
        let inner = Arc::clone(&self.inner);
        NotificationListener { inner, id, rx }
    }

    pub(crate) fn broadcast(&self, note: T) {
        let inner = self.inner.lock().unwrap();
        for (_, tx) in inner.listeners.iter() {
            // unwrap is safe here because the NotificationListener removes itself
            // from listeners on drop
            tx.send(note.clone()).unwrap();
        }
    }
}

struct Inner<T> {
    next_id: u64,
    listeners: HashMap<u64, Sender<T>>,
}

impl<T> Inner<T> {
    fn new() -> Self {
        Inner {
            next_id: 0,
            listeners: HashMap::new(),
        }
    }

    fn add_listener(&mut self) -> (u64, Receiver<T>) {
        let id = self.next_id;
        self.next_id += 1;

        let (tx, rx) = crossbeam_channel::unbounded();
        self.listeners.insert(id, tx);

        (id, rx)
    }

    #[inline]
    fn remove_listener(&mut self, id: u64) {
        self.listeners.remove(&id);
    }
}

pub struct NotificationListener<T> {
    inner: Arc<Mutex<Inner<T>>>,
    id: u64,
    rx: Receiver<T>,
}

impl<T> NotificationListener<T> {
    pub fn receiver(&self) -> &Receiver<T> {
        &self.rx
    }
}

impl<T> Drop for NotificationListener<T> {
    fn drop(&mut self) {
        self.inner.lock().unwrap().remove_listener(self.id);
    }
}
