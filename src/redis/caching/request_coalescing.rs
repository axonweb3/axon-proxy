use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
};

use tokio::sync::broadcast;

pub struct RequestCoalescing<K, T> {
    // Possible optimization: use concurrent map / sharding.
    receivers: std::sync::Mutex<HashMap<K, broadcast::Receiver<T>>>,
}

impl<K, T> Default for RequestCoalescing<K, T> {
    fn default() -> Self {
        Self {
            receivers: HashMap::new().into(),
        }
    }
}

impl<K, T> RequestCoalescing<K, T>
where
    K: Clone + Eq + Hash,
    T: Clone,
{
    /// Make sure to call remove if you get a sender!
    pub fn get(&self, key: K) -> CoalescingResult<T> {
        let mut locks = self.receivers.lock().unwrap();
        match locks.entry(key) {
            Entry::Occupied(o) => CoalescingResult::Receiver(o.get().resubscribe()),
            Entry::Vacant(v) => {
                let (tx, rx) = broadcast::channel(1);
                v.insert(rx);
                CoalescingResult::Sender(tx)
            }
        }
    }

    pub fn remove(&self, key: &K) {
        self.receivers.lock().unwrap().remove(key);
    }

    pub fn len(&self) -> usize {
        self.receivers.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum CoalescingResult<T> {
    Receiver(broadcast::Receiver<T>),
    Sender(broadcast::Sender<T>),
}
