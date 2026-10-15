//! Last-wins deduplication keyed by [`Seq`].
//!
//! Rows reach a task in no particular order (segments from many extract tasks, listed in any
//! order), so "last" cannot mean "last seen": it means the greatest `Seq`. Offering every row
//! to a [`LastWins`] gives the same winner whatever the order, which is what makes the output
//! independent of partition and worker counts.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;

use crate::seq::Seq;

/// Keeps, for each key, the value offered with the greatest [`Seq`].
#[derive(Debug, Clone)]
pub struct LastWins<K, V> {
    map: HashMap<K, (Seq, V)>,
}

impl<K: Eq + Hash, V> Default for LastWins<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V> LastWins<K, V> {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// An empty set with room for `capacity` keys.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
        }
    }

    /// Offers a value; it is kept when its `seq` is greater than or equal to the current one's
    /// (equal `Seq`s are the same record offered twice, so either copy is right). Returns
    /// whether it was kept.
    pub fn offer(&mut self, key: K, seq: Seq, value: V) -> bool {
        match self.map.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert((seq, value));
                true
            }
            Entry::Occupied(mut slot) => {
                if seq >= slot.get().0 {
                    slot.insert((seq, value));
                    true
                } else {
                    false
                }
            }
        }
    }

    /// The winning value and its `Seq` for `key`.
    pub fn get(&self, key: &K) -> Option<(Seq, &V)> {
        self.map.get(key).map(|(seq, value)| (*seq, value))
    }

    /// The winning `Seq` for `key`.
    pub fn seq(&self, key: &K) -> Option<Seq> {
        self.map.get(key).map(|(seq, _)| *seq)
    }

    /// Whether `seq` is the winning position for `key`: the test a second streaming pass uses
    /// to keep only winners without holding their values.
    pub fn is_winner(&self, key: &K, seq: Seq) -> bool {
        self.seq(key) == Some(seq)
    }

    /// The number of distinct keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether nothing was offered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// The winners, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, Seq, &V)> {
        self.map.iter().map(|(k, (seq, v))| (k, *seq, v))
    }

    /// The winners, consumed, in no particular order.
    pub fn into_winners(self) -> impl Iterator<Item = (K, Seq, V)> {
        self.map.into_iter().map(|(k, (seq, v))| (k, seq, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greatest_seq_wins_in_any_order() {
        let offers = [
            (Seq::new(0, 5), "a"),
            (Seq::new(1, 1), "c"),
            (Seq::new(0, 9), "b"),
        ];
        for rotation in 0..offers.len() {
            let mut set = LastWins::new();
            for (seq, v) in offers.iter().cycle().skip(rotation).take(offers.len()) {
                set.offer(1u64, *seq, *v);
            }
            assert_eq!(set.get(&1), Some((Seq::new(1, 1), &"c")));
            assert!(set.is_winner(&1, Seq::new(1, 1)));
            assert!(!set.is_winner(&1, Seq::new(0, 9)));
        }
    }

    #[test]
    fn offer_reports_whether_kept() {
        let mut set = LastWins::with_capacity(4);
        assert!(set.offer(1u64, Seq::new(0, 2), ()));
        assert!(!set.offer(1, Seq::new(0, 1), ()));
        assert!(set.offer(1, Seq::new(0, 2), ()), "equal seq replaces");
        assert!(set.offer(2, Seq::new(0, 1), ()));
        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());
        assert_eq!(set.iter().count(), 2);
        assert_eq!(set.into_winners().count(), 2);
    }
}
