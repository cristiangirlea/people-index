//! `Seq`: where a record sits in feed order.
//!
//! Duplicate ids resolve to the last record in feed order (input files sorted by key, then line
//! number), whatever the partition or worker count. Every row carries the `Seq` of the record
//! it came from, so any task can apply that rule locally by comparing two `Seq`s.

use serde::{Deserialize, Serialize};

/// Position in feed order: the input file's index in the plan's sorted file list, then the
/// 1-based line number inside it. The derived ordering compares `file` first, then `line`,
/// which is exactly feed order.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Seq {
    /// Index of the input file in the plan's sorted list.
    pub file: u32,
    /// Line number inside that file.
    pub line: u64,
}

impl Seq {
    /// A position in feed order.
    #[must_use]
    pub const fn new(file: u32, line: u64) -> Self {
        Self { file, line }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_file_then_line() {
        assert!(Seq::new(0, 100) < Seq::new(1, 1));
        assert!(Seq::new(1, 1) < Seq::new(1, 2));
        assert_eq!(Seq::new(2, 3).max(Seq::new(2, 1)), Seq::new(2, 3));
    }
}
