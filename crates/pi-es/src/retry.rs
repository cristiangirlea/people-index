//! Jittered exponential backoff. Every sender backing off on the same schedule would hit a
//! recovering cluster in lockstep; the jitter spreads them out.

use std::time::Duration;

/// How often and how patiently a failing request is sent again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts in total, the first one included. At least 1.
    pub max_attempts: u32,
    /// The delay before the first retry, before jitter.
    pub base_delay: Duration,
    /// The cap on any one delay.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    /// Eight attempts from 200 ms doubling to a 30 s cap: about a minute of patience in all,
    /// enough to ride out a rolling restart of a data node.
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// No retries at all, for tests and for calls that must not repeat.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    /// Whether another attempt is allowed after `attempt` (1-based) failed.
    #[must_use]
    pub fn allows_retry_after(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }

    /// The delay before retry number `retry` (0-based), drawn from `[d/2, d]` where `d` is the
    /// capped exponential delay ("equal jitter": never zero, never above the cap).
    #[must_use]
    pub fn delay(&self, retry: u32) -> Duration {
        self.delay_with(retry, rand::random::<f64>())
    }

    /// [`Self::delay`] with the random draw (in `[0, 1)`) passed in, so tests are exact.
    #[must_use]
    pub fn delay_with(&self, retry: u32, unit: f64) -> Duration {
        let factor = 2u32.saturating_pow(retry.min(31));
        let full = self
            .base_delay
            .saturating_mul(factor)
            .min(self.max_delay.max(self.base_delay));
        let half = full / 2;
        half + half.mul_f64(unit.clamp(0.0, 1.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_grow_and_stay_capped() {
        let policy = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        assert_eq!(policy.delay_with(0, 0.0), Duration::from_millis(50));
        assert_eq!(policy.delay_with(0, 1.0), Duration::from_millis(100));
        assert_eq!(policy.delay_with(3, 1.0), Duration::from_millis(800));
        assert_eq!(policy.delay_with(10, 1.0), Duration::from_secs(1));
        assert_eq!(policy.delay_with(40, 0.5), Duration::from_millis(750));
        let drawn = policy.delay(2);
        assert!(drawn >= Duration::from_millis(200) && drawn <= Duration::from_millis(400));
    }

    #[test]
    fn attempts_are_bounded() {
        let policy = RetryPolicy {
            max_attempts: 3,
            ..RetryPolicy::default()
        };
        assert!(policy.allows_retry_after(1));
        assert!(policy.allows_retry_after(2));
        assert!(!policy.allows_retry_after(3));
        assert!(!RetryPolicy::none().allows_retry_after(1));
    }
}
