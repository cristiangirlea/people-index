//! [`RetryPolicy`]: how often and how patiently a failed task is tried again.

use std::time::Duration;

/// Attempts and backoff for a failing task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, the first one included. At least 1.
    pub max_attempts: u32,
    /// The delay before the second attempt; doubled for each one after.
    pub base_delay: Duration,
    /// The longest delay between attempts.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// One attempt, no retries.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    /// The delay after failed attempt number `attempt` (1-based): exponential, capped, with
    /// jitter in `[d/2, d]` so workers that failed together do not retry together.
    #[must_use]
    pub fn delay_after(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1).min(20);
        let full = self
            .base_delay
            .saturating_mul(1u32 << exp)
            .min(self.max_delay);
        let jitter: f64 = rand::random_range(0.5..=1.0);
        full.mul_f64(jitter)
    }

    /// True when another attempt is allowed after `attempt` failed.
    #[must_use]
    pub fn allows_retry_after(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_caps_and_jitters() {
        let p = RetryPolicy {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(350),
        };
        for _ in 0..50 {
            let d1 = p.delay_after(1);
            assert!(d1 >= Duration::from_millis(50) && d1 <= Duration::from_millis(100));
            let d3 = p.delay_after(3);
            assert!(d3 >= Duration::from_millis(175) && d3 <= Duration::from_millis(350));
            assert!(p.delay_after(40) <= Duration::from_millis(350));
        }
        assert!(p.allows_retry_after(4));
        assert!(!p.allows_retry_after(5));
        assert!(!RetryPolicy::none().allows_retry_after(1));
    }
}
