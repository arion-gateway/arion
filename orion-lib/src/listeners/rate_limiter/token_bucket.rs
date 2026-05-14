// Copyright 2025 The kmesh Authors
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use std::{fmt, hash::Hash, sync::atomic::Ordering};

use atomic_time::AtomicInstant;
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

/// This `TokenBucket` implementation takes inspiration from `<https://github.com/rigtorp/TokenBucket/tree/master>`
pub struct TokenBucket {
    time: AtomicInstant,
    time_per_token: Duration,
    time_per_bucket: Duration,
    max_tokens: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TokenBucketError {
    ZeroRate,
    ZeroCapacity,
}

impl fmt::Display for TokenBucketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenBucketError::ZeroRate => write!(f, "events_per_second must be strictly greater than 0"),
            TokenBucketError::ZeroCapacity => write!(f, "max_tokens must be strictly greater than 0"),
        }
    }
}

impl TokenBucket {
    /// Construct a `TokenBucket` only using `max_token` and events per second information.
    ///
    /// * `max_tokens`: The maximum number of tokens that the bucket can hold.
    /// * `events_per_second`: Expected number of events per second.
    #[allow(dead_code)]
    pub fn with_rate_and_capacity(max_tokens: u32, events_per_second: u32) -> Result<TokenBucket, TokenBucketError> {
        if max_tokens == 0 {
            return Err(TokenBucketError::ZeroCapacity);
        }
        if events_per_second == 0 {
            return Err(TokenBucketError::ZeroRate);
        }

        let tokens_per_fill = 1;
        let fill_interval = Duration::from_secs(1) / events_per_second;

        Ok(Self::new(max_tokens, tokens_per_fill, fill_interval))
    }

    /// Construct a `TokenBucket`
    //
    /// * `max_token`: The maximum tokens that the bucket can hold.
    /// * `tokens_per_fill`: The number of tokens added to the bucket during each fill interval.
    /// * `fill_interval`: The fill interval that tokens are added to the bucket. During each fill interval `tokens_per_fill` are added to the bucket.
    pub fn new(max_tokens: u32, tokens_per_fill: u32, fill_interval: Duration) -> TokenBucket {
        let time_per_bucket = max_tokens * fill_interval / tokens_per_fill;
        let now = Instant::now();
        Self {
            time: AtomicInstant::new(now.checked_sub(2 * time_per_bucket).unwrap_or(now)),
            time_per_token: fill_interval / tokens_per_fill,
            max_tokens: max_tokens as usize,
            time_per_bucket,
        }
    }

    /// Try consuming a number of tokens and returns a boolean
    /// that represents the outcome.
    /// * `tokens`: The number of tokens to consume.
    pub fn consume(&self, tokens: u32) -> bool {
        let req_fill_period = self.time_per_token * tokens;
        let now = Instant::now();
        let min_time = now.checked_sub(self.time_per_bucket).unwrap_or(now);

        let mut old_time = self.time.load(Ordering::Relaxed);

        loop {
            let mut new_time = old_time;
            if min_time > new_time {
                new_time = min_time;
            }
            new_time += req_fill_period;
            if new_time > now {
                return false;
            }

            match self.time.compare_exchange_weak(old_time, new_time, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(x) => old_time = x,
            }
        }
    }

    /// Return the capacity of the bucket, in term of tokens.
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.max_tokens
    }

    /// Return the actual bucket size.
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        let now = Instant::now();
        let t = self.time.load(Ordering::Relaxed);
        if now < t {
            return 0;
        }

        let n = ((now - t).as_nanos() / self.time_per_token.as_nanos()) as usize;

        if n > self.max_tokens {
            self.max_tokens
        } else {
            n
        }
    }

    /// Return the last consume timestamp
    #[allow(dead_code)]
    pub fn last_consume(&self) -> Instant {
        self.time.load(Ordering::Relaxed)
    }
}

impl Debug for TokenBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenBucket")
            .field("time", &self.time.load(Ordering::Relaxed))
            .field("time_per_token", &self.time_per_token)
            .field("time_per_bucket", &self.time_per_bucket)
            .field("max_tokens", &self.max_tokens)
            .finish()
    }
}

impl PartialEq for TokenBucket {
    fn eq(&self, other: &Self) -> bool {
        self.time_per_token == other.time_per_token && self.time_per_bucket == other.time_per_bucket
    }
}

impl Clone for TokenBucket {
    fn clone(&self) -> Self {
        Self {
            time: AtomicInstant::new(self.time.load(Ordering::Relaxed)),
            time_per_token: self.time_per_token,
            time_per_bucket: self.time_per_bucket,
            max_tokens: self.max_tokens,
        }
    }
}

impl Eq for TokenBucket {}

impl Hash for TokenBucket {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.time.load(Ordering::Relaxed).hash(state);
        self.time_per_token.hash(state);
        self.time_per_bucket.hash(state);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::{
        sync::{Arc, Barrier},
        thread::{self, sleep},
    };

    #[test]
    fn token_bucket_burst() {
        let tb = TokenBucket::new(5, 1, Duration::from_millis(10));

        assert_eq!(tb.capacity(), 5);
        assert_eq!(tb.size(), 5);

        // consume the burst

        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(!tb.consume(1));

        assert_eq!(tb.capacity(), 5);
        assert_eq!(tb.size(), 0);

        // wait for the full refill

        sleep(Duration::from_millis(50));

        assert_eq!(tb.capacity(), 5);
        assert_eq!(tb.size(), 5);

        // consume the burst

        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(!tb.consume(1));
    }

    #[test]
    fn token_bucket_basic() {
        let tb = TokenBucket::new(5, 1, Duration::from_millis(10));

        // consume the burst

        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(tb.consume(1));
        assert!(!tb.consume(1));

        // wait for the full refill

        sleep(Duration::from_millis(10));

        // consume a token and then bucket is empty

        assert!(tb.consume(1));

        sleep(Duration::from_millis(10));

        // consume a token

        assert!(tb.consume(1));

        sleep(Duration::from_millis(10));

        assert!(tb.consume(1));

        sleep(Duration::from_millis(10));
    }

    #[test]
    fn token_bucket_multi_thread() {
        eprintln!("starting multithreaded test...");
        sleep(Duration::from_secs(5));

        let n = 10;
        let mut handles = Vec::with_capacity(n);
        let barrier = Arc::new(Barrier::new(n));
        let tb = Arc::new(TokenBucket::new(50, 10, Duration::from_millis(10)));

        for i in 0..n {
            let tb = Arc::clone(&tb);
            let bar = Arc::clone(&barrier);
            println!("running thread {i}...");
            handles.push(thread::spawn(move || {
                // consume all the token first...

                while tb.consume(1) {}

                bar.wait();

                sleep(Duration::from_millis(51));

                assert!(tb.consume(1));
                assert!(tb.consume(1));
                assert!(tb.consume(1));
                assert!(tb.consume(1));
                assert!(tb.consume(1));
            }));
        }

        eprintln!("waiting handles...");
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
