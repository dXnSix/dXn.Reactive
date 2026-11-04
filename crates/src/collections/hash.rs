//! The hasher of the collection layer's internal tables.
//!
//! Operators mirror their input in hash tables and hash every key several times per change,
//! so the default SipHash dominated their cost. [`FastState`] hashes with a folded 64×64-bit
//! multiply instead. Its state starts from a seed drawn once per process from the standard
//! library's random source, so the keys that collide are not predictable without it. No
//! public type exposes these tables.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::OnceLock;

/// An internal `HashMap` hashed with [`FastState`].
pub(crate) type HashMap<K, V> = std::collections::HashMap<K, V, FastState>;
/// An internal `HashSet` hashed with [`FastState`].
pub(crate) type HashSet<T> = std::collections::HashSet<T, FastState>;

const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
const LENGTH_MULTIPLIER: u64 = 0xD6E8_FEB8_6659_FD93;

fn process_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| RandomState::new().hash_one(0x5eed_u64))
}

/// Builds [`FastHasher`]s seeded with the process seed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FastState {
    seed: u64,
}

impl Default for FastState {
    fn default() -> Self {
        Self {
            seed: process_seed(),
        }
    }
}

impl BuildHasher for FastState {
    type Hasher = FastHasher;

    fn build_hasher(&self) -> FastHasher {
        FastHasher { state: self.seed }
    }
}

/// A folded-multiply hasher: each word is mixed into the state with one 128-bit multiply.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FastHasher {
    state: u64,
}

impl FastHasher {
    #[inline]
    fn mix(&mut self, word: u64) {
        let product = u128::from(self.state ^ word) * u128::from(MULTIPLIER);
        self.state = (product as u64) ^ ((product >> 64) as u64);
    }
}

impl Hasher for FastHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, rest) = bytes.as_chunks::<8>();
        for word in words {
            self.mix(u64::from_le_bytes(*word));
        }
        if !rest.is_empty() {
            let mut word = [0; 8];
            word[..rest.len()].copy_from_slice(rest);
            self.mix(u64::from_le_bytes(word));
        }
        // The length separates inputs that differ only by trailing zero bytes.
        self.mix((bytes.len() as u64).wrapping_mul(LENGTH_MULTIPLIER));
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.mix(u64::from(value));
    }

    #[inline]
    fn write_u16(&mut self, value: u16) {
        self.mix(u64::from(value));
    }

    #[inline]
    fn write_u32(&mut self, value: u32) {
        self.mix(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.mix(value);
    }

    #[inline]
    fn write_u128(&mut self, value: u128) {
        self.mix(value as u64);
        self.mix((value >> 64) as u64);
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.mix(value as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::{FastState, HashMap, HashSet};
    use std::hash::BuildHasher;

    #[test]
    fn fast_hasher_spreads_small_integer_keys_over_buckets() {
        let state = FastState::default();
        let low_bits = (0_u32..4_096)
            .map(|key| state.hash_one(key) & 0xfff)
            .collect::<HashSet<_>>();
        // A good hash fills most of 4096 buckets with 4096 sequential keys.
        assert!(
            low_bits.len() > 2_400,
            "{} distinct buckets",
            low_bits.len()
        );
        let top_bits = (0_u32..4_096)
            .map(|key| state.hash_one(key) >> 57)
            .collect::<HashSet<_>>();
        assert_eq!(top_bits.len(), 128);
    }

    #[test]
    fn fast_hasher_separates_strings_tuples_and_trailing_zero_bytes() {
        let state = FastState::default();
        assert_ne!(state.hash_one("ab"), state.hash_one("ba"));
        assert_ne!(
            state.hash_one(b"a\0".as_slice()),
            state.hash_one(b"a".as_slice())
        );
        assert_ne!(
            state.hash_one((1_u32, 2_u32)),
            state.hash_one((2_u32, 1_u32))
        );
        assert_eq!(
            state.hash_one("same"),
            FastState::default().hash_one("same")
        );
    }

    #[test]
    fn fast_tables_behave_like_std_tables() {
        let mut map = HashMap::default();
        for key in 0..1_000_u64 {
            map.insert(format!("key-{key}"), key);
        }
        assert_eq!(map.len(), 1_000);
        assert!((0..1_000_u64).all(|key| map.get(&format!("key-{key}")) == Some(&key)));
    }
}
