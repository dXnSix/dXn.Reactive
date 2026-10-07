//! IDX-3.1–3.4: subset and inline cache operators index their own output (or emit `None`),
//! never remove keys they did not include, keep positional updates, and the cache-to-list
//! bridge survives a non-conforming upstream.

mod dynamic_inputs;
mod fuzzed;
mod load_scaling;
mod repros;
