//! Synchro shared Rust core.
//!
//! The authoritative shared surfaces are:
//!
//! - `change`
//! - `contract`
//! - `limits`
//! - `state`
//!
pub mod change;
pub mod checksum;
pub mod contract;
pub mod dedup;
pub mod edge_diff;
pub mod fingerprint;
pub mod limits;
pub mod state;
pub mod version;
