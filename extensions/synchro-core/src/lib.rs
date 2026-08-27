//! Synchro shared Rust core.
//!
//! The authoritative shared surfaces are:
//!
//! - `change`
//! - `contract`
//! - `limits`
//!
pub mod change;
pub mod checksum;
pub mod contract;
pub mod edge_diff;
pub mod fingerprint;
pub mod limits;
pub mod version;
