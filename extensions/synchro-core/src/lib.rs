//! Synchro shared Rust core.
//!
//! For vNext contract work, the authoritative shared surfaces are:
//!
//! - `change`
//! - `contract`
//! - `limits`
//! - `state`
//!
//! `conflict` still contains timestamp-specific helpers used by the current
//! PostgreSQL extension. It is not the authoritative vNext conflict contract.

pub mod change;
pub mod checksum;
pub mod conflict;
pub mod contract;
pub mod dedup;
pub mod edge_diff;
pub mod limits;
pub mod state;
pub mod version;
