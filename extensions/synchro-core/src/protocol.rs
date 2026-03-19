use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Operation
// ---------------------------------------------------------------------------

/// Represents the type of change in a sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum Operation {
    Insert = 1,
    Update = 2,
    Delete = 3,
}

impl Operation {
    /// Returns the wire-protocol string for this operation.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    /// Parses a wire-protocol string into an Operation.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "create" => Some(Self::Insert),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    /// Converts from the smallint stored in the changelog.
    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            1 => Some(Self::Insert),
            2 => Some(Self::Update),
            3 => Some(Self::Delete),
            _ => None,
        }
    }

    /// Returns the smallint representation.
    pub fn to_i16(self) -> i16 {
        self as i16
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Push status constants
// ---------------------------------------------------------------------------

pub const PUSH_STATUS_APPLIED: &str = "applied";
pub const PUSH_STATUS_CONFLICT: &str = "conflict";
pub const PUSH_STATUS_REJECTED_TERMINAL: &str = "rejected_terminal";
pub const PUSH_STATUS_REJECTED_RETRYABLE: &str = "rejected_retryable";

// ---------------------------------------------------------------------------
// Pull / rebuild limits
// ---------------------------------------------------------------------------

pub const DEFAULT_PULL_LIMIT: i32 = 100;
pub const MAX_PULL_LIMIT: i32 = 1000;
pub const DEFAULT_REBUILD_LIMIT: i32 = 100;
pub const MAX_REBUILD_LIMIT: i32 = 1000;

// ---------------------------------------------------------------------------
// Snapshot reason constants (legacy, kept for backwards compatibility)
// ---------------------------------------------------------------------------

pub const SNAPSHOT_REASON_INITIAL_SYNC: &str = "initial_sync_required";
pub const SNAPSHOT_REASON_CHECKPOINT_BEFORE_LIMIT: &str = "checkpoint_before_retention";
pub const SNAPSHOT_REASON_HISTORY_UNAVAILABLE: &str = "history_unavailable";

// ---------------------------------------------------------------------------
// Default kind constants (schema introspection)
// ---------------------------------------------------------------------------

pub const DEFAULT_KIND_NONE: &str = "none";
pub const DEFAULT_KIND_PORTABLE: &str = "portable";
pub const DEFAULT_KIND_SERVER_ONLY: &str = "server_only";

// ---------------------------------------------------------------------------
// Clamp helpers
// ---------------------------------------------------------------------------

/// Clamp a pull limit to [1, MAX_PULL_LIMIT], defaulting to DEFAULT_PULL_LIMIT
/// when the input is <= 0.
pub fn clamp_pull_limit(limit: i32) -> i32 {
    if limit <= 0 {
        DEFAULT_PULL_LIMIT
    } else if limit > MAX_PULL_LIMIT {
        MAX_PULL_LIMIT
    } else {
        limit
    }
}

/// Clamp a rebuild limit to [1, MAX_REBUILD_LIMIT], defaulting to
/// DEFAULT_REBUILD_LIMIT when the input is <= 0.
pub fn clamp_rebuild_limit(limit: i32) -> i32 {
    if limit <= 0 {
        DEFAULT_REBUILD_LIMIT
    } else if limit > MAX_REBUILD_LIMIT {
        MAX_REBUILD_LIMIT
    } else {
        limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_round_trip() {
        for op in [Operation::Insert, Operation::Update, Operation::Delete] {
            let s = op.as_str();
            assert_eq!(Operation::parse(s), Some(op));
            let i = op.to_i16();
            assert_eq!(Operation::from_i16(i), Some(op));
        }
    }

    #[test]
    fn operation_display() {
        assert_eq!(format!("{}", Operation::Insert), "create");
        assert_eq!(format!("{}", Operation::Update), "update");
        assert_eq!(format!("{}", Operation::Delete), "delete");
    }

    #[test]
    fn operation_parse_unknown() {
        assert_eq!(Operation::parse("unknown"), None);
        assert_eq!(Operation::from_i16(0), None);
        assert_eq!(Operation::from_i16(4), None);
    }

    #[test]
    fn clamp_pull() {
        assert_eq!(clamp_pull_limit(0), DEFAULT_PULL_LIMIT);
        assert_eq!(clamp_pull_limit(-1), DEFAULT_PULL_LIMIT);
        assert_eq!(clamp_pull_limit(50), 50);
        assert_eq!(clamp_pull_limit(2000), MAX_PULL_LIMIT);
    }

    #[test]
    fn clamp_rebuild() {
        assert_eq!(clamp_rebuild_limit(0), DEFAULT_REBUILD_LIMIT);
        assert_eq!(clamp_rebuild_limit(-1), DEFAULT_REBUILD_LIMIT);
        assert_eq!(clamp_rebuild_limit(500), 500);
        assert_eq!(clamp_rebuild_limit(5000), MAX_REBUILD_LIMIT);
    }

    #[test]
    fn from_i16_boundary_values() {
        // Negative values, i16::MIN, i16::MAX must all return None.
        assert_eq!(Operation::from_i16(-1), None);
        assert_eq!(Operation::from_i16(i16::MIN), None);
        assert_eq!(Operation::from_i16(i16::MAX), None);
        // Valid range boundaries.
        assert_eq!(Operation::from_i16(1), Some(Operation::Insert));
        assert_eq!(Operation::from_i16(3), Some(Operation::Delete));
    }

    #[test]
    fn clamp_boundary_exact() {
        // Exactly at boundaries: 1 and MAX should pass through unchanged.
        assert_eq!(clamp_pull_limit(1), 1);
        assert_eq!(clamp_pull_limit(MAX_PULL_LIMIT), MAX_PULL_LIMIT);
        assert_eq!(clamp_rebuild_limit(1), 1);
        assert_eq!(clamp_rebuild_limit(MAX_REBUILD_LIMIT), MAX_REBUILD_LIMIT);
    }
}
