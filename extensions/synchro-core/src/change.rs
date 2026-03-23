use serde::{Deserialize, Serialize};
use std::fmt;

/// Internal operation kind shared by changelog, WAL decoding, and legacy
/// extension paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum ChangeOperation {
    Insert = 1,
    Update = 2,
    Delete = 3,
}

impl ChangeOperation {
    /// Returns the legacy wire-protocol string for this operation.
    pub fn legacy_wire_name(self) -> &'static str {
        match self {
            Self::Insert => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    /// Parses a legacy wire-protocol string into an operation.
    pub fn parse_legacy_wire(value: &str) -> Option<Self> {
        match value {
            "create" => Some(Self::Insert),
            "update" => Some(Self::Update),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    /// Converts from the smallint stored in the changelog.
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
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

impl fmt::Display for ChangeOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.legacy_wire_name())
    }
}

#[cfg(test)]
mod tests {
    use super::ChangeOperation;

    #[test]
    fn operation_round_trip() {
        for op in [
            ChangeOperation::Insert,
            ChangeOperation::Update,
            ChangeOperation::Delete,
        ] {
            let wire = op.legacy_wire_name();
            assert_eq!(ChangeOperation::parse_legacy_wire(wire), Some(op));
            let encoded = op.to_i16();
            assert_eq!(ChangeOperation::from_i16(encoded), Some(op));
        }
    }

    #[test]
    fn operation_display() {
        assert_eq!(format!("{}", ChangeOperation::Insert), "create");
        assert_eq!(format!("{}", ChangeOperation::Update), "update");
        assert_eq!(format!("{}", ChangeOperation::Delete), "delete");
    }

    #[test]
    fn operation_parse_unknown() {
        assert_eq!(ChangeOperation::parse_legacy_wire("unknown"), None);
        assert_eq!(ChangeOperation::from_i16(0), None);
        assert_eq!(ChangeOperation::from_i16(4), None);
    }

    #[test]
    fn from_i16_boundary_values() {
        assert_eq!(ChangeOperation::from_i16(-1), None);
        assert_eq!(ChangeOperation::from_i16(i16::MIN), None);
        assert_eq!(ChangeOperation::from_i16(i16::MAX), None);
        assert_eq!(ChangeOperation::from_i16(1), Some(ChangeOperation::Insert));
        assert_eq!(ChangeOperation::from_i16(3), Some(ChangeOperation::Delete));
    }
}
