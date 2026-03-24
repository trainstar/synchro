pub const DEFAULT_PULL_LIMIT: i32 = 100;
pub const MAX_PULL_LIMIT: i32 = 1000;
pub const DEFAULT_REBUILD_LIMIT: i32 = 100;
pub const MAX_REBUILD_LIMIT: i32 = 1000;

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
    use super::{
        clamp_pull_limit, clamp_rebuild_limit, DEFAULT_PULL_LIMIT, DEFAULT_REBUILD_LIMIT,
        MAX_PULL_LIMIT, MAX_REBUILD_LIMIT,
    };

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
    fn clamp_boundary_exact() {
        assert_eq!(clamp_pull_limit(1), 1);
        assert_eq!(clamp_pull_limit(MAX_PULL_LIMIT), MAX_PULL_LIMIT);
        assert_eq!(clamp_rebuild_limit(1), 1);
        assert_eq!(clamp_rebuild_limit(MAX_REBUILD_LIMIT), MAX_REBUILD_LIMIT);
    }
}
