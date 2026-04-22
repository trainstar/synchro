//! Timestamp-based conflict helpers.
//!
//! These helpers reflect the current PostgreSQL extension behavior. They are
//! not the authoritative wire contract for conflict representation.
//!
//! `server_version` remains opaque on the wire even if a specific server
//! implementation uses timestamp-based policy internally.

use chrono::{DateTime, Utc};

/// Full context for timestamp-based conflict resolution.
#[derive(Debug, Clone)]
pub struct TimestampConflictContext {
    pub table: String,
    pub record_id: String,
    pub client_id: String,
    pub user_id: String,
    pub client_time: DateTime<Utc>,
    pub server_time: DateTime<Utc>,
    pub base_version: Option<DateTime<Utc>>,
}

/// Winner chosen by a timestamp-based conflict resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampResolutionWinner {
    Client,
    Server,
}

impl TimestampResolutionWinner {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
        }
    }
}

/// Describes how a timestamp-based conflict was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampResolution {
    /// `client` or `server`.
    pub winner: TimestampResolutionWinner,
    /// Human-readable explanation.
    pub reason: &'static str,
}

// ---------------------------------------------------------------------------
// LWW (Last-Write-Wins) resolver
// ---------------------------------------------------------------------------

/// Last-Write-Wins conflict resolver.
///
/// The client timestamp is adjusted forward by `clock_skew_tolerance` before
/// comparison. This gives the client a slight advantage to compensate for
/// network latency and clock drift.
pub struct TimestampLwwResolver {
    pub clock_skew_tolerance: chrono::Duration,
}

impl TimestampLwwResolver {
    pub fn new(clock_skew_tolerance: chrono::Duration) -> Self {
        Self {
            clock_skew_tolerance,
        }
    }

    /// Resolve a conflict using LWW semantics.
    ///
    /// Algorithm (matches Go `LWWResolver.Resolve`):
    ///
    /// 1. Adjust client time by adding clock_skew_tolerance.
    /// 2. If base_version exists:
    ///    a. base > server: server wins (client version ahead of server).
    ///    b. base != server AND base <= server (server changed since base): use LWW on adjusted client time vs server time.
    ///    c. base == server (server unchanged): client wins.
    /// 3. No base_version: pure LWW comparison.
    pub fn resolve(&self, conflict: &TimestampConflictContext) -> TimestampResolution {
        let client_time = conflict.client_time + self.clock_skew_tolerance;

        if let Some(base) = conflict.base_version {
            if base > conflict.server_time {
                return TimestampResolution {
                    winner: TimestampResolutionWinner::Server,
                    reason: "client base version is ahead of server version",
                };
            }

            if base != conflict.server_time {
                // Server modified since base: use LWW.
                if client_time > conflict.server_time {
                    return TimestampResolution {
                        winner: TimestampResolutionWinner::Client,
                        reason: "client timestamp newer (LWW with base)",
                    };
                }
                return TimestampResolution {
                    winner: TimestampResolutionWinner::Server,
                    reason: "server version is newer",
                };
            }

            // base == server_time: server unchanged since client's base.
            return TimestampResolution {
                winner: TimestampResolutionWinner::Client,
                reason: "server unchanged since base version",
            };
        }

        // No base version: pure LWW.
        if client_time > conflict.server_time {
            TimestampResolution {
                winner: TimestampResolutionWinner::Client,
                reason: "client timestamp newer (LWW)",
            }
        } else {
            TimestampResolution {
                winner: TimestampResolutionWinner::Server,
                reason: "server version is newer",
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ServerWins resolver
// ---------------------------------------------------------------------------

/// Always resolves in favor of the server.
pub struct TimestampServerWinsResolver;

impl TimestampServerWinsResolver {
    pub fn resolve(&self, _conflict: &TimestampConflictContext) -> TimestampResolution {
        TimestampResolution {
            winner: TimestampResolutionWinner::Server,
            reason: "server always wins",
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    fn base_conflict() -> TimestampConflictContext {
        TimestampConflictContext {
            table: "items".into(),
            record_id: "r1".into(),
            client_id: "c1".into(),
            user_id: "u1".into(),
            client_time: ts(1000),
            server_time: ts(1000),
            base_version: None,
        }
    }

    fn resolver(tolerance_ms: i64) -> TimestampLwwResolver {
        TimestampLwwResolver::new(chrono::Duration::milliseconds(tolerance_ms))
    }

    // Pure LWW (no base version)

    #[test]
    fn lww_client_newer() {
        let mut c = base_conflict();
        c.client_time = ts(1001);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Client);
        assert_eq!(r.reason, "client timestamp newer (LWW)");
    }

    #[test]
    fn lww_server_newer() {
        let mut c = base_conflict();
        c.client_time = ts(999);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);
        assert_eq!(r.reason, "server version is newer");
    }

    #[test]
    fn lww_equal_server_wins() {
        let c = base_conflict();
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);
    }

    #[test]
    fn lww_clock_skew_tips_client() {
        let mut c = base_conflict();
        c.client_time = ts(999);
        c.server_time = ts(1000);
        // 500ms tolerance not enough (1s behind).
        assert_eq!(
            resolver(500).resolve(&c).winner,
            TimestampResolutionWinner::Server
        );
        // 1001ms tolerance tips it to client.
        assert_eq!(
            resolver(1001).resolve(&c).winner,
            TimestampResolutionWinner::Client
        );
    }

    // With base version

    #[test]
    fn base_ahead_of_server() {
        let mut c = base_conflict();
        c.base_version = Some(ts(2000));
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);
        assert_eq!(r.reason, "client base version is ahead of server version");
    }

    #[test]
    fn base_equals_server_client_wins() {
        let mut c = base_conflict();
        c.base_version = Some(ts(1000));
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Client);
        assert_eq!(r.reason, "server unchanged since base version");
    }

    #[test]
    fn base_behind_server_lww_client_newer() {
        let mut c = base_conflict();
        c.base_version = Some(ts(900));
        c.client_time = ts(1100);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Client);
        assert_eq!(r.reason, "client timestamp newer (LWW with base)");
    }

    #[test]
    fn base_behind_server_lww_server_newer() {
        let mut c = base_conflict();
        c.base_version = Some(ts(900));
        c.client_time = ts(999);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);
        assert_eq!(r.reason, "server version is newer");
    }

    // Boundary conditions: these catch off-by-one and precision issues.

    #[test]
    fn lww_exactly_at_tolerance_boundary() {
        // Client is exactly 500ms behind server. With 500ms tolerance,
        // adjusted client == server. Equal means server wins (not strictly greater).
        let mut c = base_conflict();
        c.client_time = Utc.timestamp_millis_opt(1_000_000).unwrap();
        c.server_time = Utc.timestamp_millis_opt(1_000_500).unwrap();
        let r = resolver(500).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);

        // 501ms tolerance tips it to client.
        let r2 = resolver(501).resolve(&c);
        assert_eq!(r2.winner, TimestampResolutionWinner::Client);
    }

    #[test]
    fn base_equals_server_tolerance_irrelevant() {
        // When base == server (server unchanged), client wins regardless of
        // tolerance. Tolerance only applies to the LWW timestamp comparison,
        // which is skipped when base == server.
        let mut c = base_conflict();
        c.base_version = Some(ts(1000));
        c.server_time = ts(1000);
        c.client_time = ts(500); // Client timestamp way behind, doesn't matter.
        let r = resolver(500).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Client);
        assert_eq!(r.reason, "server unchanged since base version");
    }

    #[test]
    fn lww_zero_tolerance() {
        // Degenerate config: zero tolerance means pure timestamp comparison
        // with no client advantage. Client must be strictly newer to win.
        let mut c = base_conflict();
        c.client_time = ts(1000);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);

        c.client_time = ts(1001);
        let r2 = resolver(0).resolve(&c);
        assert_eq!(r2.winner, TimestampResolutionWinner::Client);
    }

    #[test]
    fn lww_subsecond_precision() {
        // Timestamps differ by 1 millisecond. Must not be truncated to seconds.
        let mut c = base_conflict();
        c.client_time = Utc.timestamp_millis_opt(1_000_001).unwrap();
        c.server_time = Utc.timestamp_millis_opt(1_000_000).unwrap();
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Client);
    }

    // ServerWins

    #[test]
    fn server_wins_always() {
        let c = base_conflict();
        let r = TimestampServerWinsResolver.resolve(&c);
        assert_eq!(r.winner, TimestampResolutionWinner::Server);
        assert_eq!(r.reason, "server always wins");
    }

    #[test]
    fn winner_string_values_are_stable() {
        assert_eq!(TimestampResolutionWinner::Client.as_str(), "client");
        assert_eq!(TimestampResolutionWinner::Server.as_str(), "server");
    }
}
