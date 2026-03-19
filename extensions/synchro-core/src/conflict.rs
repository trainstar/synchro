use chrono::{DateTime, Utc};

/// Full context for conflict resolution.
#[derive(Debug, Clone)]
pub struct Conflict {
    pub table: String,
    pub record_id: String,
    pub client_id: String,
    pub user_id: String,
    pub client_time: DateTime<Utc>,
    pub server_time: DateTime<Utc>,
    pub base_version: Option<DateTime<Utc>>,
}

/// Describes how a conflict was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// "client" or "server".
    pub winner: &'static str,
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
pub struct LwwResolver {
    pub clock_skew_tolerance: chrono::Duration,
}

impl LwwResolver {
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
    ///    b. base != server AND base <= server (server changed since base):
    ///       use LWW on adjusted client time vs server time.
    ///    c. base == server (server unchanged): client wins.
    /// 3. No base_version: pure LWW comparison.
    pub fn resolve(&self, conflict: &Conflict) -> Resolution {
        let client_time = conflict.client_time + self.clock_skew_tolerance;

        if let Some(base) = conflict.base_version {
            if base > conflict.server_time {
                return Resolution {
                    winner: "server",
                    reason: "client base version is ahead of server version",
                };
            }

            if base != conflict.server_time {
                // Server modified since base: use LWW.
                if client_time > conflict.server_time {
                    return Resolution {
                        winner: "client",
                        reason: "client timestamp newer (LWW with base)",
                    };
                }
                return Resolution {
                    winner: "server",
                    reason: "server version is newer",
                };
            }

            // base == server_time: server unchanged since client's base.
            return Resolution {
                winner: "client",
                reason: "server unchanged since base version",
            };
        }

        // No base version: pure LWW.
        if client_time > conflict.server_time {
            Resolution {
                winner: "client",
                reason: "client timestamp newer (LWW)",
            }
        } else {
            Resolution {
                winner: "server",
                reason: "server version is newer",
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ServerWins resolver
// ---------------------------------------------------------------------------

/// Always resolves in favor of the server.
pub struct ServerWinsResolver;

impl ServerWinsResolver {
    pub fn resolve(&self, _conflict: &Conflict) -> Resolution {
        Resolution {
            winner: "server",
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

    fn base_conflict() -> Conflict {
        Conflict {
            table: "items".into(),
            record_id: "r1".into(),
            client_id: "c1".into(),
            user_id: "u1".into(),
            client_time: ts(1000),
            server_time: ts(1000),
            base_version: None,
        }
    }

    fn resolver(tolerance_ms: i64) -> LwwResolver {
        LwwResolver::new(chrono::Duration::milliseconds(tolerance_ms))
    }

    // Pure LWW (no base version)

    #[test]
    fn lww_client_newer() {
        let mut c = base_conflict();
        c.client_time = ts(1001);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "client");
        assert_eq!(r.reason, "client timestamp newer (LWW)");
    }

    #[test]
    fn lww_server_newer() {
        let mut c = base_conflict();
        c.client_time = ts(999);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "server");
        assert_eq!(r.reason, "server version is newer");
    }

    #[test]
    fn lww_equal_server_wins() {
        let c = base_conflict();
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "server");
    }

    #[test]
    fn lww_clock_skew_tips_client() {
        let mut c = base_conflict();
        c.client_time = ts(999);
        c.server_time = ts(1000);
        // 500ms tolerance not enough (1s behind).
        assert_eq!(resolver(500).resolve(&c).winner, "server");
        // 1001ms tolerance tips it to client.
        assert_eq!(resolver(1001).resolve(&c).winner, "client");
    }

    // With base version

    #[test]
    fn base_ahead_of_server() {
        let mut c = base_conflict();
        c.base_version = Some(ts(2000));
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "server");
        assert_eq!(r.reason, "client base version is ahead of server version");
    }

    #[test]
    fn base_equals_server_client_wins() {
        let mut c = base_conflict();
        c.base_version = Some(ts(1000));
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "client");
        assert_eq!(r.reason, "server unchanged since base version");
    }

    #[test]
    fn base_behind_server_lww_client_newer() {
        let mut c = base_conflict();
        c.base_version = Some(ts(900));
        c.client_time = ts(1100);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "client");
        assert_eq!(r.reason, "client timestamp newer (LWW with base)");
    }

    #[test]
    fn base_behind_server_lww_server_newer() {
        let mut c = base_conflict();
        c.base_version = Some(ts(900));
        c.client_time = ts(999);
        c.server_time = ts(1000);
        let r = resolver(0).resolve(&c);
        assert_eq!(r.winner, "server");
        assert_eq!(r.reason, "server version is newer");
    }

    // ServerWins

    #[test]
    fn server_wins_always() {
        let c = base_conflict();
        let r = ServerWinsResolver.resolve(&c);
        assert_eq!(r.winner, "server");
        assert_eq!(r.reason, "server always wins");
    }
}
