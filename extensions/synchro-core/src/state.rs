use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientSyncState {
    Uninitialized,
    LocalReady,
    Connecting,
    SchemaApplying,
    Ready,
    Pushing,
    Pulling,
    Rebuilding,
    Backoff,
    Error,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientTransition {
    from: ClientSyncState,
    to: ClientSyncState,
}

impl fmt::Display for ClientSyncState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Uninitialized => "uninitialized",
            Self::LocalReady => "local_ready",
            Self::Connecting => "connecting",
            Self::SchemaApplying => "schema_applying",
            Self::Ready => "ready",
            Self::Pushing => "pushing",
            Self::Pulling => "pulling",
            Self::Rebuilding => "rebuilding",
            Self::Backoff => "backoff",
            Self::Error => "error",
            Self::Stopped => "stopped",
        };
        f.write_str(s)
    }
}

impl FromStr for ClientSyncState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "uninitialized" => Ok(Self::Uninitialized),
            "local_ready" => Ok(Self::LocalReady),
            "connecting" => Ok(Self::Connecting),
            "schema_applying" => Ok(Self::SchemaApplying),
            "ready" => Ok(Self::Ready),
            "pushing" => Ok(Self::Pushing),
            "pulling" => Ok(Self::Pulling),
            "rebuilding" => Ok(Self::Rebuilding),
            "backoff" => Ok(Self::Backoff),
            "error" => Ok(Self::Error),
            "stopped" => Ok(Self::Stopped),
            _ => Err(format!("unknown client sync state: {s}")),
        }
    }
}

impl ClientTransition {
    pub fn new(from: ClientSyncState, to: ClientSyncState) -> Result<Self, String> {
        if Self::is_legal(from, to) {
            Ok(Self { from, to })
        } else {
            Err(format!("illegal client sync transition: {from} -> {to}"))
        }
    }

    pub const fn is_legal(from: ClientSyncState, to: ClientSyncState) -> bool {
        matches!(
            (from, to),
            (
                ClientSyncState::Uninitialized,
                ClientSyncState::LocalReady | ClientSyncState::Error | ClientSyncState::Stopped
            ) | (
                ClientSyncState::LocalReady,
                ClientSyncState::Connecting | ClientSyncState::Error | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Connecting,
                ClientSyncState::SchemaApplying
                    | ClientSyncState::Ready
                    | ClientSyncState::Backoff
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::SchemaApplying,
                ClientSyncState::Ready
                    | ClientSyncState::Rebuilding
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Ready,
                ClientSyncState::Connecting
                    | ClientSyncState::Pushing
                    | ClientSyncState::Pulling
                    | ClientSyncState::Rebuilding
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Pushing,
                ClientSyncState::Pushing
                    | ClientSyncState::Ready
                    | ClientSyncState::Pulling
                    | ClientSyncState::Connecting
                    | ClientSyncState::Backoff
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Pulling,
                ClientSyncState::Pulling
                    | ClientSyncState::Ready
                    | ClientSyncState::Rebuilding
                    | ClientSyncState::Connecting
                    | ClientSyncState::Backoff
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Rebuilding,
                ClientSyncState::Rebuilding
                    | ClientSyncState::Ready
                    | ClientSyncState::Connecting
                    | ClientSyncState::Backoff
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Backoff,
                ClientSyncState::Connecting
                    | ClientSyncState::Pushing
                    | ClientSyncState::Pulling
                    | ClientSyncState::Rebuilding
                    | ClientSyncState::Error
                    | ClientSyncState::Stopped
            ) | (
                ClientSyncState::Error,
                ClientSyncState::LocalReady | ClientSyncState::Stopped
            ) | (ClientSyncState::Stopped, ClientSyncState::LocalReady)
        )
    }

    pub const fn from(&self) -> ClientSyncState {
        self.from
    }

    pub const fn to(&self) -> ClientSyncState {
        self.to
    }

    pub fn parse(raw: &str) -> Result<Self, String> {
        let parts: Vec<&str> = raw.split("->").collect();
        if parts.len() != 2 {
            return Err(format!("invalid transition: {raw}"));
        }
        let from = parts[0].trim().parse()?;
        let to = parts[1].trim().parse()?;
        Self::new(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn trace_fixture() -> Value {
        let path = std::env::var_os("SYNCHRO_REPO_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .join("conformance/traces/offline-write-before-first-connect.json");
        let authored = std::fs::read_to_string(path).expect("authored trace must be readable");
        serde_json::from_str(&authored).expect("authored trace must be valid JSON")
    }

    #[test]
    fn state_round_trip_strings() {
        for state in [
            ClientSyncState::Uninitialized,
            ClientSyncState::LocalReady,
            ClientSyncState::Connecting,
            ClientSyncState::SchemaApplying,
            ClientSyncState::Ready,
            ClientSyncState::Pushing,
            ClientSyncState::Pulling,
            ClientSyncState::Rebuilding,
            ClientSyncState::Backoff,
            ClientSyncState::Error,
            ClientSyncState::Stopped,
        ] {
            let raw = state.to_string();
            let parsed: ClientSyncState = raw.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn parses_phase0_trace_transitions() {
        let doc = trace_fixture();
        let transitions = doc["expected_transitions"].as_array().unwrap();
        let parsed: Vec<ClientTransition> = transitions
            .iter()
            .map(|t| ClientTransition::parse(t.as_str().unwrap()).unwrap())
            .collect();

        assert_eq!(
            parsed[0],
            ClientTransition::new(ClientSyncState::Uninitialized, ClientSyncState::LocalReady)
                .unwrap()
        );
        assert_eq!(
            parsed[3],
            ClientTransition::new(ClientSyncState::Ready, ClientSyncState::Pushing).unwrap()
        );
    }

    #[test]
    fn legal_adjacency_map_is_exact() {
        let states = [
            ClientSyncState::Uninitialized,
            ClientSyncState::LocalReady,
            ClientSyncState::Connecting,
            ClientSyncState::SchemaApplying,
            ClientSyncState::Ready,
            ClientSyncState::Pushing,
            ClientSyncState::Pulling,
            ClientSyncState::Rebuilding,
            ClientSyncState::Backoff,
            ClientSyncState::Error,
            ClientSyncState::Stopped,
        ];
        let legal = [
            (ClientSyncState::Uninitialized, ClientSyncState::LocalReady),
            (ClientSyncState::Uninitialized, ClientSyncState::Error),
            (ClientSyncState::Uninitialized, ClientSyncState::Stopped),
            (ClientSyncState::LocalReady, ClientSyncState::Connecting),
            (ClientSyncState::LocalReady, ClientSyncState::Error),
            (ClientSyncState::LocalReady, ClientSyncState::Stopped),
            (ClientSyncState::Connecting, ClientSyncState::SchemaApplying),
            (ClientSyncState::Connecting, ClientSyncState::Ready),
            (ClientSyncState::Connecting, ClientSyncState::Backoff),
            (ClientSyncState::Connecting, ClientSyncState::Error),
            (ClientSyncState::Connecting, ClientSyncState::Stopped),
            (ClientSyncState::SchemaApplying, ClientSyncState::Ready),
            (ClientSyncState::SchemaApplying, ClientSyncState::Rebuilding),
            (ClientSyncState::SchemaApplying, ClientSyncState::Error),
            (ClientSyncState::SchemaApplying, ClientSyncState::Stopped),
            (ClientSyncState::Ready, ClientSyncState::Connecting),
            (ClientSyncState::Ready, ClientSyncState::Pushing),
            (ClientSyncState::Ready, ClientSyncState::Pulling),
            (ClientSyncState::Ready, ClientSyncState::Rebuilding),
            (ClientSyncState::Ready, ClientSyncState::Error),
            (ClientSyncState::Ready, ClientSyncState::Stopped),
            (ClientSyncState::Pushing, ClientSyncState::Pushing),
            (ClientSyncState::Pushing, ClientSyncState::Ready),
            (ClientSyncState::Pushing, ClientSyncState::Pulling),
            (ClientSyncState::Pushing, ClientSyncState::Connecting),
            (ClientSyncState::Pushing, ClientSyncState::Backoff),
            (ClientSyncState::Pushing, ClientSyncState::Error),
            (ClientSyncState::Pushing, ClientSyncState::Stopped),
            (ClientSyncState::Pulling, ClientSyncState::Pulling),
            (ClientSyncState::Pulling, ClientSyncState::Ready),
            (ClientSyncState::Pulling, ClientSyncState::Rebuilding),
            (ClientSyncState::Pulling, ClientSyncState::Connecting),
            (ClientSyncState::Pulling, ClientSyncState::Backoff),
            (ClientSyncState::Pulling, ClientSyncState::Error),
            (ClientSyncState::Pulling, ClientSyncState::Stopped),
            (ClientSyncState::Rebuilding, ClientSyncState::Rebuilding),
            (ClientSyncState::Rebuilding, ClientSyncState::Ready),
            (ClientSyncState::Rebuilding, ClientSyncState::Connecting),
            (ClientSyncState::Rebuilding, ClientSyncState::Backoff),
            (ClientSyncState::Rebuilding, ClientSyncState::Error),
            (ClientSyncState::Rebuilding, ClientSyncState::Stopped),
            (ClientSyncState::Backoff, ClientSyncState::Connecting),
            (ClientSyncState::Backoff, ClientSyncState::Pushing),
            (ClientSyncState::Backoff, ClientSyncState::Pulling),
            (ClientSyncState::Backoff, ClientSyncState::Rebuilding),
            (ClientSyncState::Backoff, ClientSyncState::Error),
            (ClientSyncState::Backoff, ClientSyncState::Stopped),
            (ClientSyncState::Error, ClientSyncState::LocalReady),
            (ClientSyncState::Error, ClientSyncState::Stopped),
            (ClientSyncState::Stopped, ClientSyncState::LocalReady),
        ];

        for from in states {
            for to in states {
                assert_eq!(
                    ClientTransition::is_legal(from, to),
                    legal.contains(&(from, to)),
                    "{from} -> {to}"
                );
                assert_eq!(
                    ClientTransition::new(from, to).is_ok(),
                    legal.contains(&(from, to))
                );
            }
        }
    }

    #[test]
    fn parsing_rejects_illegal_transition() {
        assert!(ClientTransition::parse("stopped -> pulling").is_err());
    }
}
