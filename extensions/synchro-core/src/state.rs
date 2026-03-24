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
    pub from: ClientSyncState,
    pub to: ClientSyncState,
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
    pub fn parse(raw: &str) -> Result<Self, String> {
        let parts: Vec<&str> = raw.split("->").collect();
        if parts.len() != 2 {
            return Err(format!("invalid transition: {raw}"));
        }
        let from = parts[0].trim().parse()?;
        let to = parts[1].trim().parse()?;
        Ok(Self { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn trace_fixture() -> Value {
        serde_json::from_str(include_str!(
            "../../../conformance/traces/offline-write-before-first-connect.json"
        ))
        .unwrap()
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
            ClientTransition {
                from: ClientSyncState::Uninitialized,
                to: ClientSyncState::LocalReady
            }
        );
        assert_eq!(
            parsed[3],
            ClientTransition {
                from: ClientSyncState::Ready,
                to: ClientSyncState::Pushing
            }
        );
    }
}
