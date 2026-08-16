use std::cmp::Ordering;

use pgrx::spi::SpiClient;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "position_kind", rename_all = "snake_case")]
pub(crate) enum StreamPosition {
    GenerationStart,
    Effect {
        #[serde(with = "lsn_serde")]
        commit_lsn: u64,
        event_ordinal: i64,
        effect_ordinal: i32,
    },
    TransactionEnd {
        #[serde(with = "lsn_serde")]
        commit_lsn: u64,
    },
}

impl StreamPosition {
    pub(crate) fn effect(
        commit_lsn: &str,
        event_ordinal: i64,
        effect_ordinal: i32,
    ) -> Result<Self, String> {
        if event_ordinal < 0 || effect_ordinal < 0 {
            return Err("effect ordinals must be nonnegative".to_string());
        }
        Ok(Self::Effect {
            commit_lsn: parse_lsn(commit_lsn)
                .ok_or_else(|| "effect commit LSN is malformed".to_string())?,
            event_ordinal,
            effect_ordinal,
        })
    }

    pub(crate) fn transaction_end(commit_lsn: &str) -> Result<Self, String> {
        Ok(Self::TransactionEnd {
            commit_lsn: parse_lsn(commit_lsn)
                .ok_or_else(|| "transaction-end commit LSN is malformed".to_string())?,
        })
    }

    pub(crate) fn from_sql_parts(
        kind: &str,
        commit_lsn: Option<&str>,
        event_ordinal: Option<i64>,
        effect_ordinal: Option<i32>,
    ) -> Result<Self, String> {
        match (kind, commit_lsn, event_ordinal, effect_ordinal) {
            ("generation_start", None, None, None) => Ok(Self::GenerationStart),
            ("effect", Some(commit_lsn), Some(event_ordinal), Some(effect_ordinal)) => {
                Self::effect(commit_lsn, event_ordinal, effect_ordinal)
            }
            ("transaction_end", Some(commit_lsn), None, None) => Self::transaction_end(commit_lsn),
            _ => Err("stored stream position is malformed".to_string()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::GenerationStart | Self::TransactionEnd { .. } => Ok(()),
            Self::Effect {
                event_ordinal,
                effect_ordinal,
                ..
            } if *event_ordinal >= 0 && *effect_ordinal >= 0 => Ok(()),
            Self::Effect { .. } => Err("effect ordinals must be nonnegative".to_string()),
        }
    }

    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::GenerationStart => "generation_start",
            Self::Effect { .. } => "effect",
            Self::TransactionEnd { .. } => "transaction_end",
        }
    }

    pub(crate) fn commit_lsn(&self) -> Option<String> {
        match self {
            Self::GenerationStart => None,
            Self::Effect { commit_lsn, .. } | Self::TransactionEnd { commit_lsn } => {
                Some(format_lsn(*commit_lsn))
            }
        }
    }

    pub(crate) const fn event_ordinal(&self) -> Option<i64> {
        match self {
            Self::Effect { event_ordinal, .. } => Some(*event_ordinal),
            Self::GenerationStart | Self::TransactionEnd { .. } => None,
        }
    }

    pub(crate) const fn effect_ordinal(&self) -> Option<i32> {
        match self {
            Self::Effect { effect_ordinal, .. } => Some(*effect_ordinal),
            Self::GenerationStart | Self::TransactionEnd { .. } => None,
        }
    }
}

impl Ord for StreamPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::GenerationStart, Self::GenerationStart) => Ordering::Equal,
            (Self::GenerationStart, _) => Ordering::Less,
            (_, Self::GenerationStart) => Ordering::Greater,
            (
                Self::Effect {
                    commit_lsn: left_lsn,
                    event_ordinal: left_event,
                    effect_ordinal: left_effect,
                },
                Self::Effect {
                    commit_lsn: right_lsn,
                    event_ordinal: right_event,
                    effect_ordinal: right_effect,
                },
            ) => (*left_lsn, *left_event, *left_effect).cmp(&(
                *right_lsn,
                *right_event,
                *right_effect,
            )),
            (
                Self::TransactionEnd {
                    commit_lsn: left_lsn,
                },
                Self::TransactionEnd {
                    commit_lsn: right_lsn,
                },
            ) => left_lsn.cmp(right_lsn),
            (
                Self::Effect {
                    commit_lsn: left_lsn,
                    ..
                },
                Self::TransactionEnd {
                    commit_lsn: right_lsn,
                },
            ) => left_lsn.cmp(right_lsn).then(Ordering::Less),
            (
                Self::TransactionEnd {
                    commit_lsn: left_lsn,
                },
                Self::Effect {
                    commit_lsn: right_lsn,
                    ..
                },
            ) => left_lsn.cmp(right_lsn).then(Ordering::Greater),
        }
    }
}

impl PartialOrd for StreamPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamBoundary {
    pub(crate) stream_generation: String,
    pub(crate) position: StreamPosition,
}

pub(crate) fn load_materialized_boundary(client: &SpiClient<'_>) -> Result<StreamBoundary, String> {
    let rows = client
        .select(
            "SELECT rs.stream_generation AS runtime_generation,
                    p.stream_generation AS progress_generation,
                    p.materialized_commit_lsn::text AS commit_lsn
             FROM sync_runtime_state rs
             JOIN sync_wal_progress p ON p.singleton = rs.singleton
             WHERE rs.singleton = true",
            None,
            &[],
        )
        .map_err(|error| format!("loading materialization boundary: {error}"))?;
    let row = rows
        .into_iter()
        .next()
        .ok_or_else(|| "materialization boundary is missing".to_string())?;
    let runtime_generation = row
        .get_by_name::<String, &str>("runtime_generation")
        .map_err(|error| format!("reading runtime stream generation: {error}"))?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "runtime stream generation is missing".to_string())?;
    let progress_generation = row
        .get_by_name::<String, &str>("progress_generation")
        .map_err(|error| format!("reading progress stream generation: {error}"))?
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "progress stream generation is missing".to_string())?;
    if progress_generation != runtime_generation {
        return Err("materialization boundary has the wrong stream generation".to_string());
    }
    let commit_lsn = row
        .get_by_name::<String, &str>("commit_lsn")
        .map_err(|error| format!("reading materialized commit LSN: {error}"))?;
    let position = match commit_lsn {
        Some(commit_lsn) => StreamPosition::transaction_end(&commit_lsn)?,
        None => StreamPosition::GenerationStart,
    };
    Ok(StreamBoundary {
        stream_generation: runtime_generation,
        position,
    })
}

pub(crate) fn parse_lsn(value: &str) -> Option<u64> {
    let (high, low) = value.split_once('/')?;
    let high = u64::from_str_radix(high, 16).ok()?;
    let low = u64::from_str_radix(low, 16).ok()?;
    high.checked_shl(32)?.checked_add(low)
}

pub(crate) fn format_lsn(value: u64) -> String {
    format!("{:X}/{:08X}", value >> 32, value & 0xffff_ffff)
}

mod lsn_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&super::format_lsn(*value))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        super::parse_lsn(&value).ok_or_else(|| serde::de::Error::custom("commit LSN is malformed"))
    }
}

#[cfg(test)]
mod tests {
    use super::StreamPosition;

    #[test]
    fn stream_positions_use_protocol_order() {
        let start = StreamPosition::GenerationStart;
        let first = StreamPosition::effect("0/00000001", 0, 0).unwrap();
        let second = StreamPosition::effect("0/00000001", 0, 1).unwrap();
        let transaction_end = StreamPosition::transaction_end("0/00000001").unwrap();
        let next = StreamPosition::effect("0/00000002", 0, 0).unwrap();

        assert!(start < first);
        assert!(first < second);
        assert!(second < transaction_end);
        assert!(transaction_end < next);
    }
}
