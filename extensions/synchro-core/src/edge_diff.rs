//! Edge-diff helpers for scope materialization.
//!
//! The current PostgreSQL extension still uses bucket terminology. These bucket
//! identifiers correspond to deterministic scope instance ids.

use crate::change::ChangeOperation;
use std::collections::HashSet;

/// A changelog entry to write after computing the edge diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogEntry {
    pub bucket_id: String,
    pub table_name: String,
    pub record_id: String,
    pub operation: ChangeOperation,
}

impl ChangelogEntry {
    pub fn scope_id(&self) -> &str {
        &self.bucket_id
    }
}

/// The three-way bucket diff: which buckets were added, kept, or removed.
#[derive(Debug, Clone, Default)]
pub struct BucketDiff {
    pub added: Vec<String>,
    pub kept: Vec<String>,
    pub removed: Vec<String>,
}

pub type ScopeDiff = BucketDiff;

/// Compute the three-way diff between existing and desired bucket sets.
///
/// Matches Go `diffBucketSets`.
pub fn diff_bucket_sets(existing: &[String], desired: &[String]) -> BucketDiff {
    let existing_set: HashSet<&str> = existing.iter().map(|s| s.as_str()).collect();
    let desired_set: HashSet<&str> = desired.iter().map(|s| s.as_str()).collect();

    let mut diff = BucketDiff::default();

    for b in desired {
        if existing_set.contains(b.as_str()) {
            diff.kept.push(b.clone());
        } else {
            diff.added.push(b.clone());
        }
    }

    for b in existing {
        if !desired_set.contains(b.as_str()) {
            diff.removed.push(b.clone());
        }
    }

    diff
}

pub fn diff_scope_sets(existing: &[String], desired: &[String]) -> ScopeDiff {
    diff_bucket_sets(existing, desired)
}

/// Build changelog entries from the edge diff between existing and desired
/// buckets for a given WAL event.
///
/// Matches Go `buildEdgeDiffEntries`:
/// - For deletes: emit OpDelete for each existing (or desired) bucket.
/// - For non-deletes:
///   - Kept buckets: emit the event's operation (Insert -> Update if re-added).
///   - Added buckets: emit OpInsert.
///   - Removed buckets: emit OpDelete.
pub fn build_edge_diff_entries(
    table_name: &str,
    record_id: &str,
    operation: ChangeOperation,
    existing: &[String],
    desired: &[String],
) -> Vec<ChangelogEntry> {
    let existing = dedup_buckets(existing);
    let desired = dedup_buckets(desired);

    let existing_set: HashSet<&str> = existing.iter().map(|s| s.as_str()).collect();
    let desired_set: HashSet<&str> = desired.iter().map(|s| s.as_str()).collect();

    let mut out = Vec::with_capacity(existing.len() + desired.len());

    if operation == ChangeOperation::Delete {
        let targets = if existing.is_empty() {
            &desired
        } else {
            &existing
        };
        for bucket in targets {
            out.push(ChangelogEntry {
                bucket_id: bucket.clone(),
                table_name: table_name.into(),
                record_id: record_id.into(),
                operation: ChangeOperation::Delete,
            });
        }
        return out;
    }

    for bucket in &desired {
        if existing_set.contains(bucket.as_str()) {
            // Kept: use event op, but Insert -> Update for re-added records.
            let op = if operation == ChangeOperation::Insert {
                ChangeOperation::Update
            } else {
                operation
            };
            out.push(ChangelogEntry {
                bucket_id: bucket.clone(),
                table_name: table_name.into(),
                record_id: record_id.into(),
                operation: op,
            });
        } else {
            // Added: always Insert.
            out.push(ChangelogEntry {
                bucket_id: bucket.clone(),
                table_name: table_name.into(),
                record_id: record_id.into(),
                operation: ChangeOperation::Insert,
            });
        }
    }

    for bucket in &existing {
        if !desired_set.contains(bucket.as_str()) {
            out.push(ChangelogEntry {
                bucket_id: bucket.clone(),
                table_name: table_name.into(),
                record_id: record_id.into(),
                operation: ChangeOperation::Delete,
            });
        }
    }

    out
}

/// Remove duplicate and empty bucket IDs, preserving order of first occurrence.
///
/// Matches Go `dedupeBuckets`.
pub fn dedup_buckets(buckets: &[String]) -> Vec<String> {
    if buckets.len() < 2 {
        return buckets.iter().filter(|b| !b.is_empty()).cloned().collect();
    }
    let mut seen = HashSet::with_capacity(buckets.len());
    let mut out = Vec::with_capacity(buckets.len());
    for b in buckets {
        if b.is_empty() {
            continue;
        }
        if seen.insert(b.as_str()) {
            out.push(b.clone());
        }
    }
    out
}

pub fn dedup_scope_ids(scope_ids: &[String]) -> Vec<String> {
    dedup_buckets(scope_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> String {
        v.into()
    }

    #[test]
    fn diff_bucket_sets_basic() {
        let existing = vec![s("a"), s("b"), s("c")];
        let desired = vec![s("b"), s("c"), s("d")];
        let diff = diff_bucket_sets(&existing, &desired);
        assert_eq!(diff.added, vec![s("d")]);
        assert!(diff.kept.contains(&s("b")));
        assert!(diff.kept.contains(&s("c")));
        assert_eq!(diff.removed, vec![s("a")]);
    }

    #[test]
    fn diff_bucket_sets_all_new() {
        let diff = diff_bucket_sets(&[], &[s("x"), s("y")]);
        assert_eq!(diff.added, vec![s("x"), s("y")]);
        assert!(diff.kept.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn diff_bucket_sets_all_removed() {
        let diff = diff_bucket_sets(&[s("x"), s("y")], &[]);
        assert!(diff.added.is_empty());
        assert!(diff.kept.is_empty());
        assert_eq!(diff.removed, vec![s("x"), s("y")]);
    }

    #[test]
    fn edge_diff_insert_new_record() {
        let entries =
            build_edge_diff_entries("items", "r1", ChangeOperation::Insert, &[], &[s("user:u1")]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucket_id, "user:u1");
        assert_eq!(entries[0].scope_id(), "user:u1");
        assert_eq!(entries[0].operation, ChangeOperation::Insert);
    }

    #[test]
    fn edge_diff_update_kept_bucket() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            ChangeOperation::Update,
            &[s("user:u1")],
            &[s("user:u1")],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, ChangeOperation::Update);
    }

    #[test]
    fn edge_diff_insert_on_existing_becomes_update() {
        // Insert op on a kept bucket becomes Update.
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            ChangeOperation::Insert,
            &[s("user:u1")],
            &[s("user:u1")],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, ChangeOperation::Update);
    }

    #[test]
    fn edge_diff_bucket_change() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            ChangeOperation::Update,
            &[s("user:u1")],
            &[s("user:u2")],
        );
        assert_eq!(entries.len(), 2);
        // Added to new bucket.
        assert_eq!(entries[0].bucket_id, "user:u2");
        assert_eq!(entries[0].operation, ChangeOperation::Insert);
        // Removed from old bucket.
        assert_eq!(entries[1].bucket_id, "user:u1");
        assert_eq!(entries[1].operation, ChangeOperation::Delete);
    }

    #[test]
    fn edge_diff_delete_emits_for_existing() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            ChangeOperation::Delete,
            &[s("user:u1"), s("user:u2")],
            &[],
        );
        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .all(|e| e.operation == ChangeOperation::Delete));
    }

    #[test]
    fn edge_diff_delete_no_existing_uses_desired() {
        let entries =
            build_edge_diff_entries("items", "r1", ChangeOperation::Delete, &[], &[s("user:u1")]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucket_id, "user:u1");
        assert_eq!(entries[0].operation, ChangeOperation::Delete);
    }

    #[test]
    fn dedup_buckets_removes_dupes_and_empty() {
        let input = vec![s("a"), s(""), s("b"), s("a"), s("c"), s("b")];
        let result = dedup_buckets(&input);
        assert_eq!(result, vec![s("a"), s("b"), s("c")]);
    }

    #[test]
    fn dedup_buckets_single_item() {
        assert_eq!(dedup_buckets(&[s("x")]), vec![s("x")]);
    }

    #[test]
    fn dedup_buckets_empty() {
        let result: Vec<String> = dedup_buckets(&[]);
        assert!(result.is_empty());
    }

    // Multi-bucket reassignment and fan-out/fan-in edge cases.

    #[test]
    fn diff_multi_bucket_reassignment() {
        // [A,B] -> [B,C]: A removed, B kept, C added.
        // Catches add/keep/remove classification error.
        let existing = vec![s("a"), s("b")];
        let desired = vec![s("b"), s("c")];
        let diff = diff_bucket_sets(&existing, &desired);
        assert_eq!(diff.added, vec![s("c")]);
        assert_eq!(diff.kept, vec![s("b")]);
        assert_eq!(diff.removed, vec![s("a")]);
    }

    #[test]
    fn diff_fan_out_zero_to_five() {
        // Empty existing -> 5 desired: all added, nothing kept or removed.
        let desired: Vec<String> = (0..5).map(|i| format!("b{i}")).collect();
        let diff = diff_bucket_sets(&[], &desired);
        assert_eq!(diff.added.len(), 5);
        assert!(diff.kept.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn diff_fan_in_five_to_one() {
        // 5 existing -> 1 desired: 4 removed, 1 kept, 0 added.
        let existing: Vec<String> = (0..5).map(|i| format!("b{i}")).collect();
        let desired = vec![s("b2")];
        let diff = diff_bucket_sets(&existing, &desired);
        assert!(diff.added.is_empty());
        assert_eq!(diff.kept, vec![s("b2")]);
        assert_eq!(diff.removed.len(), 4);
    }

    #[test]
    fn diff_identical_sets() {
        // Identical existing and desired: all kept, nothing added or removed.
        let buckets = vec![s("a"), s("b"), s("c")];
        let diff = diff_bucket_sets(&buckets, &buckets);
        assert!(diff.added.is_empty());
        assert_eq!(diff.kept.len(), 3);
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn scope_aliases_match_bucket_helpers() {
        let existing = vec![s("scope_a")];
        let desired = vec![s("scope_a"), s("scope_b"), s("")];
        let diff = diff_scope_sets(&existing, &desired);
        assert_eq!(diff.added, vec![s("scope_b"), s("")]);
        assert_eq!(dedup_scope_ids(&desired), vec![s("scope_a"), s("scope_b")]);
    }
}
