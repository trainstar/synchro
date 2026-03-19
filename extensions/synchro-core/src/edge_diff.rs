use crate::protocol::Operation;
use std::collections::HashSet;

/// A changelog entry to write after computing the edge diff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogEntry {
    pub bucket_id: String,
    pub table_name: String,
    pub record_id: String,
    pub operation: Operation,
}

/// The three-way bucket diff: which buckets were added, kept, or removed.
#[derive(Debug, Clone, Default)]
pub struct BucketDiff {
    pub added: Vec<String>,
    pub kept: Vec<String>,
    pub removed: Vec<String>,
}

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
    operation: Operation,
    existing: &[String],
    desired: &[String],
) -> Vec<ChangelogEntry> {
    let existing = dedup_buckets(existing);
    let desired = dedup_buckets(desired);

    let existing_set: HashSet<&str> = existing.iter().map(|s| s.as_str()).collect();
    let desired_set: HashSet<&str> = desired.iter().map(|s| s.as_str()).collect();

    let mut out = Vec::with_capacity(existing.len() + desired.len());

    if operation == Operation::Delete {
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
                operation: Operation::Delete,
            });
        }
        return out;
    }

    for bucket in &desired {
        if existing_set.contains(bucket.as_str()) {
            // Kept: use event op, but Insert -> Update for re-added records.
            let op = if operation == Operation::Insert {
                Operation::Update
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
                operation: Operation::Insert,
            });
        }
    }

    for bucket in &existing {
        if !desired_set.contains(bucket.as_str()) {
            out.push(ChangelogEntry {
                bucket_id: bucket.clone(),
                table_name: table_name.into(),
                record_id: record_id.into(),
                operation: Operation::Delete,
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
        return buckets
            .iter()
            .filter(|b| !b.is_empty())
            .cloned()
            .collect();
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
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            Operation::Insert,
            &[],
            &[s("user:u1")],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucket_id, "user:u1");
        assert_eq!(entries[0].operation, Operation::Insert);
    }

    #[test]
    fn edge_diff_update_kept_bucket() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            Operation::Update,
            &[s("user:u1")],
            &[s("user:u1")],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, Operation::Update);
    }

    #[test]
    fn edge_diff_insert_on_existing_becomes_update() {
        // Insert op on a kept bucket becomes Update.
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            Operation::Insert,
            &[s("user:u1")],
            &[s("user:u1")],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operation, Operation::Update);
    }

    #[test]
    fn edge_diff_bucket_change() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            Operation::Update,
            &[s("user:u1")],
            &[s("user:u2")],
        );
        assert_eq!(entries.len(), 2);
        // Added to new bucket.
        assert_eq!(entries[0].bucket_id, "user:u2");
        assert_eq!(entries[0].operation, Operation::Insert);
        // Removed from old bucket.
        assert_eq!(entries[1].bucket_id, "user:u1");
        assert_eq!(entries[1].operation, Operation::Delete);
    }

    #[test]
    fn edge_diff_delete_emits_for_existing() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            Operation::Delete,
            &[s("user:u1"), s("user:u2")],
            &[],
        );
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.operation == Operation::Delete));
    }

    #[test]
    fn edge_diff_delete_no_existing_uses_desired() {
        let entries = build_edge_diff_entries(
            "items",
            "r1",
            Operation::Delete,
            &[],
            &[s("user:u1")],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucket_id, "user:u1");
        assert_eq!(entries[0].operation, Operation::Delete);
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
}
