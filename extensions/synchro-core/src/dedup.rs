use crate::protocol::Operation;

/// A reference to a changelog entry, used during deduplication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRef {
    pub table_name: String,
    pub record_id: String,
    pub bucket_id: String,
    pub operation: Operation,
    pub seq: i64,
}

/// Input entry for deduplication (mirrors ChangelogEntry fields needed).
#[derive(Debug, Clone)]
pub struct ChangelogEntry {
    pub seq: i64,
    pub bucket_id: String,
    pub table_name: String,
    pub record_id: String,
    pub operation: Operation,
}

/// Deduplicates changelog entries, keeping only the latest entry per record.
///
/// Entries are assumed to be ordered by seq (ascending). For duplicate records,
/// the later entry replaces the earlier one **at the same position** in the
/// output slice, preserving the original order of first appearance.
///
/// Matches Go `deduplicateEntries` exactly.
pub fn deduplicate_entries(entries: &[ChangelogEntry]) -> Vec<RecordRef> {
    let mut seen = std::collections::HashMap::<String, usize>::with_capacity(entries.len());
    let mut refs: Vec<RecordRef> = Vec::with_capacity(entries.len());

    for e in entries {
        let key = format!("{}:{}", e.table_name, e.record_id);
        if let Some(&idx) = seen.get(&key) {
            refs[idx] = RecordRef {
                table_name: e.table_name.clone(),
                record_id: e.record_id.clone(),
                bucket_id: e.bucket_id.clone(),
                operation: e.operation,
                seq: e.seq,
            };
        } else {
            seen.insert(key, refs.len());
            refs.push(RecordRef {
                table_name: e.table_name.clone(),
                record_id: e.record_id.clone(),
                bucket_id: e.bucket_id.clone(),
                operation: e.operation,
                seq: e.seq,
            });
        }
    }

    refs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seq: i64, table: &str, id: &str, bucket: &str, op: Operation) -> ChangelogEntry {
        ChangelogEntry {
            seq,
            bucket_id: bucket.into(),
            table_name: table.into(),
            record_id: id.into(),
            operation: op,
        }
    }

    #[test]
    fn no_duplicates() {
        let entries = vec![
            entry(1, "items", "a", "b1", Operation::Insert),
            entry(2, "items", "b", "b1", Operation::Insert),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].record_id, "a");
        assert_eq!(refs[1].record_id, "b");
    }

    #[test]
    fn duplicate_keeps_latest_at_same_position() {
        let entries = vec![
            entry(1, "items", "a", "b1", Operation::Insert),
            entry(2, "items", "b", "b1", Operation::Insert),
            entry(3, "items", "a", "b1", Operation::Update),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 2);
        // "a" stays at position 0, but updated to seq 3 / Update.
        assert_eq!(refs[0].record_id, "a");
        assert_eq!(refs[0].seq, 3);
        assert_eq!(refs[0].operation, Operation::Update);
        // "b" remains at position 1.
        assert_eq!(refs[1].record_id, "b");
        assert_eq!(refs[1].seq, 2);
    }

    #[test]
    fn insert_then_delete() {
        let entries = vec![
            entry(1, "items", "a", "b1", Operation::Insert),
            entry(2, "items", "a", "b1", Operation::Delete),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].operation, Operation::Delete);
        assert_eq!(refs[0].seq, 2);
    }

    #[test]
    fn different_tables_not_deduped() {
        let entries = vec![
            entry(1, "items", "a", "b1", Operation::Insert),
            entry(2, "orders", "a", "b1", Operation::Insert),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn empty_input() {
        let refs = deduplicate_entries(&[]);
        assert!(refs.is_empty());
    }

    #[test]
    fn triple_update_keeps_last() {
        let entries = vec![
            entry(1, "items", "a", "b1", Operation::Insert),
            entry(5, "items", "a", "b1", Operation::Update),
            entry(10, "items", "a", "b1", Operation::Delete),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].seq, 10);
        assert_eq!(refs[0].operation, Operation::Delete);
    }

    #[test]
    fn large_batch_performance() {
        // 10k entries for 1k unique records (10 updates each).
        // Must not be O(n^2) or blow up.
        let mut entries = Vec::with_capacity(10_000);
        for i in 0..10_000i64 {
            let record_id = format!("r{}", i % 1000);
            entries.push(entry(i, "items", &record_id, "b1", Operation::Update));
        }
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 1000);
        // Each record should have seq from its last occurrence (9000+).
        for r in &refs {
            assert!(r.seq >= 9000);
        }
    }

    #[test]
    fn alternating_insert_delete() {
        // Same record: Insert, Delete, Insert, Delete.
        // Final state must be Delete at the last seq.
        let entries = vec![
            entry(1, "items", "a", "b1", Operation::Insert),
            entry(2, "items", "a", "b1", Operation::Delete),
            entry(3, "items", "a", "b1", Operation::Insert),
            entry(4, "items", "a", "b1", Operation::Delete),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].seq, 4);
        assert_eq!(refs[0].operation, Operation::Delete);
    }

    #[test]
    fn different_buckets_same_record_deduped() {
        // Same table+record appearing in different buckets. Dedup key is
        // (table_name, record_id), NOT (table_name, record_id, bucket_id).
        // The later entry replaces the earlier regardless of bucket.
        let entries = vec![
            entry(1, "items", "a", "bucket_x", Operation::Insert),
            entry(2, "items", "a", "bucket_y", Operation::Update),
        ];
        let refs = deduplicate_entries(&entries);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].seq, 2);
        assert_eq!(refs[0].bucket_id, "bucket_y");
        assert_eq!(refs[0].operation, Operation::Update);
    }
}
