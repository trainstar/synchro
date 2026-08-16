use std::collections::HashMap;

use serde::Serialize;
use synchro_core::change::ChangeOperation;

/// pgoutput message types (the first byte of a logical message).
pub const RELATION_MSG: u8 = b'R';
pub const INSERT_MSG: u8 = b'I';
pub const UPDATE_MSG: u8 = b'U';
pub const DELETE_MSG: u8 = b'D';
pub const BEGIN_MSG: u8 = b'B';
pub const COMMIT_MSG: u8 = b'C';
pub const TYPE_MSG: u8 = b'Y';
pub const ORIGIN_MSG: u8 = b'O';
pub const LOGICAL_MSG: u8 = b'M';
pub const TRUNCATE_MSG: u8 = b'T';

/// Tuple value tags used by pgoutput.
pub const COL_NULL: u8 = b'n';
pub const COL_TEXT: u8 = b't';
pub const COL_BINARY: u8 = b'b';
pub const COL_UNCHANGED: u8 = b'u';

/// A schema-qualified physical relation identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct RelationKey {
    pub namespace: String,
    pub name: String,
    pub oid: u32,
}

impl RelationKey {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>, oid: u32) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            oid,
        }
    }
}

/// A tuple value. Bytes are retained exactly, including invalid UTF-8.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TupleValue {
    Null,
    Text(Vec<u8>),
    Binary(Vec<u8>),
    Unchanged,
}

pub type TupleImage = HashMap<String, TupleValue>;

/// Cached relation metadata from a pgoutput Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationInfo {
    pub relation_name: String,
    pub namespace: String,
    pub relation_oid: u32,
    /// pgoutput replica identity code. `b'd'` is DEFAULT.
    pub replica_identity: u8,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub is_key: bool,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// One decoded row-change event. Its ordinal is assigned before filtering.
#[derive(Debug, Clone, Serialize)]
pub struct WalEvent {
    pub operation: ChangeOperation,
    pub relation: RelationKey,
    pub event_ordinal: u64,
    pub before: Option<TupleImage>,
    pub after: Option<TupleImage>,
}

/// A registered TRUNCATE record. Truncate is intentionally not a row operation.
#[derive(Debug, Clone, Serialize)]
pub struct WalTruncate {
    pub relation: RelationKey,
    pub event_ordinal: u64,
}

/// One transactional logical message retained in its source order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalLogicalMessage {
    pub prefix: String,
    pub content: Vec<u8>,
    pub message_lsn: u64,
}

/// One complete, committed source transaction.
#[derive(Debug, Clone, Serialize)]
pub struct WalTransaction {
    pub xid: u32,
    pub final_lsn: u64,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    /// PostgreSQL commit timestamp in microseconds since 2000-01-01 UTC.
    pub commit_timestamp: i64,
    pub events: Vec<WalEvent>,
    pub truncates: Vec<WalTruncate>,
    pub messages: Vec<WalLogicalMessage>,
}

struct PendingTransaction {
    xid: u32,
    final_lsn: u64,
    commit_timestamp: i64,
    next_ordinal: u64,
    events: Vec<WalEvent>,
    truncates: Vec<WalTruncate>,
    messages: Vec<WalLogicalMessage>,
}

/// Stateful strict decoder for pgoutput protocol version 1.
pub struct WalDecoder {
    relations: HashMap<u32, RelationInfo>,
    transaction: Option<PendingTransaction>,
    failed: bool,
}

impl Default for WalDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl WalDecoder {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            transaction: None,
            failed: false,
        }
    }

    /// Preload relation metadata from the catalog.
    pub fn preload_relations(&mut self, relations: Vec<(RelationKey, u8, Vec<ColumnInfo>)>) {
        for (key, replica_identity, columns) in relations {
            self.relations.insert(
                key.oid,
                RelationInfo {
                    relation_name: key.name,
                    namespace: key.namespace,
                    relation_oid: key.oid,
                    replica_identity,
                    columns,
                },
            );
        }
    }

    pub(crate) fn pending_commit_timestamp(&self) -> Option<i64> {
        self.transaction
            .as_ref()
            .map(|transaction| transaction.commit_timestamp)
    }

    /// Consume one complete pgoutput message.
    ///
    /// A result is non-empty only when COMMIT completes a transaction. Empty
    /// transactions are returned because they are required replay units.
    pub fn feed(&mut self, wal_data: &[u8]) -> Result<Vec<WalTransaction>, DecodeError> {
        if self.failed {
            return Err(DecodeError::InvalidMessage(
                "decoder is poisoned by a prior malformed message".to_string(),
            ));
        }
        let result = self.feed_inner(wal_data);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn feed_inner(&mut self, wal_data: &[u8]) -> Result<Vec<WalTransaction>, DecodeError> {
        let (&tag, data) = wal_data
            .split_first()
            .ok_or_else(|| DecodeError::InvalidMessage("empty logical message".to_string()))?;

        match tag {
            RELATION_MSG => {
                self.handle_relation(data)?;
                Ok(Vec::new())
            }
            BEGIN_MSG => {
                self.handle_begin(data)?;
                Ok(Vec::new())
            }
            COMMIT_MSG => self.handle_commit(data),
            INSERT_MSG => {
                self.handle_insert(data)?;
                Ok(Vec::new())
            }
            UPDATE_MSG => {
                self.handle_update(data)?;
                Ok(Vec::new())
            }
            DELETE_MSG => {
                self.handle_delete(data)?;
                Ok(Vec::new())
            }
            TRUNCATE_MSG => {
                self.handle_truncate(data)?;
                Ok(Vec::new())
            }
            LOGICAL_MSG => {
                self.handle_logical_message(data)?;
                Ok(Vec::new())
            }
            TYPE_MSG => {
                self.handle_type(data)?;
                Ok(Vec::new())
            }
            ORIGIN_MSG => {
                self.handle_origin(data)?;
                Ok(Vec::new())
            }
            _ => Err(DecodeError::InvalidMessage(format!(
                "unknown logical message tag {:?}",
                tag as char
            ))),
        }
    }

    fn handle_begin(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        if self.transaction.is_some() {
            return Err(DecodeError::InvalidMessage(
                "BEGIN encountered inside an open transaction".to_string(),
            ));
        }
        let mut cursor = Cursor::new(data);
        let final_lsn = cursor.read_u64()?;
        let commit_timestamp = cursor.read_i64()?;
        let xid = cursor.read_u32()?;
        cursor.finish()?;
        self.transaction = Some(PendingTransaction {
            xid,
            final_lsn,
            commit_timestamp,
            next_ordinal: 0,
            events: Vec::new(),
            truncates: Vec::new(),
            messages: Vec::new(),
        });
        Ok(())
    }

    fn handle_commit(&mut self, data: &[u8]) -> Result<Vec<WalTransaction>, DecodeError> {
        let pending = self.transaction.take().ok_or_else(|| {
            DecodeError::InvalidMessage("COMMIT encountered without BEGIN".to_string())
        })?;
        let mut cursor = Cursor::new(data);
        let flags = cursor.read_u8()?;
        if flags != 0 {
            return Err(DecodeError::InvalidMessage(
                "COMMIT contains unsupported flags".to_string(),
            ));
        }
        let commit_lsn = cursor.read_u64()?;
        let end_lsn = cursor.read_u64()?;
        let commit_timestamp = cursor.read_i64()?;
        cursor.finish()?;
        if end_lsn < commit_lsn {
            return Err(DecodeError::InvalidMessage(
                "COMMIT end_lsn precedes commit_lsn".to_string(),
            ));
        }
        if pending.final_lsn != commit_lsn {
            return Err(DecodeError::InvalidMessage(
                "BEGIN final_lsn differs from COMMIT commit_lsn".to_string(),
            ));
        }
        if pending.commit_timestamp != commit_timestamp {
            return Err(DecodeError::InvalidMessage(
                "BEGIN and COMMIT timestamps differ".to_string(),
            ));
        }
        Ok(vec![WalTransaction {
            xid: pending.xid,
            final_lsn: pending.final_lsn,
            commit_lsn,
            end_lsn,
            commit_timestamp,
            events: pending.events,
            truncates: pending.truncates,
            messages: pending.messages,
        }])
    }

    fn handle_relation(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_oid = cursor.read_u32()?;
        let namespace = cursor.read_string()?;
        let relation_name = cursor.read_string()?;
        let replica_identity = cursor.read_u8()?;
        let ncols = cursor.read_u16()? as usize;
        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let flags = cursor.read_u8()?;
            let name = cursor.read_string()?;
            let type_oid = cursor.read_u32()?;
            let type_modifier = cursor.read_i32()?;
            if flags & !1 != 0 {
                return Err(DecodeError::InvalidMessage(
                    "RELATION column contains unsupported flags".to_string(),
                ));
            }
            columns.push(ColumnInfo {
                name,
                is_key: flags & 1 == 1,
                type_oid,
                type_modifier,
            });
        }
        cursor.finish()?;

        if let Some(previous) = self.relations.get(&relation_oid) {
            if previous.namespace != namespace
                || previous.relation_name != relation_name
                || previous.replica_identity != replica_identity
                || previous.columns != columns
            {
                return Err(DecodeError::InvalidMessage(format!(
                    "relation OID {} metadata changed",
                    relation_oid
                )));
            }
        }
        self.relations.insert(
            relation_oid,
            RelationInfo {
                relation_name,
                namespace,
                relation_oid,
                replica_identity,
                columns,
            },
        );
        Ok(())
    }

    fn handle_insert(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_oid = cursor.read_u32()?;
        let tuple_type = cursor.read_u8()?;
        if tuple_type != b'N' {
            return Err(DecodeError::InvalidMessage(
                "INSERT must contain a new tuple".to_string(),
            ));
        }
        let rel = self.relation(relation_oid)?.clone();
        let after = self.read_tuple(&mut cursor, &rel.columns)?;
        cursor.finish()?;
        self.add_dml(rel, ChangeOperation::Insert, None, Some(after))
    }

    fn handle_update(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_oid = cursor.read_u32()?;
        let rel = self.relation(relation_oid)?.clone();
        let first_type = cursor.read_u8()?;
        let before = match first_type {
            b'K' => Some(self.read_key_tuple(&mut cursor, &rel)?),
            b'O' => Some(self.read_tuple(&mut cursor, &rel.columns)?),
            b'N' => None,
            _ => {
                return Err(DecodeError::InvalidMessage(
                    "UPDATE contains an invalid tuple kind".to_string(),
                ))
            }
        };
        let new_type = if before.is_some() {
            cursor.read_u8()?
        } else {
            b'N'
        };
        if new_type != b'N' {
            return Err(DecodeError::InvalidMessage(
                "UPDATE must contain a new tuple".to_string(),
            ));
        }
        let after = self.read_tuple(&mut cursor, &rel.columns)?;
        cursor.finish()?;
        self.add_dml(rel, ChangeOperation::Update, before, Some(after))
    }

    fn handle_delete(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_oid = cursor.read_u32()?;
        let rel = self.relation(relation_oid)?.clone();
        let tuple_type = cursor.read_u8()?;
        if tuple_type != b'K' && tuple_type != b'O' {
            return Err(DecodeError::InvalidMessage(
                "DELETE must contain an old or key tuple".to_string(),
            ));
        }
        let before = match tuple_type {
            b'K' => self.read_key_tuple(&mut cursor, &rel)?,
            b'O' => self.read_tuple(&mut cursor, &rel.columns)?,
            _ => unreachable!(),
        };
        cursor.finish()?;
        self.add_dml(rel, ChangeOperation::Delete, Some(before), None)
    }

    fn handle_truncate(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let count = cursor.read_u32()? as usize;
        let options = cursor.read_u8()?;
        if options & !3 != 0 {
            return Err(DecodeError::InvalidMessage(
                "TRUNCATE contains unsupported option bits".to_string(),
            ));
        }
        let mut relations = Vec::with_capacity(count);
        for _ in 0..count {
            let oid = cursor.read_u32()?;
            relations.push(self.relation(oid)?.clone());
        }
        cursor.finish()?;
        let transaction = self.transaction.as_mut().ok_or_else(|| {
            DecodeError::InvalidMessage("TRUNCATE encountered without BEGIN".to_string())
        })?;
        for rel in relations {
            let event_ordinal = next_event_ordinal(transaction)?;
            transaction.truncates.push(WalTruncate {
                relation: RelationKey::new(&rel.namespace, &rel.relation_name, rel.relation_oid),
                event_ordinal,
            });
        }
        Ok(())
    }

    fn handle_logical_message(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let flags = cursor.read_u8()?;
        if flags > 1 {
            return Err(DecodeError::InvalidMessage(
                "logical message contains unsupported flags".to_string(),
            ));
        }
        let message_lsn = cursor.read_u64()?;
        let prefix = cursor.read_string()?;
        let content = cursor.read_len_bytes()?.to_vec();
        cursor.finish()?;

        if flags == 0 {
            return Ok(());
        }

        let transaction = self.transaction.as_mut().ok_or_else(|| {
            DecodeError::InvalidMessage("logical message encountered without BEGIN".to_string())
        })?;
        transaction.messages.push(WalLogicalMessage {
            prefix,
            content,
            message_lsn,
        });
        Ok(())
    }

    fn handle_type(&self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let _type_oid = cursor.read_u32()?;
        let _namespace = cursor.read_string()?;
        let _name = cursor.read_string()?;
        cursor.finish()
    }

    fn handle_origin(&self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);
        let _origin_lsn = cursor.read_u64()?;
        let _name = cursor.read_string()?;
        cursor.finish()
    }

    fn add_dml(
        &mut self,
        rel: RelationInfo,
        operation: ChangeOperation,
        before: Option<TupleImage>,
        after: Option<TupleImage>,
    ) -> Result<(), DecodeError> {
        let transaction = self.transaction.as_mut().ok_or_else(|| {
            DecodeError::InvalidMessage("row message encountered without BEGIN".to_string())
        })?;
        let ordinal = next_event_ordinal(transaction)?;
        transaction.events.push(WalEvent {
            operation,
            relation: RelationKey::new(&rel.namespace, &rel.relation_name, rel.relation_oid),
            event_ordinal: ordinal,
            before,
            after,
        });
        Ok(())
    }

    fn relation(&self, oid: u32) -> Result<&RelationInfo, DecodeError> {
        self.relations.get(&oid).ok_or_else(|| {
            DecodeError::InvalidMessage(format!(
                "unknown relation OID {} for logical row message",
                oid
            ))
        })
    }

    fn read_tuple(
        &self,
        cursor: &mut Cursor<'_>,
        columns: &[ColumnInfo],
    ) -> Result<TupleImage, DecodeError> {
        let ncols = cursor.read_u16()? as usize;
        if ncols != columns.len() {
            return Err(DecodeError::InvalidMessage(format!(
                "tuple contains {} columns but relation has {}",
                ncols,
                columns.len()
            )));
        }
        let mut image = HashMap::with_capacity(ncols);
        for column in columns.iter().take(ncols) {
            let col_type = cursor.read_u8()?;
            let value = match col_type {
                COL_NULL => TupleValue::Null,
                COL_UNCHANGED => TupleValue::Unchanged,
                COL_TEXT => TupleValue::Text(cursor.read_len_bytes()?.to_vec()),
                COL_BINARY => TupleValue::Binary(cursor.read_len_bytes()?.to_vec()),
                _ => {
                    return Err(DecodeError::InvalidMessage(format!(
                        "unknown tuple value tag {:?}",
                        col_type as char
                    )))
                }
            };
            image.insert(column.name.clone(), value);
        }
        Ok(image)
    }

    fn read_key_tuple(
        &self,
        cursor: &mut Cursor<'_>,
        relation: &RelationInfo,
    ) -> Result<TupleImage, DecodeError> {
        let key_columns: Vec<_> = relation
            .columns
            .iter()
            .filter(|column| column.is_key)
            .cloned()
            .collect();
        self.read_tuple(cursor, &key_columns)
    }
}

fn next_event_ordinal(transaction: &mut PendingTransaction) -> Result<u64, DecodeError> {
    let ordinal = transaction.next_ordinal;
    transaction.next_ordinal = transaction
        .next_ordinal
        .checked_add(1)
        .ok_or_else(|| DecodeError::InvalidMessage("event ordinal overflow".to_string()))?;
    Ok(ordinal)
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        if self.remaining() < 1 {
            return Err(DecodeError::UnexpectedEof);
        }
        let value = self.data[self.pos];
        self.pos += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let bytes = self.read_bytes(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, DecodeError> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_i64(&mut self) -> Result<i64, DecodeError> {
        Ok(self.read_u64()? as i64)
    }

    fn read_i32(&mut self) -> Result<i32, DecodeError> {
        Ok(self.read_u32()? as i32)
    }

    fn read_bytes(&mut self, count: usize) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < count {
            return Err(DecodeError::UnexpectedEof);
        }
        let value = &self.data[self.pos..self.pos + count];
        self.pos += count;
        Ok(value)
    }

    fn read_len_bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let length = self.read_u32()? as usize;
        self.read_bytes(length)
    }

    fn read_string(&mut self) -> Result<String, DecodeError> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos == self.data.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let value = std::str::from_utf8(&self.data[start..self.pos])
            .map_err(|_| DecodeError::InvalidMessage("relation string is not UTF-8".to_string()))?
            .to_string();
        self.pos += 1;
        Ok(value)
    }

    fn finish(&self) -> Result<(), DecodeError> {
        if self.remaining() != 0 {
            return Err(DecodeError::InvalidMessage(
                "trailing bytes after logical message".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidMessage(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of WAL data"),
            Self::InvalidMessage(message) => write!(f, "invalid WAL message: {message}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation(oid: u32, namespace: &str, name: &str, identity: u8) -> Vec<u8> {
        let mut value = vec![RELATION_MSG];
        value.extend_from_slice(&oid.to_be_bytes());
        value.extend_from_slice(namespace.as_bytes());
        value.push(0);
        value.extend_from_slice(name.as_bytes());
        value.push(0);
        value.push(identity);
        value.extend_from_slice(&2u16.to_be_bytes());
        for (name, oid) in [("id", 23u32), ("value", 25u32)] {
            value.push(if name == "id" { 1 } else { 0 });
            value.extend_from_slice(name.as_bytes());
            value.push(0);
            value.extend_from_slice(&oid.to_be_bytes());
            value.extend_from_slice(&(-1i32).to_be_bytes());
        }
        value
    }

    fn tuple(values: &[TupleValue]) -> Vec<u8> {
        let mut value = (values.len() as u16).to_be_bytes().to_vec();
        for item in values {
            match item {
                TupleValue::Null => value.push(COL_NULL),
                TupleValue::Unchanged => value.push(COL_UNCHANGED),
                TupleValue::Text(bytes) => {
                    value.push(COL_TEXT);
                    value.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    value.extend_from_slice(bytes);
                }
                TupleValue::Binary(bytes) => {
                    value.push(COL_BINARY);
                    value.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                    value.extend_from_slice(bytes);
                }
            }
        }
        value
    }

    fn begin(xid: u32, final_lsn: u64, timestamp: i64) -> Vec<u8> {
        let mut value = vec![BEGIN_MSG];
        value.extend_from_slice(&final_lsn.to_be_bytes());
        value.extend_from_slice(&timestamp.to_be_bytes());
        value.extend_from_slice(&xid.to_be_bytes());
        value
    }

    fn commit(commit_lsn: u64, end_lsn: u64, timestamp: i64) -> Vec<u8> {
        let mut value = vec![COMMIT_MSG, 0];
        value.extend_from_slice(&commit_lsn.to_be_bytes());
        value.extend_from_slice(&end_lsn.to_be_bytes());
        value.extend_from_slice(&timestamp.to_be_bytes());
        value
    }

    fn dml(
        tag: u8,
        oid: u32,
        before: Option<&[TupleValue]>,
        after: Option<&[TupleValue]>,
    ) -> Vec<u8> {
        let mut value = vec![tag];
        value.extend_from_slice(&oid.to_be_bytes());
        match tag {
            INSERT_MSG => {
                value.push(b'N');
                value.extend(tuple(after.unwrap()));
            }
            DELETE_MSG => {
                value.push(b'K');
                value.extend(tuple(before.unwrap()));
            }
            UPDATE_MSG => {
                if let Some(old) = before {
                    value.push(b'O');
                    value.extend(tuple(old));
                }
                value.push(b'N');
                value.extend(tuple(after.unwrap()));
            }
            _ => unreachable!(),
        }
        value
    }

    fn decoder(_oid: u32, _namespace: &str, _name: &str) -> WalDecoder {
        WalDecoder::new()
    }

    #[test]
    fn emits_complete_transaction_with_begin_and_commit_metadata() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(9, 41, 123)).unwrap();
        assert_eq!(decoder.pending_commit_timestamp(), Some(123));
        decoder
            .feed(&dml(
                INSERT_MSG,
                7,
                None,
                Some(&[
                    TupleValue::Text(b"a".to_vec()),
                    TupleValue::Text(b"v".to_vec()),
                ]),
            ))
            .unwrap();
        let transactions = decoder.feed(&commit(41, 45, 123)).unwrap();
        assert_eq!(decoder.pending_commit_timestamp(), None);
        assert_eq!(transactions.len(), 1);
        let transaction = &transactions[0];
        assert_eq!((transaction.xid, transaction.final_lsn), (9, 41));
        assert_eq!((transaction.commit_lsn, transaction.end_lsn), (41, 45));
        assert_eq!(transaction.events[0].event_ordinal, 0);
    }

    #[test]
    fn preserves_state_across_split_batches_and_two_transactions() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 11, 5)).unwrap();
        decoder
            .feed(&dml(
                INSERT_MSG,
                7,
                None,
                Some(&[TupleValue::Text(b"a".to_vec()), TupleValue::Null]),
            ))
            .unwrap();
        assert!(decoder.feed(&commit(11, 12, 5)).unwrap().len() == 1);
        decoder.feed(&begin(2, 21, 6)).unwrap();
        decoder
            .feed(&dml(
                DELETE_MSG,
                7,
                Some(&[TupleValue::Text(b"b".to_vec())]),
                None,
            ))
            .unwrap();
        let second = decoder.feed(&commit(21, 22, 6)).unwrap();
        assert_eq!(second[0].xid, 2);
        assert_eq!(second[0].events[0].operation, ChangeOperation::Delete);
    }

    #[test]
    fn retains_all_events_with_source_ordinals() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(8, "public", "other", b'd')).unwrap();
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        decoder
            .feed(&dml(
                INSERT_MSG,
                8,
                None,
                Some(&[TupleValue::Text(b"x".to_vec()), TupleValue::Null]),
            ))
            .unwrap();
        decoder
            .feed(&dml(
                INSERT_MSG,
                7,
                None,
                Some(&[TupleValue::Text(b"y".to_vec()), TupleValue::Null]),
            ))
            .unwrap();
        let result = decoder.feed(&commit(2, 3, 1)).unwrap();
        assert_eq!(result[0].events.len(), 2);
        assert_eq!(result[0].events[0].event_ordinal, 0);
        assert_eq!(result[0].events[1].event_ordinal, 1);
    }

    #[test]
    fn same_named_relations_require_exact_identity() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(8, "other", "items", b'd')).unwrap();
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        decoder
            .feed(&dml(
                INSERT_MSG,
                8,
                None,
                Some(&[TupleValue::Text(b"x".to_vec()), TupleValue::Null]),
            ))
            .unwrap();
        let result = decoder.feed(&commit(2, 3, 1)).unwrap();
        assert_eq!(
            result[0].events[0].relation,
            RelationKey::new("other", "items", 8)
        );
    }

    #[test]
    fn full_old_tuple_uses_relation_column_layout() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'f')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        decoder
            .feed(&dml(
                UPDATE_MSG,
                7,
                Some(&[
                    TupleValue::Text(b"a".to_vec()),
                    TupleValue::Binary(vec![0xff]),
                ]),
                Some(&[TupleValue::Text(b"a".to_vec()), TupleValue::Unchanged]),
            ))
            .unwrap();
        let event = &decoder.feed(&commit(2, 3, 1)).unwrap()[0].events[0];
        assert_eq!(
            event.before.as_ref().unwrap()["value"],
            TupleValue::Binary(vec![0xff])
        );
        assert_eq!(
            event.after.as_ref().unwrap()["value"],
            TupleValue::Unchanged
        );
    }

    #[test]
    fn default_identity_delete_key_tuple_maps_only_key_columns() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        decoder
            .feed(&dml(
                DELETE_MSG,
                7,
                Some(&[TupleValue::Text(b"a".to_vec())]),
                None,
            ))
            .unwrap();

        let event = &decoder.feed(&commit(2, 3, 1)).unwrap()[0].events[0];
        let before = event.before.as_ref().unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before["id"], TupleValue::Text(b"a".to_vec()));
        assert!(!before.contains_key("value"));
    }

    #[test]
    fn update_key_tuple_and_new_tuple_use_distinct_layouts() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        let mut update = vec![UPDATE_MSG];
        update.extend_from_slice(&7u32.to_be_bytes());
        update.push(b'K');
        update.extend(tuple(&[TupleValue::Text(b"a".to_vec())]));
        update.push(b'N');
        update.extend(tuple(&[
            TupleValue::Text(b"a".to_vec()),
            TupleValue::Text(b"new".to_vec()),
        ]));
        decoder.feed(&update).unwrap();

        let event = &decoder.feed(&commit(2, 3, 1)).unwrap()[0].events[0];
        let before = event.before.as_ref().unwrap();
        let after = event.after.as_ref().unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before["id"], TupleValue::Text(b"a".to_vec()));
        assert_eq!(after.len(), 2);
        assert_eq!(after["value"], TupleValue::Text(b"new".to_vec()));
    }

    #[test]
    fn rejects_key_tuple_with_non_key_column_width() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();

        let result = decoder.feed(&dml(
            DELETE_MSG,
            7,
            Some(&[
                TupleValue::Text(b"a".to_vec()),
                TupleValue::Text(b"unexpected".to_vec()),
            ]),
            None,
        ));

        assert!(matches!(result, Err(DecodeError::InvalidMessage(_))));
    }

    #[test]
    fn registered_truncate_is_retained_as_transaction_record() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        let mut truncate = vec![TRUNCATE_MSG];
        truncate.extend_from_slice(&1u32.to_be_bytes());
        truncate.push(0);
        truncate.extend_from_slice(&7u32.to_be_bytes());
        decoder.feed(&begin(1, 2, 1)).unwrap();
        decoder.feed(&truncate).unwrap();
        let result = decoder.feed(&commit(2, 3, 1)).unwrap();
        assert_eq!(result[0].truncates[0].relation.oid, 7);
    }

    #[test]
    fn rejects_relation_drift_and_unknown_messages() {
        let mut first = decoder(7, "public", "items");
        first.preload_relations(vec![(
            RelationKey::new("public", "items", 7),
            b'd',
            vec![
                ColumnInfo {
                    name: "id".to_string(),
                    is_key: true,
                    type_oid: 23,
                    type_modifier: -1,
                },
                ColumnInfo {
                    name: "value".to_string(),
                    is_key: false,
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        )]);
        assert!(first.feed(&relation(7, "public", "items", b'f')).is_err());
        let mut fresh = decoder(7, "public", "items");
        assert!(fresh.feed(&[b'S']).is_err());
        assert!(fresh.feed(&[b'Z']).is_err());
    }

    #[test]
    fn rejects_unknown_oid_and_malformed_boundaries() {
        let mut first = decoder(7, "public", "items");
        first.feed(&begin(1, 2, 1)).unwrap();
        let error = first.feed(&dml(
            INSERT_MSG,
            99,
            None,
            Some(&[TupleValue::Text(b"x".to_vec()), TupleValue::Null]),
        ));
        assert!(error.is_err());
        let mut second = decoder(7, "public", "items");
        assert!(second.feed(&commit(1, 2, 1)).is_err());
        let mut third = decoder(7, "public", "items");
        assert!(third.feed(&[]).is_err());
        let mut fourth = decoder(7, "public", "items");
        fourth.feed(&begin(1, 2, 10)).unwrap();
        assert!(fourth.feed(&commit(2, 3, 11)).is_err());
    }

    #[test]
    fn ignores_nontransactional_logical_messages() {
        let mut decoder = decoder(7, "public", "items");
        let mut message = vec![LOGICAL_MSG, 0];
        message.extend_from_slice(&1u64.to_be_bytes());
        message.extend_from_slice(b"foreign\0");
        message.extend_from_slice(&3u32.to_be_bytes());
        message.extend_from_slice(b"abc");
        assert!(decoder.feed(&message).unwrap().is_empty());

        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        let transaction = decoder.feed(&commit(2, 3, 1)).unwrap();
        assert_eq!(transaction.len(), 1);
    }
}
