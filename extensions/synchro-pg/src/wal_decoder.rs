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

pub(crate) const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;
const MAX_TRANSACTION_RECORDS: usize = 10_000;

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

/// Relation layout from a pgoutput Relation message that row decoding uses.
#[derive(Clone)]
pub struct RelationInfo {
    pub relation_name: String,
    pub namespace: String,
    pub relation_oid: u32,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub is_key: bool,
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

#[derive(Clone)]
struct PendingTransaction {
    xid: u32,
    final_lsn: u64,
    commit_timestamp: i64,
    next_ordinal: u64,
    buffered_bytes: usize,
    buffered_records: usize,
    events: Vec<WalEvent>,
    truncates: Vec<WalTruncate>,
    messages: Vec<WalLogicalMessage>,
}

/// Stateful strict decoder for pgoutput protocol version 1.
#[derive(Clone)]
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
    pub fn preload_relations(&mut self, relations: Vec<(RelationKey, Vec<ColumnInfo>)>) {
        for (key, columns) in relations {
            self.relations.insert(
                key.oid,
                RelationInfo {
                    relation_name: key.name,
                    namespace: key.namespace,
                    relation_oid: key.oid,
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

    pub(crate) fn pending_failure_context(&self) -> Option<(u64, i64)> {
        self.transaction
            .as_ref()
            .map(|transaction| (transaction.final_lsn, transaction.commit_timestamp))
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
        let result = self
            .charge_pending_transaction(wal_data)
            .and_then(|()| self.feed_inner(wal_data));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn charge_pending_transaction(&mut self, wal_data: &[u8]) -> Result<(), DecodeError> {
        if wal_data.first() == Some(&COMMIT_MSG) {
            return Ok(());
        }
        let Some(transaction) = self.transaction.as_mut() else {
            return Ok(());
        };
        let buffered_bytes = transaction
            .buffered_bytes
            .checked_add(wal_data.len())
            .ok_or(DecodeError::TransactionTooLarge {
                max_bytes: MAX_TRANSACTION_BYTES,
                max_records: MAX_TRANSACTION_RECORDS,
            })?;
        let retained_records = match wal_data {
            [TRUNCATE_MSG, count @ ..] if count.len() >= 4 => {
                u32::from_be_bytes(count[..4].try_into().unwrap_or([0; 4])) as usize
            }
            _ => 1,
        };
        let buffered_records = transaction
            .buffered_records
            .checked_add(retained_records)
            .ok_or(DecodeError::TransactionTooLarge {
                max_bytes: MAX_TRANSACTION_BYTES,
                max_records: MAX_TRANSACTION_RECORDS,
            })?;
        if buffered_bytes > MAX_TRANSACTION_BYTES || buffered_records > MAX_TRANSACTION_RECORDS {
            return Err(DecodeError::TransactionTooLarge {
                max_bytes: MAX_TRANSACTION_BYTES,
                max_records: MAX_TRANSACTION_RECORDS,
            });
        }
        transaction.buffered_bytes = buffered_bytes;
        transaction.buffered_records = buffered_records;
        Ok(())
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
            buffered_bytes: data.len() + 1,
            buffered_records: 0,
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
        // These are the pg_class.relreplident values that pgoutput can send.
        // Key tuples are filtered by column key flags, so the byte is not kept.
        if !matches!(cursor.read_u8()?, b'd' | b'n' | b'f' | b'i') {
            return Err(DecodeError::InvalidMessage(
                "RELATION contains an unsupported replica identity".to_string(),
            ));
        }
        let ncols = cursor.read_u16()? as usize;
        let mut columns = Vec::with_capacity(ncols);
        for _ in 0..ncols {
            let flags = cursor.read_u8()?;
            let name = cursor.read_string()?;
            // Tuple values stay in their text or binary wire form, so the type OID
            // and type modifier are consumed but not kept.
            cursor.read_u32()?;
            cursor.read_i32()?;
            if flags & !1 != 0 {
                return Err(DecodeError::InvalidMessage(
                    "RELATION column contains unsupported flags".to_string(),
                ));
            }
            columns.push(ColumnInfo {
                name,
                is_key: flags & 1 == 1,
            });
        }
        cursor.finish()?;

        // pgoutput resends Relation metadata after a definition change. The new
        // entry applies to later rows. Buffered rows keep their decoded columns.
        self.relations.insert(
            relation_oid,
            RelationInfo {
                relation_name,
                namespace,
                relation_oid,
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
        // PostgreSQL encodes K tuples with every published column.
        let mut image = self.read_tuple(cursor, &relation.columns)?;
        image.retain(|name, _| {
            relation
                .columns
                .iter()
                .any(|column| column.is_key && column.name == *name)
        });
        Ok(image)
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
    TransactionTooLarge {
        max_bytes: usize,
        max_records: usize,
    },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of WAL data"),
            Self::InvalidMessage(message) => write!(f, "invalid WAL message: {message}"),
            Self::TransactionTooLarge {
                max_bytes,
                max_records,
            } => write!(
                f,
                "WAL transaction exceeds the {max_bytes}-byte or {max_records}-record decode limit"
            ),
        }
    }
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn relation(oid: u32, namespace: &str, name: &str, identity: u8) -> Vec<u8> {
        relation_with_columns(
            oid,
            namespace,
            name,
            identity,
            &[("id", 23, true), ("value", 25, false)],
        )
    }

    fn relation_with_columns(
        oid: u32,
        namespace: &str,
        name: &str,
        identity: u8,
        columns: &[(&str, u32, bool)],
    ) -> Vec<u8> {
        let mut value = vec![RELATION_MSG];
        value.extend_from_slice(&oid.to_be_bytes());
        value.extend_from_slice(namespace.as_bytes());
        value.push(0);
        value.extend_from_slice(name.as_bytes());
        value.push(0);
        value.push(identity);
        value.extend_from_slice(&(columns.len() as u16).to_be_bytes());
        for (name, type_oid, is_key) in columns {
            value.push(u8::from(*is_key));
            value.extend_from_slice(name.as_bytes());
            value.push(0);
            value.extend_from_slice(&type_oid.to_be_bytes());
            value.extend_from_slice(&(-1i32).to_be_bytes());
        }
        value
    }

    fn image(values: &[(&str, TupleValue)]) -> TupleImage {
        values
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect()
    }

    fn logical_message(transactional: bool, lsn: u64, prefix: &str, content: &[u8]) -> Vec<u8> {
        let mut value = vec![LOGICAL_MSG, u8::from(transactional)];
        value.extend_from_slice(&lsn.to_be_bytes());
        value.extend_from_slice(prefix.as_bytes());
        value.push(0);
        value.extend_from_slice(&(content.len() as u32).to_be_bytes());
        value.extend_from_slice(content);
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
                Some(&[
                    TupleValue::Text(b"b".to_vec()),
                    TupleValue::Text(b"discarded".to_vec()),
                ]),
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
    fn rejects_transaction_larger_than_decode_limit() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        let message = dml(
            INSERT_MSG,
            7,
            None,
            Some(&[
                TupleValue::Text(b"a".to_vec()),
                TupleValue::Text(vec![b'x'; 1024 * 1024]),
            ]),
        );
        let accepted = MAX_TRANSACTION_BYTES / message.len();
        for _ in 0..accepted {
            decoder.feed(&message).unwrap();
        }

        assert!(accepted * message.len() <= MAX_TRANSACTION_BYTES);
        assert!((accepted + 1) * message.len() > MAX_TRANSACTION_BYTES);
        assert!(matches!(
            decoder.feed(&message),
            Err(DecodeError::TransactionTooLarge {
                max_bytes: MAX_TRANSACTION_BYTES,
                max_records: MAX_TRANSACTION_RECORDS,
            })
        ));
        assert!(decoder.feed(&commit(2, 3, 1)).is_err());
    }

    #[test]
    fn rejects_transaction_with_too_many_records() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        let message = dml(
            INSERT_MSG,
            7,
            None,
            Some(&[TupleValue::Text(b"a".to_vec()), TupleValue::Null]),
        );
        for _ in 0..MAX_TRANSACTION_RECORDS {
            decoder.feed(&message).unwrap();
        }

        assert!(matches!(
            decoder.feed(&message),
            Err(DecodeError::TransactionTooLarge {
                max_bytes: MAX_TRANSACTION_BYTES,
                max_records: MAX_TRANSACTION_RECORDS,
            })
        ));
        assert!(decoder.feed(&commit(2, 3, 1)).is_err());
    }

    #[test]
    fn rejects_large_truncate_before_relation_allocation() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&begin(1, 2, 1)).unwrap();
        let mut truncate = vec![TRUNCATE_MSG];
        truncate.extend_from_slice(&((MAX_TRANSACTION_RECORDS + 1) as u32).to_be_bytes());
        truncate.push(0);
        truncate.resize(6 + (MAX_TRANSACTION_RECORDS + 1) * 4, 0);

        assert!(matches!(
            decoder.feed(&truncate),
            Err(DecodeError::TransactionTooLarge {
                max_bytes: MAX_TRANSACTION_BYTES,
                max_records: MAX_TRANSACTION_RECORDS,
            })
        ));
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
                Some(&[
                    TupleValue::Text(b"a".to_vec()),
                    TupleValue::Text(b"discarded".to_vec()),
                ]),
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
        update.extend(tuple(&[
            TupleValue::Text(b"a".to_vec()),
            TupleValue::Text(b"old".to_vec()),
        ]));
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
    fn rejects_key_tuple_with_incomplete_column_width() {
        let mut decoder = decoder(7, "public", "items");
        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();

        let result = decoder.feed(&dml(
            DELETE_MSG,
            7,
            Some(&[TupleValue::Text(b"a".to_vec())]),
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
    fn replaces_relation_metadata_without_rewriting_buffered_records() {
        let text = |value: &str| TupleValue::Text(value.as_bytes().to_vec());
        let items = RelationKey::new("public", "items", 7);
        let mut decoder = decoder(7, "public", "items");
        decoder
            .feed(&relation_with_columns(
                7,
                "public",
                "items",
                b'd',
                &[("id", 23, true), ("value", 25, false)],
            ))
            .unwrap();
        decoder.feed(&begin(1, 11, 5)).unwrap();
        decoder
            .feed(&dml(INSERT_MSG, 7, None, Some(&[text("1"), text("first")])))
            .unwrap();
        let first = decoder.feed(&commit(11, 12, 5)).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(
            (
                first[0].xid,
                first[0].final_lsn,
                first[0].commit_lsn,
                first[0].end_lsn,
                first[0].commit_timestamp,
            ),
            (1, 11, 11, 12, 5)
        );
        assert_eq!(first[0].events.len(), 1);
        assert_eq!(
            first[0].events[0].after,
            Some(image(&[("id", text("1")), ("value", text("first"))]))
        );

        // The same decoder buffers old-layout rows and a transactional message
        // before the replacement Relation message arrives. The replacement adds
        // a column and moves the replica identity key from id to value.
        decoder.feed(&begin(2, 21, 6)).unwrap();
        decoder
            .feed(&dml(
                INSERT_MSG,
                7,
                None,
                Some(&[text("2"), text("before")]),
            ))
            .unwrap();
        decoder
            .feed(&dml(
                DELETE_MSG,
                7,
                Some(&[text("1"), TupleValue::Null]),
                None,
            ))
            .unwrap();
        decoder
            .feed(&logical_message(true, 15, "synchro", b"before-refresh"))
            .unwrap();
        decoder
            .feed(&relation_with_columns(
                7,
                "public",
                "items",
                b'i',
                &[
                    ("id", 23, false),
                    ("value", 25, true),
                    ("added_value", 25, false),
                ],
            ))
            .unwrap();
        decoder
            .feed(&dml(
                UPDATE_MSG,
                7,
                None,
                Some(&[text("2"), text("before"), text("wp05-nonnull")]),
            ))
            .unwrap();
        decoder
            .feed(&logical_message(true, 19, "synchro", b"after-refresh"))
            .unwrap();
        decoder
            .feed(&dml(
                DELETE_MSG,
                7,
                Some(&[TupleValue::Null, text("before"), TupleValue::Null]),
                None,
            ))
            .unwrap();
        let second = decoder.feed(&commit(21, 22, 6)).unwrap();

        assert_eq!(second.len(), 1);
        let transaction = &second[0];
        assert_eq!(
            (
                transaction.xid,
                transaction.final_lsn,
                transaction.commit_lsn,
                transaction.end_lsn,
                transaction.commit_timestamp,
            ),
            (2, 21, 21, 22, 6)
        );
        assert!(transaction.truncates.is_empty());
        assert_eq!(
            transaction.messages,
            vec![
                WalLogicalMessage {
                    prefix: "synchro".to_string(),
                    content: b"before-refresh".to_vec(),
                    message_lsn: 15,
                },
                WalLogicalMessage {
                    prefix: "synchro".to_string(),
                    content: b"after-refresh".to_vec(),
                    message_lsn: 19,
                },
            ]
        );
        let expected = [
            (
                ChangeOperation::Insert,
                None,
                Some(image(&[("id", text("2")), ("value", text("before"))])),
            ),
            (
                ChangeOperation::Delete,
                Some(image(&[("id", text("1"))])),
                None,
            ),
            (
                ChangeOperation::Update,
                None,
                Some(image(&[
                    ("id", text("2")),
                    ("value", text("before")),
                    ("added_value", text("wp05-nonnull")),
                ])),
            ),
            (
                ChangeOperation::Delete,
                Some(image(&[("value", text("before"))])),
                None,
            ),
        ];
        assert_eq!(transaction.events.len(), expected.len());
        for (ordinal, (event, (operation, before, after))) in
            transaction.events.iter().zip(expected).enumerate()
        {
            assert_eq!(event.operation, operation);
            assert_eq!(event.event_ordinal, ordinal as u64);
            assert_eq!(event.relation, items);
            assert_eq!(event.before, before);
            assert_eq!(event.after, after);
        }
    }

    #[test]
    fn malformed_relation_replacement_poisons_decoder() {
        let mut truncated = relation_with_columns(
            7,
            "public",
            "items",
            b'd',
            &[
                ("id", 23, true),
                ("value", 25, false),
                ("added_value", 25, false),
            ],
        );
        truncated.pop();
        let undefined_identity = relation(7, "public", "items", b'x');
        let row = dml(
            INSERT_MSG,
            7,
            None,
            Some(&[TupleValue::Text(b"1".to_vec()), TupleValue::Null]),
        );
        for replacement in [truncated, undefined_identity] {
            let mut decoder = decoder(7, "public", "items");
            decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
            decoder.feed(&begin(1, 2, 1)).unwrap();
            decoder.feed(&row).unwrap();
            assert!(decoder.feed(&replacement).is_err());
            // The old entry must not keep decoding after a rejected replacement.
            assert!(decoder.feed(&row).is_err());
            assert!(decoder.feed(&commit(2, 3, 1)).is_err());
        }
    }

    #[test]
    fn refreshes_each_replica_identity_and_rejects_unknown_messages() {
        let text = |value: &str| TupleValue::Text(value.as_bytes().to_vec());
        let mut first = decoder(7, "public", "items");
        first.preload_relations(vec![(
            RelationKey::new("public", "items", 7),
            vec![
                ColumnInfo {
                    name: "id".to_string(),
                    is_key: true,
                },
                ColumnInfo {
                    name: "value".to_string(),
                    is_key: false,
                },
            ],
        )]);
        let refresh = |identity: u8, id_key: bool, value_key: bool| {
            relation_with_columns(
                7,
                "public",
                "items",
                identity,
                &[("id", 23, id_key), ("value", 25, value_key)],
            )
        };
        first.feed(&begin(1, 30, 7)).unwrap();
        // FULL flags every column and sends the complete old tuple.
        first.feed(&refresh(b'f', true, true)).unwrap();
        first
            .feed(&dml(
                UPDATE_MSG,
                7,
                Some(&[text("1"), text("old")]),
                Some(&[text("1"), text("new")]),
            ))
            .unwrap();
        // NOTHING flags no column, so only an insert is published.
        first.feed(&refresh(b'n', false, false)).unwrap();
        first
            .feed(&dml(
                INSERT_MSG,
                7,
                None,
                Some(&[text("2"), text("inserted")]),
            ))
            .unwrap();
        // INDEX flags the index column, and DEFAULT flags the primary key.
        first.feed(&refresh(b'i', false, true)).unwrap();
        first
            .feed(&dml(
                DELETE_MSG,
                7,
                Some(&[TupleValue::Null, text("inserted")]),
                None,
            ))
            .unwrap();
        first.feed(&refresh(b'd', true, false)).unwrap();
        first
            .feed(&dml(
                DELETE_MSG,
                7,
                Some(&[text("1"), TupleValue::Null]),
                None,
            ))
            .unwrap();
        let transactions = first.feed(&commit(30, 31, 7)).unwrap();
        assert_eq!(transactions.len(), 1);
        let expected = [
            (
                ChangeOperation::Update,
                Some(image(&[("id", text("1")), ("value", text("old"))])),
                Some(image(&[("id", text("1")), ("value", text("new"))])),
            ),
            (
                ChangeOperation::Insert,
                None,
                Some(image(&[("id", text("2")), ("value", text("inserted"))])),
            ),
            (
                ChangeOperation::Delete,
                Some(image(&[("value", text("inserted"))])),
                None,
            ),
            (
                ChangeOperation::Delete,
                Some(image(&[("id", text("1"))])),
                None,
            ),
        ];
        assert_eq!(transactions[0].events.len(), expected.len());
        for (ordinal, (event, (operation, before, after))) in
            transactions[0].events.iter().zip(expected).enumerate()
        {
            assert_eq!(event.operation, operation);
            assert_eq!(event.event_ordinal, ordinal as u64);
            assert_eq!(event.before, before);
            assert_eq!(event.after, after);
        }
        for tag in [b'S', b'Z'] {
            let mut fresh = decoder(7, "public", "items");
            assert!(fresh.feed(&[tag]).is_err());
        }
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
        let message = logical_message(false, 1, "foreign", b"abc");
        assert!(decoder.feed(&message).unwrap().is_empty());

        decoder.feed(&relation(7, "public", "items", b'd')).unwrap();
        decoder.feed(&begin(1, 2, 1)).unwrap();
        let transaction = decoder.feed(&commit(2, 3, 1)).unwrap();
        assert_eq!(transaction.len(), 1);
    }
}
