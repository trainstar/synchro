use std::collections::HashMap;
use synchro_core::protocol::Operation;

/// A decoded WAL event relevant to sync.
#[derive(Debug, Clone)]
pub struct WalEvent {
    pub table_name: String,
    pub record_id: String,
    pub operation: Operation,
    pub data: HashMap<String, Option<String>>,
}

/// Cached relation metadata from pgoutput RelationMessages.
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub relation_id: u32,
    pub relation_name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type_oid: u32,
}

/// pgoutput message types (first byte).
pub const RELATION_MSG: u8 = b'R';
pub const INSERT_MSG: u8 = b'I';
pub const UPDATE_MSG: u8 = b'U';
pub const DELETE_MSG: u8 = b'D';
pub const BEGIN_MSG: u8 = b'B';
pub const COMMIT_MSG: u8 = b'C';
pub const TYPE_MSG: u8 = b'Y';
pub const ORIGIN_MSG: u8 = b'O';
pub const TRUNCATE_MSG: u8 = b'T';

/// Column data types within a tuple.
pub const COL_NULL: u8 = b'n';
pub const COL_TEXT: u8 = b't';
pub const COL_UNCHANGED: u8 = b'u';

/// Decodes pgoutput protocol messages into WalEvents.
///
/// Maintains a cache of RelationMessages to resolve RelationIDs.
/// The background worker provides the table registry for filtering.
pub struct WalDecoder {
    /// Cached relation info by relation ID.
    relations: HashMap<u32, RelationInfo>,
    /// Set of registered table names (for filtering unregistered tables).
    registered_tables: HashMap<String, TableMeta>,
}

/// Minimal table metadata needed by the decoder.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub pk_column: String,
    pub deleted_at_col: String,
    pub has_deleted_at: bool,
}

/// Extract the primary key value from decoded tuple data.
/// Returns a DecodeError if the PK column is missing or NULL.
fn extract_record_id(
    tuple_data: &HashMap<String, Option<String>>,
    pk_column: &str,
    table_name: &str,
) -> Result<String, DecodeError> {
    match tuple_data.get(pk_column) {
        Some(Some(id)) if !id.is_empty() => Ok(id.clone()),
        Some(Some(_)) => Err(DecodeError::InvalidMessage(format!(
            "empty primary key column '{}' in table '{}'",
            pk_column, table_name
        ))),
        Some(None) => Err(DecodeError::InvalidMessage(format!(
            "NULL primary key column '{}' in table '{}'",
            pk_column, table_name
        ))),
        None => Err(DecodeError::InvalidMessage(format!(
            "missing primary key column '{}' in table '{}' (check REPLICA IDENTITY)",
            pk_column, table_name
        ))),
    }
}

impl WalDecoder {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            registered_tables: HashMap::new(),
        }
    }

    /// Update the set of registered tables (called when registry changes).
    pub fn set_registered_tables(&mut self, tables: HashMap<String, TableMeta>) {
        self.registered_tables = tables;
    }

    /// Decode a pgoutput message. Returns events (0 or 1 typically).
    ///
    /// Mirrors Go `Decoder.Decode`.
    pub fn decode(&mut self, wal_data: &[u8]) -> Result<Vec<WalEvent>, DecodeError> {
        if wal_data.is_empty() {
            return Ok(vec![]);
        }

        match wal_data[0] {
            RELATION_MSG => {
                self.handle_relation(&wal_data[1..])?;
                Ok(vec![])
            }
            INSERT_MSG => self.handle_insert(&wal_data[1..]),
            UPDATE_MSG => self.handle_update(&wal_data[1..]),
            DELETE_MSG => self.handle_delete(&wal_data[1..]),
            BEGIN_MSG | COMMIT_MSG | TYPE_MSG | ORIGIN_MSG | TRUNCATE_MSG => Ok(vec![]),
            _ => Ok(vec![]),
        }
    }

    /// Parse a RelationMessage and cache it.
    fn handle_relation(&mut self, data: &[u8]) -> Result<(), DecodeError> {
        let mut cursor = Cursor::new(data);

        let relation_id = cursor.read_u32()?;
        let _namespace = cursor.read_string()?;
        let relation_name = cursor.read_string()?;
        let _replica_identity = cursor.read_u8()?;
        let ncols = cursor.read_u16()?;

        let mut columns = Vec::with_capacity(ncols as usize);
        for _ in 0..ncols {
            let _flags = cursor.read_u8()?;
            let name = cursor.read_string()?;
            let data_type_oid = cursor.read_u32()?;
            let _type_modifier = cursor.read_u32()?;
            columns.push(ColumnInfo {
                name,
                data_type_oid,
            });
        }

        self.relations.insert(
            relation_id,
            RelationInfo {
                relation_id,
                relation_name,
                columns,
            },
        );

        Ok(())
    }

    fn handle_insert(&self, data: &[u8]) -> Result<Vec<WalEvent>, DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32()?;
        let _tuple_type = cursor.read_u8()?; // 'N' for new tuple

        let rel = match self.relations.get(&relation_id) {
            Some(r) => r,
            None => return Ok(vec![]),
        };

        let meta = match self.registered_tables.get(&rel.relation_name) {
            Some(m) => m,
            None => return Ok(vec![]),
        };

        let tuple_data = self.read_tuple(&mut cursor, &rel.columns)?;
        let record_id = extract_record_id(&tuple_data, &meta.pk_column, &rel.relation_name)?;

        Ok(vec![WalEvent {
            table_name: rel.relation_name.clone(),
            record_id,
            operation: Operation::Insert,
            data: tuple_data,
        }])
    }

    fn handle_update(&self, data: &[u8]) -> Result<Vec<WalEvent>, DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32()?;

        let rel = match self.relations.get(&relation_id) {
            Some(r) => r,
            None => return Ok(vec![]),
        };

        let meta = match self.registered_tables.get(&rel.relation_name) {
            Some(m) => m,
            None => return Ok(vec![]),
        };

        // Update may have optional old tuple (key type K or O), then new tuple (N).
        let mut tuple_type = cursor.read_u8()?;
        if tuple_type == b'K' || tuple_type == b'O' {
            // Skip old tuple.
            self.skip_tuple(&mut cursor, &rel.columns)?;
            tuple_type = cursor.read_u8()?;
        }

        if tuple_type != b'N' {
            return Ok(vec![]);
        }

        let tuple_data = self.read_tuple(&mut cursor, &rel.columns)?;
        let record_id = extract_record_id(&tuple_data, &meta.pk_column, &rel.relation_name)?;

        // Detect soft deletes: if deleted_at column exists and is non-null, emit Delete.
        let mut op = Operation::Update;
        if meta.has_deleted_at {
            if let Some(Some(_)) = tuple_data.get(&meta.deleted_at_col) {
                op = Operation::Delete;
            }
        }

        Ok(vec![WalEvent {
            table_name: rel.relation_name.clone(),
            record_id,
            operation: op,
            data: tuple_data,
        }])
    }

    fn handle_delete(&self, data: &[u8]) -> Result<Vec<WalEvent>, DecodeError> {
        let mut cursor = Cursor::new(data);
        let relation_id = cursor.read_u32()?;

        let rel = match self.relations.get(&relation_id) {
            Some(r) => r,
            None => return Ok(vec![]),
        };

        let meta = match self.registered_tables.get(&rel.relation_name) {
            Some(m) => m,
            None => return Ok(vec![]),
        };

        let tuple_type = cursor.read_u8()?;
        if tuple_type != b'K' && tuple_type != b'O' {
            return Ok(vec![]);
        }

        let tuple_data = self.read_tuple(&mut cursor, &rel.columns)?;
        let record_id = extract_record_id(&tuple_data, &meta.pk_column, &rel.relation_name)?;

        Ok(vec![WalEvent {
            table_name: rel.relation_name.clone(),
            record_id,
            operation: Operation::Delete,
            data: tuple_data,
        }])
    }

    /// Read a tuple from the pgoutput stream.
    fn read_tuple(
        &self,
        cursor: &mut Cursor,
        columns: &[ColumnInfo],
    ) -> Result<HashMap<String, Option<String>>, DecodeError> {
        let ncols = cursor.read_u16()? as usize;
        let mut data = HashMap::with_capacity(ncols);

        for i in 0..ncols {
            if i >= columns.len() {
                break;
            }
            let col_type = cursor.read_u8()?;
            match col_type {
                COL_NULL => {
                    data.insert(columns[i].name.clone(), None);
                }
                COL_TEXT => {
                    let len = cursor.read_u32()? as usize;
                    let val = cursor.read_bytes(len)?;
                    let s = String::from_utf8_lossy(val).to_string();
                    data.insert(columns[i].name.clone(), Some(s));
                }
                COL_UNCHANGED => {
                    // Skip: unchanged toast column, not available.
                }
                _ => {
                    // Unknown column type, skip.
                }
            }
        }

        Ok(data)
    }

    /// Skip a tuple without storing data.
    fn skip_tuple(
        &self,
        cursor: &mut Cursor,
        _columns: &[ColumnInfo],
    ) -> Result<(), DecodeError> {
        let ncols = cursor.read_u16()? as usize;
        for _ in 0..ncols {
            let col_type = cursor.read_u8()?;
            match col_type {
                COL_NULL | COL_UNCHANGED => {}
                COL_TEXT => {
                    let len = cursor.read_u32()? as usize;
                    cursor.advance(len)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Cursor helper for reading binary pgoutput data
// ---------------------------------------------------------------------------

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
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u16(&mut self) -> Result<u16, DecodeError> {
        if self.remaining() < 2 {
            return Err(DecodeError::UnexpectedEof);
        }
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u32(&mut self) -> Result<u32, DecodeError> {
        if self.remaining() < 4 {
            return Err(DecodeError::UnexpectedEof);
        }
        let v = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::UnexpectedEof);
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn read_string(&mut self) -> Result<String, DecodeError> {
        // Null-terminated string.
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != 0 {
            self.pos += 1;
        }
        if self.pos >= self.data.len() {
            return Err(DecodeError::UnexpectedEof);
        }
        let s = String::from_utf8_lossy(&self.data[start..self.pos]).to_string();
        self.pos += 1; // skip null terminator
        Ok(s)
    }

    fn advance(&mut self, n: usize) -> Result<(), DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::UnexpectedEof);
        }
        self.pos += n;
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
            Self::InvalidMessage(msg) => write!(f, "invalid WAL message: {msg}"),
        }
    }
}

impl std::error::Error for DecodeError {}
