//! Capture fence of each registered row write.
//!
//! In each row identity, the capture fences fire in the order of the writes.
//! A fence fails the source transaction with SQLSTATE 27000 when a later write
//! of its row identity already fired its fence.

use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::Infallible;
use std::ffi::{c_void, CStr, CString};

use pgrx::itemptr::item_pointer_get_both;
use pgrx::prelude::*;

#[pg_trigger]
fn synchro_capture_fence<'a>(
    trigger: &'a PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, AllocatedByPostgres>>, Infallible> {
    let operation = row_operation(trigger);
    let data = trigger.trigger_data();
    require_heap_table(data.tg_relation);
    let arguments = fence_arguments(trigger);
    let key_column = registered_key_column(&arguments[3]);
    let key_attnum = immediate_key_attnum(data.tg_relation, &key_column);
    let write = match operation {
        RowOperation::Insert => insert_write(data, &arguments[0], key_attnum),
        RowOperation::Update => update_write(data, key_attnum),
        RowOperation::Delete => delete_write(data, &arguments[0], key_attnum),
    };
    check_write_order(&write.check);
    call_record_body(data);
    check_write_order(&write.check);
    if let Some(entry) = write.record {
        record_write_order(entry);
    }
    Ok(None)
}

type TupleKey = (
    pg_sys::Oid,
    pg_sys::BlockNumber,
    pg_sys::OffsetNumber,
    pg_sys::TransactionId,
    pg_sys::CommandId,
);

#[derive(Default)]
struct OrderState {
    superseded: HashMap<TupleKey, pg_sys::TransactionId>,
    inserts: HashMap<Vec<u8>, Vec<(pg_sys::CommandId, pg_sys::TransactionId)>>,
}

thread_local! {
    static ORDER_STATE: RefCell<Option<OrderState>> = const { RefCell::new(None) };
}

enum RowOperation {
    Insert,
    Update,
    Delete,
}

enum OrderCheck {
    Superseded(TupleKey),
    LaterInsert(Vec<u8>, pg_sys::CommandId),
}

enum OrderEntry {
    Superseded(TupleKey, pg_sys::TransactionId),
    Insert(Vec<u8>, pg_sys::CommandId, pg_sys::TransactionId),
}

struct Write {
    check: OrderCheck,
    record: Option<OrderEntry>,
}

struct TupleHeader {
    tid: pg_sys::ItemPointerData,
    header: pg_sys::HeapTupleHeaderData,
}

fn row_operation(trigger: &PgTrigger<'_>) -> RowOperation {
    let data = trigger.trigger_data();
    let operation = match (trigger.when(), trigger.level(), trigger.op()) {
        (Ok(PgTriggerWhen::After), PgTriggerLevel::Row, Ok(PgTriggerOperation::Insert)) => {
            Some(RowOperation::Insert)
        }
        (Ok(PgTriggerWhen::After), PgTriggerLevel::Row, Ok(PgTriggerOperation::Update)) => {
            Some(RowOperation::Update)
        }
        (Ok(PgTriggerWhen::After), PgTriggerLevel::Row, Ok(PgTriggerOperation::Delete)) => {
            Some(RowOperation::Delete)
        }
        _ => None,
    };
    let tuples_present = !data.tg_trigtuple.is_null()
        && (!matches!(operation, Some(RowOperation::Update)) || !data.tg_newtuple.is_null());
    match operation {
        Some(operation) if tuples_present => operation,
        _ => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED,
                "capture fence must run as an AFTER ROW INSERT, UPDATE, or DELETE trigger"
            );
        }
    }
}

fn require_heap_table(relation: pg_sys::Relation) {
    // SAFETY: PostgreSQL gives a valid open relation to each trigger call.
    let (kind, access_method) =
        unsafe { ((*(*relation).rd_rel).relkind, (*(*relation).rd_rel).relam) };
    if kind as u8 != pg_sys::RELKIND_RELATION || access_method != pg_sys::HEAP_TABLE_AM_OID {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "capture fence requires a heap table"
        );
    }
}

fn fence_arguments(trigger: &PgTrigger<'_>) -> Vec<String> {
    match trigger.extra_args() {
        Ok(arguments) if trigger.trigger().tgnargs == 5 => arguments,
        _ => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                "capture fence trigger arguments are invalid"
            );
        }
    }
}

fn registered_key_column(metadata: &str) -> String {
    match serde_json::from_str::<Vec<String>>(metadata).as_deref() {
        Ok([column]) if !column.is_empty() => column.clone(),
        _ => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                "capture fence key metadata is invalid"
            );
        }
    }
}

fn immediate_key_attnum(relation: pg_sys::Relation, key_column: &str) -> i16 {
    let attnum = match CString::new(key_column) {
        // SAFETY: relation is valid, and name is a NUL-terminated column name.
        Ok(name) => unsafe { pg_sys::get_attnum((*relation).rd_id, name.as_ptr()) },
        Err(_) => 0,
    };
    // SAFETY: relation is valid for the duration of the trigger call.
    let index = unsafe { pg_sys::RelationGetPrimaryKeyIndex(relation, false) };
    if attnum > 0 && index != pg_sys::InvalidOid {
        // SAFETY: The INDEXRELID cache takes one index OID key.
        let entry = unsafe {
            pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier::INDEXRELID as i32, index.into())
        };
        if !entry.is_null() {
            // SAFETY: An INDEXRELID cache entry holds a pg_index row with at least one key column.
            let immediate = unsafe {
                let index_row = pg_sys::GETSTRUCT(entry).cast::<pg_sys::FormData_pg_index>();
                (*index_row).indnkeyatts == 1 && (*index_row).indkey.values.as_slice(1)[0] == attnum
            };
            // SAFETY: entry came from SearchSysCache1 and this code releases it once.
            unsafe { pg_sys::ReleaseSysCache(entry) };
            if immediate {
                return attnum;
            }
        }
    }
    ereport!(
        ERROR,
        PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
        "capture fence requires one immediate primary key on the registered key column"
    );
}

fn insert_write(data: &pg_sys::TriggerData, relation_name: &str, key_attnum: i16) -> Write {
    let identity = row_identity(
        relation_name,
        registered_key_text(data, data.tg_trigtuple, key_attnum),
    );
    let output = read_tuple_header(data.tg_relation, data.tg_trigtuple);
    let output_key = output_tuple_key(data.tg_relation, &output);
    let (_, _, _, xmin, cmin) = output_key;
    Write {
        check: OrderCheck::Superseded(output_key),
        record: Some(OrderEntry::Insert(identity, cmin, xmin)),
    }
}

fn update_write(data: &pg_sys::TriggerData, key_attnum: i16) -> Write {
    let old_key = registered_key_text(data, data.tg_trigtuple, key_attnum);
    let new_key = registered_key_text(data, data.tg_newtuple, key_attnum);
    if old_key != new_key {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_CHECK_VIOLATION,
            "registered primary key cannot change"
        );
    }
    let old = read_tuple_header(data.tg_relation, data.tg_trigtuple);
    let output = read_tuple_header(data.tg_relation, data.tg_newtuple);
    let output_key = output_tuple_key(data.tg_relation, &output);
    Write {
        check: OrderCheck::Superseded(output_key),
        record: superseded_entry(
            data.tg_relation,
            &old,
            heap_tuple_header_get_raw_xmin(&output.header),
        ),
    }
}

fn delete_write(data: &pg_sys::TriggerData, relation_name: &str, key_attnum: i16) -> Write {
    let identity = row_identity(
        relation_name,
        registered_key_text(data, data.tg_trigtuple, key_attnum),
    );
    let old = read_tuple_header(data.tg_relation, data.tg_trigtuple);
    let (next_block, next_offset) = item_pointer_get_both(old.header.t_ctid);
    if next_block == pg_sys::InvalidBlockNumber
        && u32::from(next_offset) == pg_sys::MovedPartitionsOffsetNumber
    {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_CHECK_VIOLATION,
            "registered primary key cannot change"
        );
    }
    let update_xid = heap_tuple_header_get_update_xid(&old.header);
    // SAFETY: TransactionIdIsCurrentTransactionId reads only transaction state.
    if !unsafe { pg_sys::TransactionIdIsCurrentTransactionId(update_xid) } {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "capture fence tuple is not from the current transaction"
        );
    }
    // SAFETY: The update xid of the header copy is current, as HeapTupleHeaderGetCmax requires.
    let cmax = unsafe { pg_sys::HeapTupleHeaderGetCmax(&old.header) };
    Write {
        check: OrderCheck::LaterInsert(identity, cmax),
        record: superseded_entry(data.tg_relation, &old, update_xid),
    }
}

fn registered_key_text(
    data: &pg_sys::TriggerData,
    tuple: *mut pg_sys::HeapTupleData,
    key_attnum: i16,
) -> &CStr {
    let mut is_null = false;
    // SAFETY: tuple is a trigger tuple of the trigger relation, and key_attnum is one of its attributes.
    let (value, type_id) = unsafe {
        let descriptor = (*data.tg_relation).rd_att;
        let value = pg_sys::heap_getattr(tuple, i32::from(key_attnum), descriptor, &mut is_null);
        let attribute = pg_sys::TupleDescAttr(descriptor, i32::from(key_attnum) - 1);
        (value, (*attribute).atttypid)
    };
    if is_null {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "capture fence registered key is null"
        );
    }
    let mut output_function = pg_sys::InvalidOid;
    let mut is_varlena = false;
    // SAFETY: value is a non-null value of type_id. Its text output lives in the memory context of the trigger call.
    unsafe {
        pg_sys::getTypeOutputInfo(type_id, &mut output_function, &mut is_varlena);
        CStr::from_ptr(pg_sys::OidOutputFunctionCall(output_function, value))
    }
}

fn row_identity(relation_name: &str, key: &CStr) -> Vec<u8> {
    let key = key.to_bytes();
    let mut identity = Vec::new();
    if identity
        .try_reserve_exact(relation_name.len() + 1 + key.len())
        .is_err()
    {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY,
            "capture fence order state is out of memory"
        );
    }
    identity.extend_from_slice(relation_name.as_bytes());
    identity.push(0);
    identity.extend_from_slice(key);
    identity
}

fn read_tuple_header(
    relation: pg_sys::Relation,
    tuple: *const pg_sys::HeapTupleData,
) -> TupleHeader {
    // SAFETY: tuple is a valid trigger tuple.
    let tid = unsafe { (*tuple).t_self };
    let (block, offset) = item_pointer_get_both(tid);
    // SAFETY: The buffer stays pinned and share-locked while the code reads the page item.
    // A normal heap item starts with a MAXALIGN heap tuple header.
    let header = unsafe {
        let buffer = pg_sys::ReadBuffer(relation, block);
        pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_SHARE as i32);
        let page = pg_sys::BufferGetPage(buffer);
        let mut header = None;
        if (1..=pg_sys::PageGetMaxOffsetNumber(page)).contains(&offset) {
            let item = pg_sys::PageGetItemId(page, offset);
            if (*item).lp_flags() == pg_sys::LP_NORMAL {
                header = Some(std::ptr::read(
                    pg_sys::PageGetItem(page, item).cast::<pg_sys::HeapTupleHeaderData>(),
                ));
            }
        }
        pg_sys::UnlockReleaseBuffer(buffer);
        header
    };
    let Some(header) = header else {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "capture fence tuple is not a normal heap item"
        );
    };
    TupleHeader { tid, header }
}

fn output_tuple_key(relation: pg_sys::Relation, output: &TupleHeader) -> TupleKey {
    if heap_tuple_header_xmin_frozen(&output.header) {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "COPY FREEZE is not supported on a registered table"
        );
    }
    let xmin = heap_tuple_header_get_raw_xmin(&output.header);
    // SAFETY: TransactionIdIsCurrentTransactionId reads only transaction state.
    if !unsafe { pg_sys::TransactionIdIsCurrentTransactionId(xmin) } {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "capture fence tuple is not from the current transaction"
        );
    }
    tuple_key(relation, output)
}

fn superseded_entry(
    relation: pg_sys::Relation,
    old: &TupleHeader,
    write_xid: pg_sys::TransactionId,
) -> Option<OrderEntry> {
    let xmin = heap_tuple_header_get_xmin(&old.header);
    // SAFETY: TransactionIdIsCurrentTransactionId reads only transaction state.
    unsafe { pg_sys::TransactionIdIsCurrentTransactionId(xmin) }
        .then(|| OrderEntry::Superseded(tuple_key(relation, old), write_xid))
}

fn tuple_key(relation: pg_sys::Relation, tuple: &TupleHeader) -> TupleKey {
    let (block, offset) = item_pointer_get_both(tuple.tid);
    // SAFETY: relation is valid, and the caller proved that the xmin of the header copy is current,
    // as HeapTupleHeaderGetCmin requires.
    let (relation_id, cmin) = unsafe {
        (
            (*relation).rd_id,
            pg_sys::HeapTupleHeaderGetCmin(&tuple.header),
        )
    };
    (
        relation_id,
        block,
        offset,
        heap_tuple_header_get_raw_xmin(&tuple.header),
        cmin,
    )
}

fn heap_tuple_header_get_raw_xmin(header: &pg_sys::HeapTupleHeaderData) -> pg_sys::TransactionId {
    // SAFETY: A heap tuple header on a heap page uses the t_heap union field.
    unsafe { header.t_choice.t_heap.t_xmin }
}

fn heap_tuple_header_xmin_frozen(header: &pg_sys::HeapTupleHeaderData) -> bool {
    u32::from(header.t_infomask) & pg_sys::HEAP_XMIN_FROZEN == pg_sys::HEAP_XMIN_FROZEN
}

fn heap_tuple_header_get_xmin(header: &pg_sys::HeapTupleHeaderData) -> pg_sys::TransactionId {
    if heap_tuple_header_xmin_frozen(header) {
        pg_sys::FrozenTransactionId
    } else {
        heap_tuple_header_get_raw_xmin(header)
    }
}

fn heap_tuple_header_get_update_xid(header: &pg_sys::HeapTupleHeaderData) -> pg_sys::TransactionId {
    let infomask = u32::from(header.t_infomask);
    if infomask & pg_sys::HEAP_XMAX_INVALID == 0
        && infomask & pg_sys::HEAP_XMAX_IS_MULTI != 0
        && infomask & pg_sys::HEAP_XMAX_LOCK_ONLY == 0
    {
        // SAFETY: header is a valid heap tuple header copy with an updating multixact.
        unsafe { pg_sys::HeapTupleGetUpdateXid(header) }
    } else {
        // SAFETY: A heap tuple header on a heap page uses the t_heap union field.
        unsafe { header.t_choice.t_heap.t_xmax }
    }
}

fn check_write_order(check: &OrderCheck) {
    let Some(later_xids) = later_write_xids(check) else {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY,
            "capture fence order state is out of memory"
        );
    };
    let later_write_is_current = later_xids.into_iter().any(|xid| {
        // SAFETY: TransactionIdIsCurrentTransactionId reads only transaction state.
        unsafe { pg_sys::TransactionIdIsCurrentTransactionId(xid) }
    });
    if later_write_is_current {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION,
            "a later write changed the registered row before its capture fence fired"
        );
    }
}

fn later_write_xids(check: &OrderCheck) -> Option<Vec<pg_sys::TransactionId>> {
    ORDER_STATE.with(|state| {
        let state = state.borrow();
        let mut xids = Vec::new();
        let Some(state) = state.as_ref() else {
            return Some(xids);
        };
        match check {
            OrderCheck::Superseded(key) => {
                if let Some(xid) = state.superseded.get(key) {
                    xids.try_reserve_exact(1).ok()?;
                    xids.push(*xid);
                }
            }
            OrderCheck::LaterInsert(identity, command) => {
                let later = state
                    .inserts
                    .get(identity)
                    .into_iter()
                    .flatten()
                    .filter(|(cmin, _)| cmin > command);
                xids.try_reserve_exact(later.clone().count()).ok()?;
                xids.extend(later.map(|(_, xid)| *xid));
            }
        }
        Some(xids)
    })
}

fn call_record_body(data: &pg_sys::TriggerData) {
    // SAFETY: regprocedurein takes one cstring argument.
    let function = unsafe {
        pgrx::direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            &[c"synchro.synchro_capture_fence_record()".into_datum()],
        )
    }
    .expect("regprocedurein returns an OID");
    let mut flinfo = pg_sys::FmgrInfo::default();
    // SAFETY: function is a valid function OID, and flinfo is a writable FmgrInfo.
    unsafe { pg_sys::fmgr_info(function, &mut flinfo) };
    let body = flinfo.fn_addr.expect("fmgr_info sets the function address");
    let mut fcinfo = pg_sys::FunctionCallInfoBaseData {
        flinfo: &mut flinfo,
        context: std::ptr::from_ref(data).cast_mut().cast(),
        fncollation: pg_sys::InvalidOid,
        nargs: 0,
        ..Default::default()
    };
    // SAFETY: fcinfo has no arguments and the TriggerData of this trigger call, as the trigger body requires.
    unsafe { pg_sys::ffi::pg_guard_ffi_boundary(|| body(&mut fcinfo)) };
}

fn record_write_order(entry: OrderEntry) {
    if ORDER_STATE.with(|state| state.borrow().is_none()) {
        register_order_state_reset();
    }
    let recorded = ORDER_STATE.with(|state| {
        let mut state = state.borrow_mut();
        let state = state.get_or_insert_with(OrderState::default);
        match entry {
            OrderEntry::Superseded(key, xid) => {
                state.superseded.try_reserve(1).ok()?;
                state.superseded.insert(key, xid);
            }
            OrderEntry::Insert(identity, cmin, xid) => {
                state.inserts.try_reserve(1).ok()?;
                let entries = state.inserts.entry(identity).or_default();
                entries.try_reserve(1).ok()?;
                entries.push((cmin, xid));
            }
        }
        Some(())
    });
    if recorded.is_none() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_OUT_OF_MEMORY,
            "capture fence order state is out of memory"
        );
    }
}

fn register_order_state_reset() {
    // SAFETY: TopTransactionContext is valid in a transaction. It owns the zeroed callback until its reset.
    unsafe {
        let callback = pg_sys::MemoryContextAllocZero(
            pg_sys::TopTransactionContext,
            std::mem::size_of::<pg_sys::MemoryContextCallback>(),
        )
        .cast::<pg_sys::MemoryContextCallback>();
        (*callback).func = Some(reset_order_state);
        pg_sys::MemoryContextRegisterResetCallback(pg_sys::TopTransactionContext, callback);
    }
}

#[pg_guard]
extern "C-unwind" fn reset_order_state(_argument: *mut c_void) {
    ORDER_STATE.with(|state| *state.borrow_mut() = None);
}
