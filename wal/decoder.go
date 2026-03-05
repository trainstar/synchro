package wal

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/trainstar/synchro"
)

// WALEvent represents a decoded change from the WAL.
type WALEvent struct {
	TableName string
	RecordID  string
	Operation synchro.Operation
	Data      map[string]any
}

// Decoder decodes pgoutput protocol messages into WALEvents.
type Decoder struct {
	registry  *synchro.Registry
	relations map[uint32]*pglogrepl.RelationMessage
}

// NewDecoder creates a new WAL message decoder.
func NewDecoder(registry *synchro.Registry) *Decoder {
	return &Decoder{
		registry:  registry,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

// Decode processes a raw WAL message and returns any sync-relevant events.
func (d *Decoder) Decode(walData []byte) ([]WALEvent, error) {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("parsing WAL message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[msg.RelationID] = msg
		return nil, nil

	case *pglogrepl.InsertMessage:
		return d.handleInsert(msg)

	case *pglogrepl.UpdateMessage:
		return d.handleUpdate(msg)

	case *pglogrepl.DeleteMessage:
		return d.handleDelete(msg)

	case *pglogrepl.BeginMessage, *pglogrepl.CommitMessage:
		return nil, nil

	default:
		return nil, nil
	}
}

func (d *Decoder) handleInsert(msg *pglogrepl.InsertMessage) ([]WALEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, nil
	}

	if !d.registry.IsRegistered(rel.RelationName) {
		return nil, nil
	}

	data := d.tupleToMap(rel, msg.Tuple)
	cfg := d.registry.Get(rel.RelationName)
	recordID := fmt.Sprintf("%v", data[cfg.IDColumn])

	return []WALEvent{{
		TableName: rel.RelationName,
		RecordID:  recordID,
		Operation: synchro.OpInsert,
		Data:      data,
	}}, nil
}

func (d *Decoder) handleUpdate(msg *pglogrepl.UpdateMessage) ([]WALEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, nil
	}

	if !d.registry.IsRegistered(rel.RelationName) {
		return nil, nil
	}

	data := d.tupleToMap(rel, msg.NewTuple)
	cfg := d.registry.Get(rel.RelationName)
	recordID := fmt.Sprintf("%v", data[cfg.IDColumn])

	// Detect soft deletes: if deleted_at column is now non-null, emit OpDelete.
	op := synchro.OpUpdate
	if deletedAtVal, ok := data[cfg.DeletedAtColumn]; ok && deletedAtVal != nil {
		op = synchro.OpDelete
	}

	return []WALEvent{{
		TableName: rel.RelationName,
		RecordID:  recordID,
		Operation: op,
		Data:      data,
	}}, nil
}

func (d *Decoder) handleDelete(msg *pglogrepl.DeleteMessage) ([]WALEvent, error) {
	rel, ok := d.relations[msg.RelationID]
	if !ok {
		return nil, nil
	}

	if !d.registry.IsRegistered(rel.RelationName) {
		return nil, nil
	}

	// Delete messages use OldTuple (REPLICA IDENTITY FULL) or key columns
	tuple := msg.OldTuple
	if tuple == nil {
		return nil, nil
	}

	data := d.tupleToMap(rel, tuple)
	cfg := d.registry.Get(rel.RelationName)
	recordID := fmt.Sprintf("%v", data[cfg.IDColumn])

	return []WALEvent{{
		TableName: rel.RelationName,
		RecordID:  recordID,
		Operation: synchro.OpDelete,
		Data:      data,
	}}, nil
}

// tupleToMap converts a pglogrepl tuple to a map using relation column info.
func (d *Decoder) tupleToMap(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	if tuple == nil {
		return nil
	}

	data := make(map[string]any, len(rel.Columns))
	for i, col := range rel.Columns {
		if i >= len(tuple.Columns) {
			break
		}

		colData := tuple.Columns[i]
		switch colData.DataType {
		case 'n': // null
			data[col.Name] = nil
		case 't': // text
			data[col.Name] = string(colData.Data)
		case 'u': // unchanged toast
			// Skip — not available
		}
	}

	return data
}
