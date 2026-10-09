package postgres

import (
	"context"
	"database/sql"
	"time"

	"golang.org/x/xerrors"

	"github.com/lib/pq"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	pgmodel "github.com/coinbase/chainstorage/internal/storage/metastorage/postgres/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
)

const (
	addEventsSafePadding = int64(20)
)

type (
	eventStorageImpl struct {
		db                                     *sql.DB
		instrumentAddEvents                    instrument.Instrument
		instrumentGetEventByEventId            instrument.InstrumentWithResult[*model.EventEntry]
		instrumentGetEventsAfterEventId        instrument.InstrumentWithResult[[]*model.EventEntry]
		instrumentGetEventsByEventIdRange      instrument.InstrumentWithResult[[]*model.EventEntry]
		instrumentGetMaxEventId                instrument.InstrumentWithResult[int64]
		instrumentSetMaxEventId                instrument.Instrument
		instrumentGetFirstEventIdByBlockHeight instrument.InstrumentWithResult[int64]
		instrumentGetEventsByBlockHeight       instrument.InstrumentWithResult[[]*model.EventEntry]
	}
)

func newEventStorage(db *sql.DB, params Params) (internal.EventStorage, error) {
	metrics := params.Metrics.SubScope("event_storage").Tagged(map[string]string{
		"storage_type": "postgres",
	})
	storage := &eventStorageImpl{
		db:                                     db,
		instrumentAddEvents:                    instrument.New(metrics, "add_events"),
		instrumentGetEventByEventId:            instrument.NewWithResult[*model.EventEntry](metrics, "get_event_by_event_id"),
		instrumentGetEventsAfterEventId:        instrument.NewWithResult[[]*model.EventEntry](metrics, "get_events_after_event_id"),
		instrumentGetEventsByEventIdRange:      instrument.NewWithResult[[]*model.EventEntry](metrics, "get_events_by_event_id_range"),
		instrumentGetMaxEventId:                instrument.NewWithResult[int64](metrics, "get_max_event_id"),
		instrumentSetMaxEventId:                instrument.New(metrics, "set_max_event_id"),
		instrumentGetFirstEventIdByBlockHeight: instrument.NewWithResult[int64](metrics, "get_first_event_id_by_block_height"),
		instrumentGetEventsByBlockHeight:       instrument.NewWithResult[[]*model.EventEntry](metrics, "get_events_by_block_height"),
	}
	return storage, nil
}

func (e *eventStorageImpl) AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
	if len(events) == 0 {
		return nil
	}
	return e.instrumentAddEvents.Instrument(ctx, func(ctx context.Context) error {
		maxEventId, err := e.GetMaxEventId(ctx, eventTag)
		var startEventId int64
		if err != nil {
			if !xerrors.Is(err, errors.ErrNoEventHistory) {
				return xerrors.Errorf("failed to get max event id: %w", err)
			}
			startEventId = model.EventIdStartValue
		} else {
			startEventId = maxEventId + 1
		}

		eventEntries := model.ConvertBlockEventsToEventEntries(events, eventTag, startEventId)
		return e.AddEventEntries(ctx, eventTag, eventEntries)
	})
}

func (e *eventStorageImpl) AddEventEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventEntry) error {
	if len(eventEntries) == 0 {
		return nil
	}
	return e.instrumentAddEvents.Instrument(ctx, func(ctx context.Context) error {
		startEventId := eventEntries[0].EventId
		var eventsToValidate []*model.EventEntry
		startFetchId := startEventId - addEventsSafePadding
		if startFetchId < model.EventIdStartValue {
			startFetchId = model.EventIdStartValue
		}
		if startFetchId < startEventId {
			beforeEvents, err := e.GetEventsByEventIdRange(ctx, eventTag, startFetchId, startEventId)
			if err != nil {
				return xerrors.Errorf("failed to fetch events: %w", err)
			}
			eventsToValidate = append(beforeEvents, eventEntries...)
		} else {
			eventsToValidate = eventEntries
		}

		if err := internal.ValidateEvents(eventsToValidate); err != nil {
			return xerrors.Errorf("events failed validation: %w", err)
		}

		// Create transaction with timeout context for event operations
		txCtx, cancel := context.WithTimeout(ctx, 180*time.Second)
		defer cancel()

		tx, err := e.db.BeginTx(txCtx, nil)
		if err != nil {
			return xerrors.Errorf("failed to start transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					_ = rollbackErr
				}
			}
		}()

		// Stage incoming events (no per-row lookups) for set-based processing
		if _, err := tx.ExecContext(ctx, `
			CREATE TEMP TABLE temp_events (
				event_tag INT NOT NULL,
				event_sequence BIGINT NOT NULL,
				event_type TEXT NOT NULL,
				height BIGINT NOT NULL,
				hash VARCHAR(66),
				bm_tag INT NOT NULL,
				skipped BOOLEAN NOT NULL
			) ON COMMIT DROP
		`); err != nil {
			return xerrors.Errorf("failed to create temp_events: %w", err)
		}

		stmt, err := tx.Prepare(pq.CopyIn("temp_events",
			"event_tag", "event_sequence", "event_type", "height", "hash", "bm_tag", "skipped"))
		if err != nil {
			return xerrors.Errorf("failed to prepare COPY for temp_events: %w", err)
		}

		for _, e := range eventEntries {
			if _, err := stmt.Exec(
				eventTag,
				e.EventId,
				pgmodel.EventTypeToString(e.EventType),
				e.BlockHeight,
				e.BlockHash,
				int(e.Tag),
				e.BlockSkipped,
			); err != nil {
				_ = stmt.Close()
				return xerrors.Errorf("failed to buffer temp_events row: %w", err)
			}
		}

		if _, err := stmt.Exec(); err != nil {
			_ = stmt.Close()
			return xerrors.Errorf("failed to finalize COPY temp_events: %w", err)
		}
		if err := stmt.Close(); err != nil {
			return xerrors.Errorf("failed to close COPY statement: %w", err)
		}

		// Ensure skipped block_metadata rows exist in bulk
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO block_metadata (height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped)
			SELECT DISTINCT e.height, e.bm_tag, NULL, NULL, 0, NULL, 0, true
			FROM temp_events e
			WHERE e.skipped = true
			ON CONFLICT (tag, height) WHERE skipped = true DO NOTHING
		`); err != nil {
			return xerrors.Errorf("failed to upsert skipped block_metadata: %w", err)
		}

		// Insert non-skipped events by joining on (tag, hash) with fallback for DefaultBlockTag via UNION ALL
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO block_events (event_tag, event_sequence, event_type, block_metadata_id, height, hash)
			SELECT e.event_tag, e.event_sequence, e.event_type::event_type_enum, bm.id, e.height, e.hash
			FROM temp_events e
			JOIN block_metadata bm
			  ON bm.tag = e.bm_tag AND bm.hash = e.hash AND bm.skipped = false
			WHERE e.skipped = false
			UNION ALL
			SELECT e.event_tag, e.event_sequence, e.event_type::event_type_enum, bm.id, e.height, e.hash
			FROM temp_events e
			JOIN block_metadata bm
			  ON e.bm_tag = $1 AND bm.tag = 0 AND bm.hash = e.hash AND bm.skipped = false
			WHERE e.skipped = false
			ON CONFLICT (event_tag, event_sequence) DO NOTHING
		`, model.DefaultBlockTag); err != nil {
			return xerrors.Errorf("failed to insert non-skipped events: %w", err)
		}

		// Insert skipped events by joining on (tag, height, skipped=true) with fallback for DefaultBlockTag via UNION ALL
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO block_events (event_tag, event_sequence, event_type, block_metadata_id, height, hash)
			SELECT e.event_tag, e.event_sequence, e.event_type::event_type_enum, bm.id, e.height, e.hash
			FROM temp_events e
			JOIN block_metadata bm
			  ON bm.tag = e.bm_tag AND bm.height = e.height AND bm.skipped = true
			WHERE e.skipped = true
			UNION ALL
			SELECT e.event_tag, e.event_sequence, e.event_type::event_type_enum, bm.id, e.height, e.hash
			FROM temp_events e
			JOIN block_metadata bm
			  ON e.bm_tag = $1 AND bm.tag = 0 AND bm.height = e.height AND bm.skipped = true
			WHERE e.skipped = true
			ON CONFLICT (event_tag, event_sequence) DO NOTHING
		`, model.DefaultBlockTag); err != nil {
			return xerrors.Errorf("failed to insert skipped events: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return xerrors.Errorf("failed to commit transaction: %w", err)
		}
		committed = true
		return nil
	})
}

func (e *eventStorageImpl) GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error) {
	return e.instrumentGetEventByEventId.Instrument(ctx, func(ctx context.Context) (*model.EventEntry, error) {
		var eventEntry model.EventEntry
		var eventTypeStr string
		var blockHash sql.NullString
		var tag sql.NullInt32
		var parentHash sql.NullString
		var skipped sql.NullBool
		var timestamp sql.NullInt64

		err := e.db.QueryRowContext(ctx, `
			SELECT be.event_sequence, be.event_type, be.height, be.hash, 
				   bm.tag, bm.parent_hash, bm.skipped, bm.timestamp, be.event_tag
			FROM block_events be
			LEFT JOIN block_metadata bm ON be.block_metadata_id = bm.id
			WHERE be.event_tag = $1 AND be.event_sequence = $2
		`, eventTag, eventId).Scan(
			&eventEntry.EventId,
			&eventTypeStr,
			&eventEntry.BlockHeight,
			&blockHash,
			&tag,
			&parentHash,
			&skipped,
			&timestamp,
			&eventEntry.EventTag,
		)

		if err != nil {
			if err == sql.ErrNoRows {
				return nil, errors.ErrItemNotFound
			}
			return nil, xerrors.Errorf("failed to get event by event id: %w", err)
		}

		// Handle null values from LEFT JOIN
		if blockHash.Valid {
			eventEntry.BlockHash = blockHash.String
		} else {
			eventEntry.BlockHash = ""
		}
		if tag.Valid {
			eventEntry.Tag = uint32(tag.Int32)
		} else {
			eventEntry.Tag = model.DefaultBlockTag
		}
		if parentHash.Valid {
			eventEntry.ParentHash = parentHash.String
		} else {
			eventEntry.ParentHash = ""
		}
		if skipped.Valid {
			eventEntry.BlockSkipped = skipped.Bool
		} else {
			eventEntry.BlockSkipped = false
		}
		if timestamp.Valid {
			eventEntry.BlockTimestamp = timestamp.Int64
		} else {
			eventEntry.BlockTimestamp = 0
		}

		// switch to defaultTag is not set
		if eventEntry.Tag == 0 {
			eventEntry.Tag = model.DefaultBlockTag
		}

		eventEntry.EventType = pgmodel.ParseEventType(eventTypeStr)
		return &eventEntry, nil
	})
}

func (e *eventStorageImpl) GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
	return e.instrumentGetEventsAfterEventId.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		rows, err := e.db.QueryContext(ctx, `
			SELECT be.event_sequence, be.event_type, be.height, be.hash, bm.tag, bm.parent_hash, 
				   bm.skipped, bm.timestamp, be.event_tag
			FROM block_events be
			LEFT JOIN block_metadata bm ON be.block_metadata_id = bm.id
			WHERE be.event_tag = $1 AND be.event_sequence > $2
			ORDER BY be.event_sequence ASC
			LIMIT $3
		`, eventTag, eventId, maxEvents)

		if err != nil {
			return nil, xerrors.Errorf("failed to get events after event id: %w", err)
		}

		var result []*model.EventEntry
		var scanErr error
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && scanErr == nil {
				scanErr = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		result, scanErr = e.scanEventEntries(rows)
		return result, scanErr
	})
}

func (e *eventStorageImpl) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	return e.instrumentGetEventsByEventIdRange.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		rows, err := e.db.QueryContext(ctx, `
			SELECT be.event_sequence, be.event_type, be.height, be.hash, bm.tag, bm.parent_hash, 
				   bm.skipped, bm.timestamp, be.event_tag
			FROM block_events be
			LEFT JOIN block_metadata bm ON be.block_metadata_id = bm.id
			WHERE be.event_tag = $1 AND be.event_sequence >= $2 AND be.event_sequence < $3
			ORDER BY be.event_sequence ASC
		`, eventTag, minEventId, maxEventId)

		if err != nil {
			return nil, xerrors.Errorf("failed to get events by event id range: %w", err)
		}

		var events []*model.EventEntry
		var scanErr error
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && scanErr == nil && events != nil {
				scanErr = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		events, scanErr = e.scanEventEntries(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		// Validate that we have all events in the range
		expectedCount := maxEventId - minEventId
		if int64(len(events)) != expectedCount {
			return nil, errors.ErrItemNotFound
		}

		// Check for close error one more time
		if scanErr != nil {
			return nil, scanErr
		}

		return events, nil
	})
}

func (e *eventStorageImpl) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	return e.instrumentGetMaxEventId.Instrument(ctx, func(ctx context.Context) (int64, error) {
		var maxEventId sql.NullInt64
		err := e.db.QueryRowContext(ctx, `
			SELECT MAX(event_sequence) FROM block_events WHERE event_tag = $1 
		`, eventTag).Scan(&maxEventId) //watermark
		if err != nil {
			return 0, xerrors.Errorf("failed to get max event id: %w", err)
		}
		if !maxEventId.Valid {
			return 0, errors.ErrNoEventHistory
		}
		return maxEventId.Int64, nil
	})
}

// basically if we have events 1,2,3,4,5,6,7 and call SetMaxEventId(ctx, eventTag, 4), then we will delete all events after 4
func (e *eventStorageImpl) SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error {
	return e.instrumentSetMaxEventId.Instrument(ctx, func(ctx context.Context) error {
		if maxEventId < model.EventIdStartValue && maxEventId != model.EventIdDeleted {
			return xerrors.Errorf("invalid max event id: %d", maxEventId)
		}

		txCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		tx, err := e.db.BeginTx(txCtx, nil)
		if err != nil {
			return xerrors.Errorf("failed to start transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					// Log the rollback error but don't override the original error
					_ = rollbackErr
				}
			}
		}()

		if maxEventId == model.EventIdDeleted {
			// Delete all events for this tag
			_, err = tx.ExecContext(txCtx, `
				DELETE FROM block_events WHERE event_tag = $1
			`, eventTag)
			if err != nil {
				return xerrors.Errorf("failed to delete events: %w", err)
			}
		} else {
			// Validate the new max event ID exists
			var exists bool
			err = tx.QueryRowContext(txCtx, `
				SELECT EXISTS(SELECT 1 FROM block_events WHERE event_tag = $1 AND event_sequence = $2)
			`, eventTag, maxEventId).Scan(&exists)
			if err != nil {
				return xerrors.Errorf("failed to validate max event id: %w", err)
			}
			if !exists {
				return xerrors.Errorf("event entry with max event id %d does not exist", maxEventId)
			}
			// Delete events beyond the max event ID
			_, err = tx.ExecContext(txCtx, `
				DELETE FROM block_events WHERE event_tag = $1 AND event_sequence > $2
			`, eventTag, maxEventId)
			if err != nil {
				return xerrors.Errorf("failed to delete events beyond max event id: %w", err)
			}
		}

		err = tx.Commit()
		if err != nil {
			return xerrors.Errorf("failed to commit transaction: %w", err)
		}
		committed = true
		return nil
	})
}

func (e *eventStorageImpl) GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error) {
	return e.instrumentGetFirstEventIdByBlockHeight.Instrument(ctx, func(ctx context.Context) (int64, error) {
		var firstEventId int64

		err := e.db.QueryRowContext(ctx, `
			SELECT MIN(be.event_sequence)
			FROM block_events be
			WHERE be.event_tag = $1 AND be.height = $2
		`, eventTag, blockHeight).Scan(&firstEventId)

		if err != nil {
			if err == sql.ErrNoRows {
				return 0, errors.ErrItemNotFound
			}
			return 0, xerrors.Errorf("failed to get first event id by block height: %w", err)
		}

		return firstEventId, nil
	})
}

func (e *eventStorageImpl) GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	return e.instrumentGetEventsByBlockHeight.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		rows, err := e.db.QueryContext(ctx, `
			SELECT be.event_sequence, be.event_type, be.height, be.hash, bm.tag, bm.parent_hash, 
				   bm.skipped, bm.timestamp, be.event_tag
			FROM block_events be
			LEFT JOIN block_metadata bm ON be.block_metadata_id = bm.id
			WHERE be.event_tag = $1 AND be.height = $2
			ORDER BY be.event_sequence ASC
		`, eventTag, blockHeight)

		if err != nil {
			return nil, xerrors.Errorf("failed to get events by block height: %w", err)
		}

		var events []*model.EventEntry
		var scanErr error
		defer func() {
			if closeErr := rows.Close(); closeErr != nil && scanErr == nil {
				scanErr = xerrors.Errorf("failed to close rows: %w", closeErr)
			}
		}()

		events, scanErr = e.scanEventEntries(rows)
		if scanErr != nil {
			return nil, scanErr
		}

		if len(events) == 0 {
			return nil, errors.ErrItemNotFound
		}

		// Check for close error one more time
		if scanErr != nil {
			return nil, scanErr
		}

		return events, nil
	})
}

// Helper functions
func (e *eventStorageImpl) getOrCreateBlockMetadataId(ctx context.Context, tx *sql.Tx, eventEntry *model.EventEntry) (int64, error) {
	// For skipped blocks, create or find block metadata with specific fields
	if eventEntry.BlockSkipped {
		// Try to find existing block metadata for this skipped event
		var blockMetadataId int64
		err := tx.QueryRowContext(ctx, `
			SELECT id FROM block_metadata WHERE tag = $1 AND height = $2 AND skipped = true
		`, eventEntry.Tag, eventEntry.BlockHeight).Scan(&blockMetadataId)
		if err == nil {
			return blockMetadataId, nil
		}
		// If not found and eventEntry.Tag is DefaultBlockTag, try with tag = 0
		if err == sql.ErrNoRows && eventEntry.Tag == model.DefaultBlockTag {
			err = tx.QueryRowContext(ctx, `
				SELECT id FROM block_metadata WHERE tag = $1 AND height = $2 AND skipped = true
			`, uint32(0), eventEntry.BlockHeight).Scan(&blockMetadataId)

			if err == nil {
				return blockMetadataId, nil
			}
		}

		// If block metadata not found for skipped event, create it
		if err == sql.ErrNoRows {
			return e.createSkippedBlockMetadata(ctx, tx, eventEntry)
		}
		return 0, xerrors.Errorf("failed to query block metadata: %w", err)
	}

	// For non-skipped blocks, look up by tag and hash
	// First try with the eventEntry.Tag
	var blockMetadataId int64
	err := tx.QueryRowContext(ctx, `		SELECT id FROM block_metadata WHERE tag = $1 AND hash = $2
	`, eventEntry.Tag, eventEntry.BlockHash).Scan(&blockMetadataId)
	if err == nil {
		return blockMetadataId, nil
	}
	// If not found and eventEntry.Tag is DefaultBlockTag, try with tag = 0
	if err == sql.ErrNoRows && eventEntry.Tag == model.DefaultBlockTag {
		err = tx.QueryRowContext(ctx, `
			SELECT id FROM block_metadata WHERE tag = $1 AND hash = $2
		`, uint32(0), eventEntry.BlockHash).Scan(&blockMetadataId)

		if err == nil {
			return blockMetadataId, nil
		}
	}

	// If we get here, the block metadata was not found
	if err == sql.ErrNoRows {
		return 0, xerrors.Errorf("block metadata not found for tag %d and hash %s", eventEntry.Tag, eventEntry.BlockHash)
	}
	return 0, xerrors.Errorf("failed to query block metadata: %w", err)
}

// createSkippedBlockMetadata creates a new block_metadata entry for a skipped block
func (e *eventStorageImpl) createSkippedBlockMetadata(ctx context.Context, tx *sql.Tx, eventEntry *model.EventEntry) (int64, error) {
	// Create block metadata for skipped block with NULL values as specified
	var blockMetadataId int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO block_metadata (height, tag, hash, parent_hash, parent_height, object_key_main, timestamp, skipped) 
		VALUES ($1, $2, NULL, NULL, $3, NULL, $4, true)
		ON CONFLICT (tag, height) WHERE skipped = true DO UPDATE SET
			hash = EXCLUDED.hash,
			parent_hash = EXCLUDED.parent_hash,
			parent_height = EXCLUDED.parent_height,
			object_key_main = EXCLUDED.object_key_main,
			timestamp = EXCLUDED.timestamp,
			skipped = EXCLUDED.skipped
		RETURNING id
	`, eventEntry.BlockHeight, eventEntry.Tag, 0, 0).Scan(&blockMetadataId)
	if err != nil {
		return 0, xerrors.Errorf("failed to create block metadata for skipped block: %w", err)
	}

	return blockMetadataId, nil
}

func (e *eventStorageImpl) scanEventEntries(rows *sql.Rows) ([]*model.EventEntry, error) {
	var events []*model.EventEntry

	for rows.Next() {
		var eventEntry model.EventEntry
		var eventTypeStr string
		var blockHash sql.NullString
		var tag sql.NullInt32
		var parentHash sql.NullString
		var skipped sql.NullBool
		var timestamp sql.NullInt64

		err := rows.Scan(
			&eventEntry.EventId,
			&eventTypeStr,
			&eventEntry.BlockHeight,
			&blockHash,
			&tag,
			&parentHash,
			&skipped,
			&timestamp,
			&eventEntry.EventTag,
		)

		if err != nil {
			return nil, xerrors.Errorf("failed to scan event entry: %w", err)
		}

		// Handle null values from LEFT JOIN
		if blockHash.Valid {
			eventEntry.BlockHash = blockHash.String
		} else {
			eventEntry.BlockHash = ""
		}
		if tag.Valid {
			eventEntry.Tag = uint32(tag.Int32)
		} else {
			eventEntry.Tag = model.DefaultBlockTag
		}
		if parentHash.Valid {
			eventEntry.ParentHash = parentHash.String
		} else {
			eventEntry.ParentHash = ""
		}
		if skipped.Valid {
			eventEntry.BlockSkipped = skipped.Bool
		} else {
			eventEntry.BlockSkipped = false
		}
		if timestamp.Valid {
			eventEntry.BlockTimestamp = timestamp.Int64
		} else {
			eventEntry.BlockTimestamp = 0
		}

		// switch to defaultTag is not set
		if eventEntry.Tag == 0 {
			eventEntry.Tag = model.DefaultBlockTag
		}

		eventEntry.EventType = pgmodel.ParseEventType(eventTypeStr)
		events = append(events, &eventEntry)
	}

	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("error iterating over rows: %w", err)
	}

	return events, nil
}
