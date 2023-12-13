package firestore

import (
	"context"

	"cloud.google.com/go/firestore"

	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
)

type (
	eventStorageImpl struct {
		client                                 *firestore.Client
		latestEventTag                         uint32
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

func newEventStorage(params Params, client *firestore.Client) (internal.EventStorage, error) {
	metrics := params.Metrics.SubScope("event_storage_firestore")
	storage := eventStorageImpl{
		client:                                 client,
		latestEventTag:                         params.Config.GetLatestEventTag(),
		instrumentAddEvents:                    instrument.New(metrics, "add_events"),
		instrumentGetEventByEventId:            instrument.NewWithResult[*model.EventEntry](metrics, "get_event_by_event_id"),
		instrumentGetEventsAfterEventId:        instrument.NewWithResult[[]*model.EventEntry](metrics, "get_events_after_event_id"),
		instrumentGetEventsByEventIdRange:      instrument.NewWithResult[[]*model.EventEntry](metrics, "get_events_by_event_id_range"),
		instrumentGetMaxEventId:                instrument.NewWithResult[int64](metrics, "get_max_event_id"),
		instrumentSetMaxEventId:                instrument.New(metrics, "set_max_event_id"),
		instrumentGetFirstEventIdByBlockHeight: instrument.NewWithResult[int64](metrics, "get_first_event_id_by_block_height"),
		instrumentGetEventsByBlockHeight:       instrument.NewWithResult[[]*model.EventEntry](metrics, "get_events_by_block_height"),
	}
	return &storage, nil
}

// AddEventEntries implements internal.EventStorage.
func (*eventStorageImpl) AddEventEntries(ctx context.Context, eventTag uint32, eventDDBEntries []*model.EventEntry) error {
	panic("unimplemented")
}

// AddEvents implements internal.EventStorage.
func (*eventStorageImpl) AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
	panic("unimplemented")
}

// GetEventByEventId implements internal.EventStorage.
func (*eventStorageImpl) GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error) {
	panic("unimplemented")
}

// GetEventsAfterEventId implements internal.EventStorage.
func (*eventStorageImpl) GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
	panic("unimplemented")
}

// GetEventsByBlockHeight implements internal.EventStorage.
func (*eventStorageImpl) GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	panic("unimplemented")
}

// GetEventsByEventIdRange implements internal.EventStorage.
func (*eventStorageImpl) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	panic("unimplemented")
}

// GetFirstEventIdByBlockHeight implements internal.EventStorage.
func (*eventStorageImpl) GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error) {
	panic("unimplemented")
}

// GetMaxEventId implements internal.EventStorage.
func (*eventStorageImpl) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	panic("unimplemented")
}

// SetMaxEventId implements internal.EventStorage.
func (*eventStorageImpl) SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error {
	panic("unimplemented")
}
