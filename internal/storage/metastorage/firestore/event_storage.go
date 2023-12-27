package firestore

import (
	"context"
	"fmt"
	"math"

	"cloud.google.com/go/firestore"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"

	"github.com/gogo/status"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	addEventsSafePadding = int64(20)
)

type (
	eventStorageImpl struct {
		client                                 *firestore.Client
		env                                    string
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
		env:                                    params.Config.ConfigName,
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

func (e *eventStorageImpl) validateEventTag(eventTag uint32) error {
	if eventTag > e.latestEventTag {
		return xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	return nil
}

// AddEventEntries implements internal.EventStorage.
func (e *eventStorageImpl) AddEventEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventEntry) error {
	if err := e.validateEventTag(eventTag); err != nil {
		return err
	}
	if len(eventEntries) == 0 {
		return nil
	}
	startEventId := eventEntries[0].EventId

	return e.instrumentAddEvents.Instrument(ctx, func(ctx context.Context) error {
		var eventsToValidate []*model.EventEntry
		// fetch some events before startEventId
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

		err := internal.ValidateEvents(eventsToValidate)
		if err != nil {
			return xerrors.Errorf("events failed validation: %w", err)
		}

		for start, end := 0, maxBulkWriteSize; start < len(eventEntries); start, end = end, end+maxBulkWriteSize {
			err = e.client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
				maxEventId, err := e.getMaxEventId(ctx, eventTag, t)
				if err != nil {
					return xerrors.Errorf("failed to get max event id: %w", err)
				}
				if maxEventId != model.EventIdDeleted && maxEventId >= startEventId {
					return xerrors.Errorf(
						"cannot override existing event entry with max event id %d with events starting from event id %d",
						maxEventId, startEventId)
				}

				i := start
				for ; i < end && i < len(eventEntries); i++ {
					data := e.fromEventEntry(eventEntries[i])
					err = t.Set(e.getEventDocRef(eventTag, data.Payload.EventId), data)
					if err != nil {
						return xerrors.Errorf("failed to save event entry with id %d: %w", data.Payload.EventId, err)
					}
				}
				return e.setMaxEventId(ctx, eventTag, eventEntries[i-1].EventId, t)
			})
			if err != nil {
				return xerrors.Errorf("failed to save event entries: %w", err)
			}
		}
		return nil
	})
}

// AddEvents implements internal.EventStorage.
func (e *eventStorageImpl) AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
	maxEventId, err := e.GetMaxEventId(ctx, eventTag)
	var startEventId int64
	if err != nil {
		if !xerrors.Is(err, errors.ErrNoEventHistory) {
			return err
		}
		startEventId = model.EventIdStartValue
	} else {
		startEventId = maxEventId + 1
	}
	eventsToAdd := model.ConvertBlockEventsToEventEntries(events, eventTag, startEventId)
	return e.AddEventEntries(ctx, eventTag, eventsToAdd)
}

// GetEventByEventId implements internal.EventStorage.
func (e *eventStorageImpl) GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}

	return e.instrumentGetEventByEventId.Instrument(ctx, func(ctx context.Context) (*model.EventEntry, error) {
		maxEventId, err := e.getMaxEventId(ctx, eventTag, nil)
		if err != nil {
			return nil, xerrors.Errorf("failed to get max event id for eventTag=%d: %w", eventTag, err)
		}
		if maxEventId == model.EventIdDeleted {
			return nil, errors.ErrNoMaxEventIdFound
		}
		if eventId > maxEventId {
			return nil, xerrors.Errorf("invalid eventId %d (event ends at %d) for eventTag=%d: %w", eventId, maxEventId, eventTag, errors.ErrInvalidEventId)
		}
		doc, err := e.getEventDocRef(eventTag, eventId).Get(ctx)
		if err != nil {
			return nil, xerrors.Errorf("failed to get event entry: %w", err)
		}
		if !doc.Exists() {
			return nil, errors.ErrItemNotFound
		}
		eventEntry, err := e.intoEventEntry(doc)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse event entry from firestore document: %w", err)
		}
		return eventEntry, nil
	})
}

// GetEventsAfterEventId implements internal.EventStorage.
func (e *eventStorageImpl) GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}
	return e.instrumentGetEventsAfterEventId.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		docs, err := e.client.
			Collection("env").Doc(e.env).
			Collection("event-tags").Doc(fmt.Sprintf("%d", eventTag)).
			Collection("events").Query.
			StartAfter(e.getEventDocRef(eventTag, eventId)).
			OrderBy(firestore.DocumentID, firestore.Asc).
			Limit(int(maxEvents)).
			Documents(ctx).GetAll()
		if err != nil {
			return nil, xerrors.Errorf("failed to get event entries: %w", err)
		}
		eventEntries := make([]*model.EventEntry, len(docs))
		for i, doc := range docs {
			eventEntries[i], err = e.intoEventEntry(doc)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse event entry from firestore document: %w", err)
			}
		}
		return eventEntries, nil
	})
}

func (e *eventStorageImpl) getEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	docs, err := e.client.
		Collection("env").Doc(e.env).
		Collection("event-tags").Doc(fmt.Sprintf("%d", eventTag)).
		Collection("events").Query.
		Where("BlockHeight", "==", int64(blockHeight)).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, xerrors.Errorf("failed to get event entries: %w", err)
	}
	eventEntries := make([]*model.EventEntry, len(docs))
	for i, doc := range docs {
		eventEntries[i], err = e.intoEventEntry(doc)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse event entry from firestore document: %w", err)
		}
	}
	return eventEntries, nil
}

// GetEventsByBlockHeight implements internal.EventStorage.
func (e *eventStorageImpl) GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}
	return e.instrumentGetEventsByBlockHeight.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		items, err := e.getEventsByBlockHeight(ctx, eventTag, blockHeight)
		if len(items) == 0 {
			return nil, errors.ErrItemNotFound
		}
		return items, err
	})
}

// GetEventsByEventIdRange implements internal.EventStorage.
func (e *eventStorageImpl) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}
	return e.instrumentGetEventsByEventIdRange.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		docs, err := e.client.
			Collection("env").Doc(e.env).
			Collection("event-tags").Doc(fmt.Sprintf("%d", eventTag)).
			Collection("events").Query.
			StartAt(e.getEventDocRef(eventTag, minEventId)).
			EndBefore(e.getEventDocRef(eventTag, maxEventId)).
			OrderBy(firestore.DocumentID, firestore.Asc).
			Documents(ctx).GetAll()
		if err != nil {
			return nil, xerrors.Errorf("failed to get event entries: %w", err)
		}
		if len(docs) != int(maxEventId-minEventId) {
			return nil, errors.ErrItemNotFound
		}
		eventEntries := make([]*model.EventEntry, len(docs))
		for i, doc := range docs {
			eventEntries[i], err = e.intoEventEntry(doc)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse event entry from firestore document: %w", err)
			}
			if eventEntries[i].EventId != minEventId+int64(i) {
				return nil, errors.ErrItemNotFound
			}
		}
		return eventEntries, nil
	})
}

// GetFirstEventIdByBlockHeight implements internal.EventStorage.
func (e *eventStorageImpl) GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return 0, err
	}
	return e.instrumentGetFirstEventIdByBlockHeight.Instrument(ctx, func(ctx context.Context) (int64, error) {
		events, err := e.getEventsByBlockHeight(ctx, eventTag, blockHeight)
		if err != nil {
			return 0, xerrors.Errorf("failed to get events by block height: %w", err)
		}
		if len(events) == 0 {
			return 0, errors.ErrItemNotFound
		}
		eventId := int64(math.MaxInt64)
		for _, event := range events {
			if event.EventId < eventId {
				eventId = event.EventId
			}
		}
		return eventId, nil
	})
}

// GetMaxEventId implements internal.EventStorage.
func (e *eventStorageImpl) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return 0, err
	}
	return e.instrumentGetMaxEventId.Instrument(ctx, func(ctx context.Context) (int64, error) {
		maxEventId, err := e.getMaxEventId(ctx, eventTag, nil)
		if err != nil {
			return 0, nil
		}
		if maxEventId == model.EventIdDeleted {
			return 0, errors.ErrNoEventHistory
		}
		return maxEventId, nil
	})
}

// SetMaxEventId implements internal.EventStorage.
func (e *eventStorageImpl) SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error {
	if err := e.validateEventTag(eventTag); err != nil {
		return err
	}
	if maxEventId < model.EventIdStartValue && maxEventId != model.EventIdDeleted {
		return xerrors.Errorf("invalid max event id: %d", maxEventId)
	}
	return e.instrumentSetMaxEventId.Instrument(ctx, func(ctx context.Context) error {
		return e.client.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
			currentMaxEventId, err := e.getMaxEventId(ctx, eventTag, t)
			if err != nil {
				return xerrors.Errorf("failed to get max event id: %w", err)
			}
			if maxEventId > currentMaxEventId {
				return xerrors.Errorf("can not set max event id to be %d, which is bigger than current max event id: %d", maxEventId, currentMaxEventId)
			}
			if maxEventId != model.EventIdDeleted {
				newLatestEventDocRef := e.getEventDocRef(eventTag, maxEventId)
				newLatestEventDoc, err := t.Get(newLatestEventDocRef)
				if err != nil {
					return xerrors.Errorf("failed to get event entry with new max event id: %d, error: %w", maxEventId, err)
				}
				if !newLatestEventDoc.Exists() {
					return xerrors.Errorf("event entry with new max event id %d does not exist", maxEventId)
				}
			}
			return e.setMaxEventId(ctx, eventTag, maxEventId, t)
		})
	})
}

/////////////////////////////////////////////////////////
// firestore storage

// Fields in the payload map are configured not to be automatically indexed.
type firestoreEventEntryPayload struct {
	EventTag       uint32
	EventId        int64
	EventType      api.BlockchainEvent_Type
	BlockHeight    int64
	BlockHash      string
	Tag            uint32
	ParentHash     string
	BlockSkipped   bool
	BlockTimestamp int64
}

type firestoreEventEntry struct {
	BlockHeight int64
	Payload     firestoreEventEntryPayload
}

type firestoreEventsCollectionMetadata struct {
	MaxEventId int64
}

func (e *eventStorageImpl) getMaxEventId(ctx context.Context, eventTag uint32, t *firestore.Transaction) (int64, error) {
	docRef := e.getEventsCollectionMetadataDocRef(eventTag)
	var doc *firestore.DocumentSnapshot
	var err error
	if t == nil {
		doc, err = docRef.Get(ctx)
	} else {
		doc, err = t.Get(docRef)
	}
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return model.EventIdDeleted, nil
		}
		return 0, xerrors.Errorf("failed to get max event id, error: %w", err)
	}
	if doc.Exists() {
		var entry firestoreEventsCollectionMetadata
		err = doc.DataTo(&entry)
		if err != nil {
			return 0, xerrors.Errorf("failed to parse max event id from firestore, error: %w", err)
		}
		return entry.MaxEventId, nil
	}
	return model.EventIdDeleted, nil
}

func (e *eventStorageImpl) setMaxEventId(ctx context.Context, eventTag uint32, eventId int64, t *firestore.Transaction) error {
	docRef := e.getEventsCollectionMetadataDocRef(eventTag)
	var err error
	var entry firestoreEventsCollectionMetadata
	entry.MaxEventId = eventId
	if t == nil {
		_, err = docRef.Set(ctx, &entry)
	} else {
		err = t.Set(docRef, &entry)
	}
	return err
}

func (*eventStorageImpl) fromEventEntry(eventEntry *model.EventEntry) *firestoreEventEntry {
	entry := &firestoreEventEntry{
		BlockHeight: int64(eventEntry.BlockHeight),
		Payload: firestoreEventEntryPayload{
			EventId:        eventEntry.EventId,
			EventType:      eventEntry.EventType,
			BlockHeight:    int64(eventEntry.BlockHeight),
			BlockHash:      eventEntry.BlockHash,
			Tag:            eventEntry.Tag,
			ParentHash:     eventEntry.ParentHash,
			BlockSkipped:   eventEntry.BlockSkipped,
			EventTag:       eventEntry.EventTag,
			BlockTimestamp: eventEntry.BlockTimestamp,
		},
	}
	if entry.Payload.Tag == 0 {
		entry.Payload.Tag = model.DefaultBlockTag
	}
	return entry
}

func (*eventStorageImpl) intoEventEntry(doc *firestore.DocumentSnapshot) (*model.EventEntry, error) {
	var s firestoreEventEntry
	err := doc.DataTo(&s)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse document into EventEntry: %w", err)
	}
	if s.BlockHeight < 0 {
		return nil, xerrors.Errorf("expecting block Height to be uint64, but got %d", s.BlockHeight)
	}
	return &model.EventEntry{
		EventId:        s.Payload.EventId,
		EventType:      s.Payload.EventType,
		BlockHeight:    uint64(s.Payload.BlockHeight),
		BlockHash:      s.Payload.BlockHash,
		Tag:            s.Payload.Tag,
		ParentHash:     s.Payload.ParentHash,
		MaxEventId:     0,
		BlockSkipped:   s.Payload.BlockSkipped,
		EventTag:       s.Payload.EventTag,
		BlockTimestamp: s.Payload.BlockTimestamp,
	}, nil
}

func (e *eventStorageImpl) getEventDocRef(eventTag uint32, eventId int64) *firestore.DocumentRef {
	return e.client.Doc(fmt.Sprintf("env/%s/event-tags/%d/events/%020d", e.env, eventTag, eventId))
}

func (e *eventStorageImpl) getEventsCollectionMetadataDocRef(eventTag uint32) *firestore.DocumentRef {
	return e.client.Doc(fmt.Sprintf("env/%s/event-tags/%d", e.env, eventTag))
}
