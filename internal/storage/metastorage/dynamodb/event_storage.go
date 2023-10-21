package dynamodb

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/utils/instrument"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

const (
	eventIdKeyName          = "event_id"
	heightKeyName           = "height"
	heightValueName         = ":heightValue"
	blockIdKeyName          = "block_id"
	blockIdValueName        = ":blockIdValue"
	versionedEventWatermark = "latest"
	versionedIdFormat       = "%d-%v"
	pkeyValueForWatermark   = int64(-1)
	blockHeightForWatermark = uint64(0)
	addEventsSafePadding    = int64(20)
	defaultEventTag         = uint32(0)
)

type (
	eventStorageImpl struct {
		eventTable                             ddbTable
		versionedEventTable                    ddbTable
		heightIndexName                        string
		versionedEventBlockIndexName           string
		latestEventTag                         uint32
		instrumentAddEvents                    instrument.Call
		instrumentGetEventByEventId            instrument.Call
		instrumentGetEventsAfterEventId        instrument.Call
		instrumentGetEventsByEventIdRange      instrument.Call
		instrumentGetMaxEventId                instrument.Call
		instrumentSetMaxEventId                instrument.Call
		instrumentGetFirstEventIdByBlockHeight instrument.Call
		instrumentGetEventsByBlockHeight       instrument.Call
	}
)

func newEventStorage(params Params) (internal.EventStorage, error) {
	heightIndexName := params.Config.AWS.DynamoDB.EventTableHeightIndex
	eventTable, err := createEventTable(params)
	if err != nil {
		return nil, xerrors.Errorf("failed to create event table: %w", err)
	}

	versionedEventBlockIndexName := params.Config.AWS.DynamoDB.VersionedEventTableBlockIndex
	versionedEventTable, err := createVersionedEventTable(params)
	if err != nil {
		return nil, xerrors.Errorf("failed to create versioned event table: %w", err)
	}

	metrics := params.Metrics.SubScope("event_storage")
	storage := eventStorageImpl{
		eventTable:                             eventTable,
		heightIndexName:                        heightIndexName,
		versionedEventTable:                    versionedEventTable,
		versionedEventBlockIndexName:           versionedEventBlockIndexName,
		latestEventTag:                         params.Config.GetLatestEventTag(),
		instrumentAddEvents:                    instrument.NewCall(metrics, "add_events"),
		instrumentGetEventByEventId:            instrument.NewCall(metrics, "get_event_by_event_id"),
		instrumentGetEventsAfterEventId:        instrument.NewCall(metrics, "get_events_after_event_id"),
		instrumentGetEventsByEventIdRange:      instrument.NewCall(metrics, "get_events_by_event_id_range"),
		instrumentGetMaxEventId:                instrument.NewCall(metrics, "get_max_event_id"),
		instrumentSetMaxEventId:                instrument.NewCall(metrics, "set_max_event_id"),
		instrumentGetFirstEventIdByBlockHeight: instrument.NewCall(metrics, "get_first_event_id_by_block_height"),
		instrumentGetEventsByBlockHeight:       instrument.NewCall(metrics, "get_events_by_block_height"),
	}
	return &storage, nil
}

func createEventTable(params Params) (ddbTable, error) {
	heightIndexName := params.Config.AWS.DynamoDB.EventTableHeightIndex
	attrDefs := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(eventIdKeyName),
			AttributeType: awsNumberType,
		},
		{
			AttributeName: aws.String(heightKeyName),
			AttributeType: awsNumberType,
		},
	}
	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(eventIdKeyName),
			KeyType:       hashKeyType,
		},
	}
	globalSecondaryIndexes := []*dynamodb.GlobalSecondaryIndex{
		{
			IndexName: aws.String(heightIndexName),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(heightKeyName),
					KeyType:       hashKeyType,
				},
			},
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
	}

	eventTable, err := newDDBTable(
		params.Config.AWS.DynamoDB.EventTable,
		reflect.TypeOf(model.EventDDBEntry{}),
		keySchema, attrDefs, globalSecondaryIndexes,
		params,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create event table accessor: %w", err)
	}

	return eventTable, nil
}

func createVersionedEventTable(params Params) (ddbTable, error) {
	if params.Config.GetLatestEventTag() == defaultEventTag {
		return nil, nil
	}

	versionedEventBlockIndexName := params.Config.AWS.DynamoDB.VersionedEventTableBlockIndex
	attrDefs := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(eventIdKeyName),
			AttributeType: awsStringType,
		},
		{
			AttributeName: aws.String(blockIdKeyName),
			AttributeType: awsStringType,
		},
	}
	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(eventIdKeyName),
			KeyType:       hashKeyType,
		},
	}
	globalSecondaryIndexes := []*dynamodb.GlobalSecondaryIndex{
		{
			IndexName: aws.String(versionedEventBlockIndexName),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(blockIdKeyName),
					KeyType:       hashKeyType,
				},
			},
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
	}

	versionedEventTable, err := newDDBTable(
		params.Config.AWS.DynamoDB.VersionedEventTable,
		reflect.TypeOf(model.VersionedEventDDBEntry{}),
		keySchema, attrDefs, globalSecondaryIndexes,
		params,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create versioned event table accessor: %w", err)
	}

	return versionedEventTable, nil
}

func getEventStorageKeyMap(eventId int64) StringMap {
	return StringMap{
		eventIdKeyName: eventId,
	}
}

func getVersionedEventStorageKeyMap(eventId string) StringMap {
	return StringMap{
		eventIdKeyName: eventId,
	}
}

func (e *eventStorageImpl) getEventByKey(ctx context.Context, eventId int64) (*internal.EventEntry, error) {
	eventKeyMap := getEventStorageKeyMap(eventId)
	outputItem, err := e.eventTable.GetItem(ctx, eventKeyMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to get event: %w", err)
	}
	eventDDBEntry, ok := model.CastItemToDDBEntry(outputItem)
	if !ok {
		return nil, xerrors.Errorf("failed to cast to EventDDBEntry: %v", outputItem)
	}
	return model.IntoEventEntry(eventDDBEntry), nil
}

func (e *eventStorageImpl) getVersionedEventByKey(
	ctx context.Context, eventId string) (*internal.EventEntry, error) {

	eventKeyMap := getVersionedEventStorageKeyMap(eventId)
	outputItem, err := e.versionedEventTable.GetItem(ctx, eventKeyMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to get versioned event: %w", err)
	}
	eventDDBEntry, ok := castVersionedItemToDDBEntry(outputItem)
	if !ok {
		return nil, xerrors.Errorf("failed to cast versioned item to EventDDBEntry: %v", outputItem)
	}
	return model.IntoEventEntry(eventDDBEntry), nil
}

func makeWatermarkDDBEntry(eventTag uint32, maxEventId int64) *model.EventDDBEntry {
	return &model.EventDDBEntry{
		EventId:     pkeyValueForWatermark,
		EventType:   api.BlockchainEvent_UNKNOWN,
		BlockHeight: blockHeightForWatermark,
		BlockHash:   "",
		MaxEventId:  maxEventId,
		EventTag:    eventTag,
	}
}

func makeWatermarkVersionedDDBEntry(eventTag uint32, eventId int64) *model.VersionedEventDDBEntry {
	return &model.VersionedEventDDBEntry{
		EventId:      getEventIdForWatermark(eventTag),
		Sequence:     eventId,
		BlockId:      getBlockIdForWatermark(eventTag), // block_id cannot be empty string
		BlockHeight:  blockHeightForWatermark,
		BlockHash:    "",
		EventType:    api.BlockchainEvent_UNKNOWN,
		Tag:          0,
		ParentHash:   "",
		BlockSkipped: false,
		EventTag:     eventTag,
	}
}

func (e *eventStorageImpl) AddEvents(ctx context.Context, eventTag uint32, events []*internal.BlockEvent) error {
	if eventTag > e.latestEventTag {
		return xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}

	maxEventId, err := e.GetMaxEventId(ctx, eventTag)
	var startEventId int64
	if err != nil {
		if !xerrors.Is(err, errors.ErrNoEventHistory) {
			return err
		}
		startEventId = internal.EventIdStartValue
	} else {
		startEventId = maxEventId + 1
	}

	eventsToAdd := model.ConvertBlockEventsToEventDDBEntries(events, eventTag, startEventId)

	return e.addEventsWithEntries(ctx, eventTag, eventsToAdd)
}

func (e *eventStorageImpl) addEventsWithEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventDDBEntry) error {
	if eventTag > e.latestEventTag {
		return xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	startEventId := eventEntries[0].EventId

	return e.instrumentAddEvents.Instrument(ctx, func(ctx context.Context) error {
		watermark := makeWatermarkDDBEntry(eventTag, eventEntries[len(eventEntries)-1].EventId)
		var eventsToValidate []*model.EventDDBEntry
		// fetch some events before startEventId
		startFetchId := startEventId - addEventsSafePadding
		if startFetchId < internal.EventIdStartValue {
			startFetchId = internal.EventIdStartValue
		}
		if startFetchId < startEventId {
			beforeEvents, err := e.GetEventsByEventIdRange(ctx, eventTag, startFetchId, startEventId)
			if err != nil {
				return xerrors.Errorf("failed to fetch events: %w", err)
			}
			eventsToValidate = append(model.FromEventEntries(beforeEvents), eventEntries...)
		} else {
			eventsToValidate = eventEntries
		}

		err := validateEvents(eventsToValidate)
		if err != nil {
			return xerrors.Errorf("events failed validation: %w", err)
		}

		if eventTag == defaultEventTag {
			itemsToWrite := make([]interface{}, len(eventEntries))
			for i, event := range eventEntries {
				itemsToWrite[i] = event
			}
			err = e.eventTable.WriteItems(ctx, itemsToWrite)
			if err != nil {
				return xerrors.Errorf("failed to write events: %w", err)
			}
			err = e.eventTable.WriteItem(ctx, watermark)
			if err != nil {
				return xerrors.Errorf("failed to update watermark: %w", err)
			}
			return nil
		} else {
			itemsToWrite := make([]interface{}, len(eventEntries))
			for i, event := range eventEntries {
				itemsToWrite[i] = castDDBEntryToVersionedDDBEntry(event)
			}
			err = e.versionedEventTable.WriteItems(ctx, itemsToWrite)
			if err != nil {
				return xerrors.Errorf("failed to write versioned events: %w", err)
			}
			versionedWatermark := castDDBEntryToVersionedDDBEntry(watermark)
			err = e.versionedEventTable.WriteItem(ctx, versionedWatermark)
			if err != nil {
				return xerrors.Errorf("failed to update versioned watermark: %w", err)
			}
			return nil
		}
	})
}

func (e *eventStorageImpl) GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*internal.EventEntry, error) {
	if eventTag > e.latestEventTag {
		return nil, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}

	var event *internal.EventEntry
	if err := e.instrumentGetEventByEventId.Instrument(ctx, func(ctx context.Context) error {
		maxEventId, err := e.GetMaxEventId(ctx, eventTag)
		if err != nil {
			if xerrors.Is(err, errors.ErrNoEventHistory) {
				return errors.ErrNoMaxEventIdFound
			}
			return xerrors.Errorf("failed to get max event id for eventTag=%d: %w", eventTag, err)
		}
		if eventId > maxEventId {
			return xerrors.Errorf("invalid eventId %d (event ends at %d) for eventTag=%d: %w", eventId, maxEventId, eventTag, errors.ErrInvalidEventId)
		}
		events, err := e.GetEventsByEventIdRange(ctx, eventTag, eventId, eventId+1)
		if err != nil {
			return xerrors.Errorf("failed to get events for eventTag=%d, eventId=%d: %w", eventTag, eventId, err)
		}
		if len(events) != 1 {
			return xerrors.Errorf("got %d events for eventTag=%d, eventId=%d", len(events), eventTag, eventId)
		}

		event = events[0]
		return nil
	}); err != nil {
		return nil, err
	}
	return event, nil
}

func (e *eventStorageImpl) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*internal.EventEntry, error) {
	if minEventId < internal.EventIdStartValue {
		return nil, xerrors.Errorf("invalid minEventId %d (event starts at %d): %w", minEventId, internal.EventIdStartValue, errors.ErrInvalidEventId)
	}

	if eventTag > e.latestEventTag {
		return nil, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}

	var ddbEvents []*model.EventDDBEntry
	inputKeys := make([]StringMap, 0, maxEventId-minEventId)
	if err := e.instrumentGetEventsByEventIdRange.Instrument(ctx, func(ctx context.Context) error {
		if eventTag == defaultEventTag {
			for i := minEventId; i < maxEventId; i++ {
				eventKeyMap := getEventStorageKeyMap(i)
				inputKeys = append(inputKeys, eventKeyMap)
			}
			outputItems, err := e.eventTable.GetItems(ctx, inputKeys)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return xerrors.Errorf("miss events for range [%d, %d): %w", minEventId, maxEventId, err)
				}
				return xerrors.Errorf("failed to get events: %w", err)
			}
			ddbEvents = make([]*model.EventDDBEntry, 0, len(outputItems))
			for _, outputItem := range outputItems {
				event, ok := model.CastItemToDDBEntry(outputItem)
				if !ok {
					return xerrors.Errorf("failed to cast to EventDDBEntry: %v", outputItem)
				}
				ddbEvents = append(ddbEvents, event)
			}
			return nil
		} else {
			for i := minEventId; i < maxEventId; i++ {
				eventId := getEventIdForEventSequence(eventTag, i)
				eventKeyMap := getVersionedEventStorageKeyMap(eventId)
				inputKeys = append(inputKeys, eventKeyMap)
			}
			outputItems, err := e.versionedEventTable.GetItems(ctx, inputKeys)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return xerrors.Errorf("miss versioned events (eventTag=%d) for range [%d, %d): %w", eventTag, minEventId, maxEventId, err)
				}
				return xerrors.Errorf("failed to get versioned events: %w", err)
			}
			ddbEvents = make([]*model.EventDDBEntry, 0, len(outputItems))
			for _, outputItem := range outputItems {
				event, ok := castVersionedItemToDDBEntry(outputItem)
				if !ok {
					return xerrors.Errorf("failed to cast versioned item to EventDDBEntry: %v", outputItem)
				}
				ddbEvents = append(ddbEvents, event)
			}
			return nil
		}
	}); err != nil {
		return nil, err
	}
	return model.IntoEventEntries(ddbEvents), nil
}

func (e *eventStorageImpl) GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*internal.EventEntry, error) {
	if eventTag > e.latestEventTag {
		return nil, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}

	var ddbEvents []*model.EventDDBEntry
	if err := e.instrumentGetEventsAfterEventId.Instrument(ctx, func(ctx context.Context) error {
		maxEventId, err := e.GetMaxEventId(ctx, eventTag)
		if err != nil {
			if xerrors.Is(err, errors.ErrNoEventHistory) {
				return errors.ErrNoMaxEventIdFound
			}
			return xerrors.Errorf("failed to get max event id for eventTag=%d: %w", eventTag, err)
		}
		if maxEventId == eventId {
			return nil
		}
		if eventId > maxEventId {
			return xerrors.Errorf("invalid eventId %d (event ends at %d) for eventTag=%d: %w", eventId, maxEventId, eventTag, errors.ErrInvalidEventId)
		}
		if eventId+int64(maxEvents) < maxEventId {
			maxEventId = eventId + int64(maxEvents)
		}
		events, err := e.GetEventsByEventIdRange(ctx, eventTag, eventId+1, maxEventId+1)
		ddbEvents = make([]*model.EventDDBEntry, len(events))
		for i := range ddbEvents {
			ddbEvents[i] = model.FromEventEntry(events[i])
		}
		return err
	}); err != nil {
		return nil, err
	}
	err := validateEvents(ddbEvents)
	if err != nil {
		return nil, xerrors.Errorf("events failed validation for eventTag=%d: %w", eventTag, err)
	}
	return model.IntoEventEntries(ddbEvents), nil
}

func validateEvents(ddbEvents []*model.EventDDBEntry) error {
	// check if event ids are continuous
	for i, event := range ddbEvents {
		if i > 0 {
			if event.EventId != ddbEvents[i-1].EventId+1 {
				return xerrors.Errorf("events are not continuous: prev event id: %d, current event id: %d", ddbEvents[i-1].EventId, event.EventId)
			}
		}
	}
	// check if we can prepend events to an event-chain adaptor to make sure it can construct a continuous chain
	eventsToChainAdaptor := internal.NewEventsToChainAdaptor()
	return eventsToChainAdaptor.AppendEvents(model.IntoEventEntries(ddbEvents))
}

func (e *eventStorageImpl) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	var maxEventId int64

	if eventTag > e.latestEventTag {
		return maxEventId, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	err := e.instrumentGetMaxEventId.Instrument(ctx, func(ctx context.Context) error {
		if eventTag == defaultEventTag {
			ddbEntry, err := e.getEventByKey(ctx, pkeyValueForWatermark)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return errors.ErrNoEventHistory
				}
				return err
			}
			// this scenario happens when we soft delete max event id to repopulate events table
			if ddbEntry.MaxEventId == internal.EventIdDeleted {
				return errors.ErrNoEventHistory
			}
			maxEventId = ddbEntry.MaxEventId
			return nil
		} else {
			watermarkEventId := getEventIdForWatermark(eventTag)
			ddbEntry, err := e.getVersionedEventByKey(ctx, watermarkEventId)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return errors.ErrNoEventHistory
				}
				return err
			}
			// this scenario happens when we soft delete max event id to repopulate events table
			if ddbEntry.MaxEventId == internal.EventIdDeleted {
				return errors.ErrNoEventHistory
			}
			maxEventId = ddbEntry.MaxEventId
			return nil
		}
	})

	return maxEventId, err
}

func (e *eventStorageImpl) SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error {
	if eventTag > e.latestEventTag {
		return xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}

	if maxEventId < internal.EventIdStartValue && maxEventId != internal.EventIdDeleted {
		return xerrors.Errorf("invalid max event id: %d", maxEventId)
	}
	err := e.instrumentSetMaxEventId.Instrument(ctx, func(ctx context.Context) error {
		currentMaxEventId, err := e.GetMaxEventId(ctx, eventTag)
		if err != nil {
			return xerrors.Errorf("failed to get current max event id for eventTag=%d: %w", eventTag, err)
		}
		if maxEventId > currentMaxEventId {
			return xerrors.Errorf("can not set max event id to be %d, which is bigger than current max event id: %d", maxEventId, currentMaxEventId)
		}

		if eventTag == defaultEventTag {
			watermark := makeWatermarkDDBEntry(eventTag, maxEventId)
			err = e.eventTable.WriteItem(ctx, watermark)
			if err != nil {
				return xerrors.Errorf("failed to update watermark for eventTag=%d: %w", eventTag, err)
			}
			return nil
		} else {
			watermark := makeWatermarkVersionedDDBEntry(eventTag, maxEventId)
			err = e.versionedEventTable.WriteItem(ctx, watermark)
			if err != nil {
				return xerrors.Errorf("failed to update watermark for eventTag=%d: %w", eventTag, err)
			}
			return nil
		}
	})
	return err
}

func (e *eventStorageImpl) getEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*internal.EventEntry, error) {
	if eventTag > e.latestEventTag {
		return nil, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	var events []*internal.EventEntry
	if eventTag == defaultEventTag {
		outputItems, err := e.eventTable.QueryItems(ctx, e.heightIndexName, fmt.Sprintf("%s = %s", heightKeyName, heightValueName),
			map[string]*dynamodb.AttributeValue{
				heightValueName: {
					N: aws.String(fmt.Sprintf("%d", blockHeight)),
				},
			})

		if err != nil {
			return nil, xerrors.Errorf("failed to query events by height (%v): %w", blockHeight, err)
		}
		if len(outputItems) == 0 {
			return nil, errors.ErrItemNotFound
		}
		for _, outputItem := range outputItems {
			eventDDBEntry, ok := model.CastItemToDDBEntry(outputItem)
			if !ok {
				return nil, xerrors.Errorf("failed to cast to EventDDBEntry: %v", outputItem)
			}
			if eventDDBEntry.EventId == pkeyValueForWatermark {
				// does not count since it is the watermark entry
				continue
			}
			events = append(events, model.IntoEventEntry(eventDDBEntry))
		}
	} else {
		outputItems, err := e.versionedEventTable.QueryItems(ctx, e.versionedEventBlockIndexName, fmt.Sprintf("%s = %s", blockIdKeyName, blockIdValueName),
			map[string]*dynamodb.AttributeValue{
				blockIdValueName: {
					S: aws.String(fmt.Sprintf("%d-%d", eventTag, blockHeight)),
				},
			})
		if err != nil {
			return nil, xerrors.Errorf("failed to query events for (eventTag=%v, height=%v): %w", eventTag, blockHeight, err)
		}
		if len(outputItems) == 0 {
			return nil, errors.ErrItemNotFound
		}
		for _, outputItem := range outputItems {
			eventDDBEntry, ok := castVersionedItemToDDBEntry(outputItem)
			if !ok {
				return nil, xerrors.Errorf("failed to cast versioned item to EventDDBEntry: %v", outputItem)
			}
			if eventDDBEntry.EventId == pkeyValueForWatermark {
				// does not count since it is the watermark entry
				continue
			}
			events = append(events, model.IntoEventEntry(eventDDBEntry))
		}
	}
	return events, nil
}

func (e *eventStorageImpl) GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error) {
	eventId := int64(math.MaxInt64)
	if eventTag > e.latestEventTag {
		return eventId, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	err := e.instrumentGetFirstEventIdByBlockHeight.Instrument(ctx, func(ctx context.Context) error {
		events, err := e.getEventsByBlockHeight(ctx, eventTag, blockHeight)
		if err != nil {
			return xerrors.Errorf("failed to get events for eventTag=%v, blockHeight=%v: %w", eventTag, blockHeight, err)
		}
		for _, event := range events {
			if event.EventId < eventId {
				eventId = event.EventId
			}
		}
		return nil
	})
	return eventId, err
}

func (e *eventStorageImpl) GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*internal.EventEntry, error) {
	if eventTag > e.latestEventTag {
		return nil, xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	var events []*internal.EventEntry
	if err := e.instrumentGetEventsByBlockHeight.Instrument(ctx, func(ctx context.Context) error {
		var err error
		events, err = e.getEventsByBlockHeight(ctx, eventTag, blockHeight)
		if err != nil {
			return xerrors.Errorf("failed to get events for eventTag=%v, blockHeight=%v: %w", eventTag, blockHeight, err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return events, nil
}

func getEventIdForWatermark(eventTag uint32) string {
	return fmt.Sprintf(versionedIdFormat, eventTag, versionedEventWatermark)
}

func getEventIdForEventSequence(eventTag uint32, sequence int64) string {
	return fmt.Sprintf(versionedIdFormat, eventTag, sequence)
}

func getBlockIdForWatermark(eventTag uint32) string {
	return fmt.Sprintf(versionedIdFormat, eventTag, versionedEventWatermark)
}

func getBlockIdForHeight(eventTag uint32, height uint64) string {
	return fmt.Sprintf(versionedIdFormat, eventTag, height)
}

func castDDBEntryToVersionedDDBEntry(eventDDBEntry *model.EventDDBEntry) *model.VersionedEventDDBEntry {
	eventTag := eventDDBEntry.EventTag
	if eventDDBEntry.EventId == pkeyValueForWatermark {
		return makeWatermarkVersionedDDBEntry(eventTag, eventDDBEntry.MaxEventId)
	}

	eventId := getEventIdForEventSequence(eventTag, eventDDBEntry.EventId)
	blockId := getBlockIdForHeight(eventTag, eventDDBEntry.BlockHeight)
	return &model.VersionedEventDDBEntry{
		EventId:        eventId,
		Sequence:       eventDDBEntry.EventId,
		BlockId:        blockId,
		BlockHeight:    eventDDBEntry.BlockHeight,
		BlockHash:      eventDDBEntry.BlockHash,
		EventType:      eventDDBEntry.EventType,
		Tag:            eventDDBEntry.Tag,
		ParentHash:     eventDDBEntry.ParentHash,
		BlockSkipped:   eventDDBEntry.BlockSkipped,
		EventTag:       eventTag,
		BlockTimestamp: eventDDBEntry.BlockTimestamp,
	}
}

func castVersionedItemToDDBEntry(outputItem interface{}) (*model.EventDDBEntry, bool) {
	versionedEventDDBEntry, ok := model.CastVersionedItemToVersionedDDBEntry(outputItem)
	if !ok {
		return nil, ok
	}

	// update eventId to -1 if this is for watermark event
	eventId := versionedEventDDBEntry.Sequence
	maxEventId := int64(0)
	if strings.Contains(versionedEventDDBEntry.EventId, versionedEventWatermark) {
		eventId = pkeyValueForWatermark
		maxEventId = versionedEventDDBEntry.Sequence
	}
	eventDDBEntry := &model.EventDDBEntry{
		EventId:        eventId,
		EventType:      versionedEventDDBEntry.EventType,
		BlockHeight:    versionedEventDDBEntry.BlockHeight,
		BlockHash:      versionedEventDDBEntry.BlockHash,
		Tag:            versionedEventDDBEntry.Tag,
		ParentHash:     versionedEventDDBEntry.ParentHash,
		MaxEventId:     maxEventId,
		BlockSkipped:   versionedEventDDBEntry.BlockSkipped,
		EventTag:       versionedEventDDBEntry.EventTag,
		BlockTimestamp: versionedEventDDBEntry.BlockTimestamp,
	}

	return eventDDBEntry, true
}
