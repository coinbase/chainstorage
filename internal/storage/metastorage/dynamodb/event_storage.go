package dynamodb

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	ddbmodel "github.com/coinbase/chainstorage/internal/storage/metastorage/dynamodb/model"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/internal"
	"github.com/coinbase/chainstorage/internal/storage/metastorage/model"
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
	EventIdStartValue       = int64(1)
	EventIdDeleted          = int64(0)
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

func newEventStorage(params Params) (internal.EventStorage, error) {
	var eventTable ddbTable
	var err error

	heightIndexName := params.Config.AWS.DynamoDB.EventTableHeightIndex
	if params.Config.AWS.DynamoDB.EventTable != "" {
		eventTable, err = createEventTable(params)
		if err != nil {
			return nil, xerrors.Errorf("failed to create event table: %w", err)
		}
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
		reflect.TypeOf(ddbmodel.EventDDBEntry{}),
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
		reflect.TypeOf(ddbmodel.VersionedEventDDBEntry{}),
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

func (e *eventStorageImpl) getEventByKey(
	ctx context.Context, eventId int64) (*model.EventEntry, error) {

	eventKeyMap := getEventStorageKeyMap(eventId)
	outputItem, err := e.eventTable.GetItem(ctx, eventKeyMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to get event: %w", err)
	}
	eventEntry, ok := castItemToEventEntry(outputItem)
	if !ok {
		return nil, xerrors.Errorf("failed to cast to EventDDBEntry: %v", outputItem)
	}
	return eventEntry, nil
}

func (e *eventStorageImpl) getVersionedEventByKey(
	ctx context.Context, eventId string) (*model.EventEntry, error) {

	eventKeyMap := getVersionedEventStorageKeyMap(eventId)
	outputItem, err := e.versionedEventTable.GetItem(ctx, eventKeyMap)
	if err != nil {
		return nil, xerrors.Errorf("failed to get versioned event: %w", err)
	}
	eventEntry, ok := castVersionedItemToEventEntry(outputItem)
	if !ok {
		return nil, xerrors.Errorf("failed to cast versioned item to EventDDBEntry: %v", outputItem)
	}
	return eventEntry, nil
}

func makeWatermarkDDBEntry(eventTag uint32, maxEventId int64) *ddbmodel.EventDDBEntry {
	return &ddbmodel.EventDDBEntry{
		EventId:     pkeyValueForWatermark,
		EventType:   api.BlockchainEvent_UNKNOWN,
		BlockHeight: blockHeightForWatermark,
		BlockHash:   "",
		MaxEventId:  maxEventId,
		EventTag:    eventTag,
	}
}

func makeWatermarkVersionedDDBEntry(eventTag uint32, eventId int64) *ddbmodel.VersionedEventDDBEntry {
	return &ddbmodel.VersionedEventDDBEntry{
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

func convertBlockEventsToEventEntries(blockEvents []*model.BlockEvent, eventTag uint32, eventId int64) []*model.EventEntry {
	if len(blockEvents) == 0 {
		return []*model.EventEntry{}
	}
	eventEntries := make([]*model.EventEntry, len(blockEvents))
	for i, inputEvent := range blockEvents {
		event := model.NewEventEntry(eventTag, eventId, inputEvent)
		eventEntries[i] = event
		eventId += 1
	}

	return eventEntries
}

func (e *eventStorageImpl) AddEvents(ctx context.Context, eventTag uint32, events []*model.BlockEvent) error {
	if err := e.validateEventTag(eventTag); err != nil {
		return err
	}

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

	eventsToAdd := convertBlockEventsToEventEntries(events, eventTag, startEventId)

	return e.AddEventEntries(ctx, eventTag, eventsToAdd)
}

func (e *eventStorageImpl) AddEventEntries(ctx context.Context, eventTag uint32, eventEntries []*model.EventEntry) error {
	if err := e.validateEventTag(eventTag); err != nil {
		return err
	}
	startEventId := eventEntries[0].EventId

	return e.instrumentAddEvents.Instrument(ctx, func(ctx context.Context) error {
		watermark := makeWatermarkDDBEntry(eventTag, eventEntries[len(eventEntries)-1].EventId)
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

		err := e.validateEvents(eventsToValidate)
		if err != nil {
			return xerrors.Errorf("events failed validation: %w", err)
		}

		if eventTag == defaultEventTag {
			itemsToWrite := make([]any, len(eventEntries))
			for i, event := range eventEntries {
				itemsToWrite[i] = (*ddbmodel.EventDDBEntry)(event)
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
			itemsToWrite := make([]any, len(eventEntries))
			for i, event := range eventEntries {
				itemsToWrite[i] = castEventEntryToVersionedDDBEntry(event)
			}
			err = e.versionedEventTable.WriteItems(ctx, itemsToWrite)
			if err != nil {
				return xerrors.Errorf("failed to write versioned events: %w", err)
			}
			versionedWatermark := castEventEntryToVersionedDDBEntry((*model.EventEntry)(watermark))
			err = e.versionedEventTable.WriteItem(ctx, versionedWatermark)
			if err != nil {
				return xerrors.Errorf("failed to update versioned watermark: %w", err)
			}
			return nil
		}
	})
}

func (e *eventStorageImpl) GetEventByEventId(ctx context.Context, eventTag uint32, eventId int64) (*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}

	return e.instrumentGetEventByEventId.Instrument(ctx, func(ctx context.Context) (*model.EventEntry, error) {
		maxEventId, err := e.GetMaxEventId(ctx, eventTag)
		if err != nil {
			if xerrors.Is(err, errors.ErrNoEventHistory) {
				return nil, errors.ErrNoMaxEventIdFound
			}
			return nil, xerrors.Errorf("failed to get max event id for eventTag=%d: %w", eventTag, err)
		}
		if eventId > maxEventId {
			return nil, xerrors.Errorf("invalid eventId %d (event ends at %d) for eventTag=%d: %w", eventId, maxEventId, eventTag, errors.ErrInvalidEventId)
		}
		events, err := e.GetEventsByEventIdRange(ctx, eventTag, eventId, eventId+1)
		if err != nil {
			return nil, xerrors.Errorf("failed to get events for eventTag=%d, eventId=%d: %w", eventTag, eventId, err)
		}
		if len(events) != 1 {
			return nil, xerrors.Errorf("got %d events for eventTag=%d, eventId=%d", len(events), eventTag, eventId)
		}

		event := events[0]
		return event, nil
	})
}

func (e *eventStorageImpl) GetEventsByEventIdRange(ctx context.Context, eventTag uint32, minEventId int64, maxEventId int64) ([]*model.EventEntry, error) {
	if minEventId < model.EventIdStartValue {
		return nil, xerrors.Errorf("invalid minEventId %d (event starts at %d): %w", minEventId, model.EventIdStartValue, errors.ErrInvalidEventId)
	}

	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}

	inputKeys := make([]StringMap, 0, maxEventId-minEventId)
	return e.instrumentGetEventsByEventIdRange.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		if eventTag == defaultEventTag {
			for i := minEventId; i < maxEventId; i++ {
				eventKeyMap := getEventStorageKeyMap(i)
				inputKeys = append(inputKeys, eventKeyMap)
			}
			outputItems, err := e.eventTable.GetItems(ctx, inputKeys)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return nil, xerrors.Errorf("miss events for range [%d, %d): %w", minEventId, maxEventId, err)
				}
				return nil, xerrors.Errorf("failed to get events: %w", err)
			}
			events := make([]*model.EventEntry, 0, len(outputItems))
			for _, outputItem := range outputItems {
				event, ok := castItemToEventEntry(outputItem)
				if !ok {
					return nil, xerrors.Errorf("failed to cast to EventDDBEntry: %v", outputItem)
				}
				events = append(events, event)
			}
			return events, nil
		} else {
			for i := minEventId; i < maxEventId; i++ {
				eventId := getEventIdForEventSequence(eventTag, i)
				eventKeyMap := getVersionedEventStorageKeyMap(eventId)
				inputKeys = append(inputKeys, eventKeyMap)
			}
			outputItems, err := e.versionedEventTable.GetItems(ctx, inputKeys)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return nil, xerrors.Errorf("miss versioned events (eventTag=%d) for range [%d, %d): %w", eventTag, minEventId, maxEventId, err)
				}
				return nil, xerrors.Errorf("failed to get versioned events: %w", err)
			}
			events := make([]*model.EventEntry, 0, len(outputItems))
			for _, outputItem := range outputItems {
				event, ok := castVersionedItemToEventEntry(outputItem)
				if !ok {
					return nil, xerrors.Errorf("failed to cast versioned item to EventDDBEntry: %v", outputItem)
				}
				events = append(events, event)
			}
			return events, nil
		}
	})
}

func (e *eventStorageImpl) GetEventsAfterEventId(ctx context.Context, eventTag uint32, eventId int64, maxEvents uint64) ([]*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}

	events, err := e.instrumentGetEventsAfterEventId.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		maxEventId, err := e.GetMaxEventId(ctx, eventTag)
		if err != nil {
			if xerrors.Is(err, errors.ErrNoEventHistory) {
				return nil, errors.ErrNoMaxEventIdFound
			}
			return nil, xerrors.Errorf("failed to get max event id for eventTag=%d: %w", eventTag, err)
		}
		if maxEventId == eventId {
			return nil, nil
		}
		if eventId > maxEventId {
			return nil, xerrors.Errorf("invalid eventId %d (event ends at %d) for eventTag=%d: %w", eventId, maxEventId, eventTag, errors.ErrInvalidEventId)
		}
		if eventId+int64(maxEvents) < maxEventId {
			maxEventId = eventId + int64(maxEvents)
		}
		return e.GetEventsByEventIdRange(ctx, eventTag, eventId+1, maxEventId+1)
	})
	if err != nil {
		return nil, err
	}

	if err := e.validateEvents(events); err != nil {
		return nil, xerrors.Errorf("events failed validation for eventTag=%d: %w", eventTag, err)
	}
	return events, nil
}

func (e *eventStorageImpl) validateEvents(events []*model.EventEntry) error {
	// check if event ids are continuous
	for i, event := range events {
		if i > 0 {
			if event.EventId != events[i-1].EventId+1 {
				return xerrors.Errorf("events are not continuous: prev event id: %d, current event id: %d", events[i-1].EventId, event.EventId)
			}
		}
	}
	// check if we can prepend events to an event-chain adaptor to make sure it can construct a continuous chain
	eventsToChainAdaptor := internal.NewEventsToChainAdaptor()
	return eventsToChainAdaptor.AppendEvents(events)
}

func (e *eventStorageImpl) GetMaxEventId(ctx context.Context, eventTag uint32) (int64, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return 0, err
	}
	return e.instrumentGetMaxEventId.Instrument(ctx, func(ctx context.Context) (int64, error) {
		if eventTag == defaultEventTag {
			ddbEntry, err := e.getEventByKey(ctx, pkeyValueForWatermark)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return 0, errors.ErrNoEventHistory
				}
				return 0, err
			}
			// this scenario happens when we soft delete max event id to repopulate events table
			if ddbEntry.MaxEventId == model.EventIdDeleted {
				return 0, errors.ErrNoEventHistory
			}
			maxEventId := ddbEntry.MaxEventId
			return maxEventId, nil
		} else {
			watermarkEventId := getEventIdForWatermark(eventTag)
			ddbEntry, err := e.getVersionedEventByKey(ctx, watermarkEventId)
			if err != nil {
				if xerrors.Is(err, errors.ErrItemNotFound) {
					return 0, errors.ErrNoEventHistory
				}
				return 0, err
			}
			// this scenario happens when we soft delete max event id to repopulate events table
			if ddbEntry.MaxEventId == model.EventIdDeleted {
				return 0, errors.ErrNoEventHistory
			}
			maxEventId := ddbEntry.MaxEventId
			return maxEventId, nil
		}
	})
}

func (e *eventStorageImpl) SetMaxEventId(ctx context.Context, eventTag uint32, maxEventId int64) error {
	if err := e.validateEventTag(eventTag); err != nil {
		return err
	}

	if maxEventId < model.EventIdStartValue && maxEventId != model.EventIdDeleted {
		return xerrors.Errorf("invalid max event id: %d", maxEventId)
	}
	return e.instrumentSetMaxEventId.Instrument(ctx, func(ctx context.Context) error {
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
}

func (e *eventStorageImpl) getEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}
	var events []*model.EventEntry
	if eventTag == defaultEventTag {
		keyCondition := expression.Key(heightKeyName).Equal(expression.Value(blockHeight))
		builder := expression.NewBuilder().WithKeyCondition(keyCondition)
		expr, err := builder.Build()
		if err != nil {
			return nil, xerrors.Errorf("failed to build expression for querying events: %w", err)
		}

		outputItems, err := e.eventTable.QueryItems(ctx, &QueryItemsRequest{
			IndexName:                 e.heightIndexName,
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to query events by height (%v): %w", blockHeight, err)
		}
		if len(outputItems) == 0 {
			return nil, errors.ErrItemNotFound
		}
		for _, outputItem := range outputItems {
			eventEntry, ok := castItemToEventEntry(outputItem)
			if !ok {
				return nil, xerrors.Errorf("failed to cast to EventDDBEntry: %v", outputItem)
			}
			if eventEntry.EventId == pkeyValueForWatermark {
				// does not count since it is the watermark entry
				continue
			}
			events = append(events, eventEntry)
		}
	} else {
		keyCondition := expression.Key(blockIdKeyName).Equal(expression.Value(fmt.Sprintf("%d-%d", eventTag, blockHeight)))
		builder := expression.NewBuilder().WithKeyCondition(keyCondition)
		expr, err := builder.Build()
		if err != nil {
			return nil, xerrors.Errorf("failed to build expression for querying events: %w", err)
		}

		outputItems, err := e.versionedEventTable.QueryItems(ctx, &QueryItemsRequest{
			IndexName:                 e.versionedEventBlockIndexName,
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})
		if err != nil {
			return nil, xerrors.Errorf("failed to query events for (eventTag=%v, height=%v): %w", eventTag, blockHeight, err)
		}
		if len(outputItems) == 0 {
			return nil, errors.ErrItemNotFound
		}
		for _, outputItem := range outputItems {
			eventDDBEntry, ok := castVersionedItemToEventEntry(outputItem)
			if !ok {
				return nil, xerrors.Errorf("failed to cast versioned item to EventDDBEntry: %v", outputItem)
			}
			if eventDDBEntry.EventId == pkeyValueForWatermark {
				// does not count since it is the watermark entry
				continue
			}
			events = append(events, eventDDBEntry)
		}
	}
	return events, nil
}

func (e *eventStorageImpl) GetFirstEventIdByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) (int64, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return 0, err
	}
	return e.instrumentGetFirstEventIdByBlockHeight.Instrument(ctx, func(ctx context.Context) (int64, error) {
		eventId := int64(math.MaxInt64)
		events, err := e.getEventsByBlockHeight(ctx, eventTag, blockHeight)
		if err != nil {
			return eventId, xerrors.Errorf("failed to get events for eventTag=%v, blockHeight=%v: %w", eventTag, blockHeight, err)
		}
		for _, event := range events {
			if event.EventId < eventId {
				eventId = event.EventId
			}
		}
		return eventId, nil
	})
}

func (e *eventStorageImpl) GetEventsByBlockHeight(ctx context.Context, eventTag uint32, blockHeight uint64) ([]*model.EventEntry, error) {
	if err := e.validateEventTag(eventTag); err != nil {
		return nil, err
	}

	return e.instrumentGetEventsByBlockHeight.Instrument(ctx, func(ctx context.Context) ([]*model.EventEntry, error) {
		events, err := e.getEventsByBlockHeight(ctx, eventTag, blockHeight)
		if err != nil {
			return nil, xerrors.Errorf("failed to get events for eventTag=%v, blockHeight=%v: %w", eventTag, blockHeight, err)
		}
		return events, nil
	})
}

func (e *eventStorageImpl) validateEventTag(eventTag uint32) error {
	if eventTag > e.latestEventTag {
		return xerrors.Errorf("do not support eventTag=%d, latestEventTag=%d", eventTag, e.latestEventTag)
	}
	if eventTag == 0 && e.eventTable == nil {
		return errors.ErrNoEventHistory
	}

	return nil
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

func castEventEntryToVersionedDDBEntry(eventEntry *model.EventEntry) *ddbmodel.VersionedEventDDBEntry {
	eventTag := eventEntry.EventTag
	if eventEntry.EventId == pkeyValueForWatermark {
		return makeWatermarkVersionedDDBEntry(eventTag, eventEntry.MaxEventId)
	}

	eventId := getEventIdForEventSequence(eventTag, eventEntry.EventId)
	blockId := getBlockIdForHeight(eventTag, eventEntry.BlockHeight)
	return &ddbmodel.VersionedEventDDBEntry{
		EventId:        eventId,
		Sequence:       eventEntry.EventId,
		BlockId:        blockId,
		BlockHeight:    eventEntry.BlockHeight,
		BlockHash:      eventEntry.BlockHash,
		EventType:      eventEntry.EventType,
		Tag:            eventEntry.Tag,
		ParentHash:     eventEntry.ParentHash,
		BlockSkipped:   eventEntry.BlockSkipped,
		EventTag:       eventTag,
		BlockTimestamp: eventEntry.BlockTimestamp,
	}
}

func castItemToEventEntry(outputItem any) (*model.EventEntry, bool) {
	eventDDBEntry, ok := outputItem.(*ddbmodel.EventDDBEntry)
	if !ok {
		return nil, ok
	}
	// switch to defaultTag is not set
	if eventDDBEntry.Tag == 0 {
		eventDDBEntry.Tag = model.DefaultBlockTag
	}
	return (*model.EventEntry)(eventDDBEntry), true
}

func castVersionedItemToEventEntry(outputItem any) (*model.EventEntry, bool) {
	versionedEventDDBEntry, ok := outputItem.(*ddbmodel.VersionedEventDDBEntry)
	if !ok {
		return nil, ok
	}
	// switch to defaultTag is not set
	if versionedEventDDBEntry.Tag == 0 {
		versionedEventDDBEntry.Tag = model.DefaultBlockTag
	}

	// update eventId to -1 if this is for watermark event
	eventId := versionedEventDDBEntry.Sequence
	maxEventId := int64(0)
	if strings.Contains(versionedEventDDBEntry.EventId, versionedEventWatermark) {
		eventId = pkeyValueForWatermark
		maxEventId = versionedEventDDBEntry.Sequence
	}
	eventEntry := &model.EventEntry{
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

	return eventEntry, true
}
