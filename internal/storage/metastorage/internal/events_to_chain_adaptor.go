package internal

import (
	"container/list"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/internal/storage/internal/errors"
	api "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	EventsToChainAdaptor struct {
		eventList *list.List
	}
)

func NewEventsToChainAdaptor() *EventsToChainAdaptor {
	eventList := list.New()
	return &EventsToChainAdaptor{
		eventList: eventList,
	}
}

func castItemToEventEntry(outputItem interface{}) (*EventEntry, bool) {
	eventEntry, ok := outputItem.(*EventEntry)
	if !ok {
		return nil, ok
	}
	// switch to defaultTag is not set
	if eventEntry.Tag == 0 {
		eventEntry.Tag = DefaultBlockTag
	}
	return eventEntry, true
}

func validateChainEvents(event *EventEntry, lastEvent *EventEntry) error {
	if lastEvent.BlockHeight != event.BlockHeight-1 {
		return xerrors.Errorf("chain is not continuous because of inconsistent heights (last={%+v}, curr={%+v})", lastEvent, event)
	}

	if !event.BlockSkipped && !lastEvent.BlockSkipped && event.ParentHash != "" && lastEvent.BlockHash != event.ParentHash {
		return xerrors.Errorf("chain is not continuous because of inconsistent parent hash (last={%+v}, curr={%+v})", lastEvent, event)
	}
	return nil
}

func (e *EventsToChainAdaptor) AppendEvents(events []*EventEntry) error {
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		lastItem := e.eventList.Back()
		if lastItem != nil {
			lastEvent, ok := castItemToEventEntry(lastItem.Value)
			if !ok {
				return xerrors.Errorf("failed to cast {%+v} to *model.EventDDBEntry", lastItem.Value)
			}
			if lastEvent.EventType == event.EventType {
				if event.EventType == api.BlockchainEvent_BLOCK_ADDED {
					// chain normal growing case, +1, [+2, +3]
					err := validateChainEvents(lastEvent, event)
					if err != nil {
						return xerrors.Errorf("parent hash of later add event is expected to be the same with previous event block hash (event={%+v}, lastEvent={%+v}): %w", event, lastEvent, err)
					}
				}
				if event.EventType == api.BlockchainEvent_BLOCK_REMOVED {
					// rollback case, +1, +2, +3, [-3, -2]
					err := validateChainEvents(event, lastEvent)
					if err != nil {
						return xerrors.Errorf("parent hash of remove event is expected to be the same with later remove event block hash (event={%+v}, lastEvent={%+v}): %w", event, lastEvent, err)
					}
				}
			} else {
				if lastEvent.BlockHeight != event.BlockHeight {
					return xerrors.Errorf("expect adjacent events with different types to have the same block height (event={%+v}, lastEvent={%+v})", event, lastEvent)
				}
				if lastEvent.EventType == api.BlockchainEvent_BLOCK_REMOVED && event.EventType == api.BlockchainEvent_BLOCK_ADDED {
					// rollback case +1, +2, [+3, -3], need pop the remove and skip append
					if lastEvent.BlockHeight != event.BlockHeight || lastEvent.BlockHash != event.BlockHash {
						return xerrors.Errorf("expect event {%+v} and lastEvent {%+v} to have the same block hash/height", event, lastEvent)
					}
					e.eventList.Remove(lastItem)
					continue
				} else if event.EventType == api.BlockchainEvent_BLOCK_REMOVED && lastEvent.EventType == api.BlockchainEvent_BLOCK_ADDED {
					// rollback and regrow case +1, +2, +3, -3, [-2, +2]
					// blocks could have different hashes
				} else {
					return xerrors.Errorf("unexpect event sequence last event={%+v}, new event={%+v}", lastEvent, event)
				}
			}
		}
		e.eventList.PushBack(event)
	}
	return nil
}

func (e *EventsToChainAdaptor) PopEventForTailBlock() (*EventEntry, error) {
	headItem := e.eventList.Front()
	if headItem != nil {
		headEvent, ok := castItemToEventEntry(headItem.Value)
		if !ok {
			return nil, xerrors.Errorf("failed to cast {%+v} to *EventEntry", headItem.Value)
		}
		if headEvent.EventType == api.BlockchainEvent_BLOCK_ADDED {
			e.eventList.Remove(headItem)
			return headEvent, nil
		}
	}
	return nil, errors.ErrNoEventAvailable
}
