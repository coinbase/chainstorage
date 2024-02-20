package workflow

import (
	"encoding/json"
	"strings"

	"golang.org/x/xerrors"
)

type WorkflowIdentity int

const (
	UnknownIdentity WorkflowIdentity = iota
	BackfillerIdentity
	BenchmarkerIdentity
	MonitorIdentity
	PollerIdentity
	StreamerIdentity
	CrossValidatorIdentity
	EventBackfillerIdentity
	ReplicatorIdentity
)

var workflowIdentityToString = map[WorkflowIdentity]string{
	BackfillerIdentity:      "workflow.backfiller",
	BenchmarkerIdentity:     "workflow.benchmarker",
	MonitorIdentity:         "workflow.monitor",
	PollerIdentity:          "workflow.poller",
	StreamerIdentity:        "workflow.streamer",
	CrossValidatorIdentity:  "workflow.cross_validator",
	EventBackfillerIdentity: "workflow.event_backfiller",
	ReplicatorIdentity:      "workflow.replicator",
}

var workflowIdentities = map[string]WorkflowIdentity{
	"backfiller":       BackfillerIdentity,
	"benchmarker":      BenchmarkerIdentity,
	"monitor":          MonitorIdentity,
	"poller":           PollerIdentity,
	"streamer":         StreamerIdentity,
	"cross_validator":  CrossValidatorIdentity,
	"event_backfiller": EventBackfillerIdentity,
	"replicator":       ReplicatorIdentity,
}

func GetWorkflowIdentify(name string) WorkflowIdentity {
	return workflowIdentities[name]
}

func (w WorkflowIdentity) String() (string, error) {
	workflowIdentityString, ok := workflowIdentityToString[w]
	if !ok {
		return "", xerrors.Errorf("unsupported workflow identity: %v", w)
	}
	return workflowIdentityString, nil
}

func (w WorkflowIdentity) UnmarshalJsonStringToRequest(str string) (any, error) {
	if str == "" {
		str = "{}"
	}

	decoder := json.NewDecoder(strings.NewReader(str))
	decoder.DisallowUnknownFields()

	var err error
	switch w {
	case BackfillerIdentity:
		var req BackfillerRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case BenchmarkerIdentity:
		var req BenchmarkerRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case MonitorIdentity:
		var req MonitorRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case PollerIdentity:
		var req PollerRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case StreamerIdentity:
		var req StreamerRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case CrossValidatorIdentity:
		var req CrossValidatorRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case EventBackfillerIdentity:
		var req EventBackfillerRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	case ReplicatorIdentity:
		var req ReplicatorRequest
		if err = decoder.Decode(&req); err == nil {
			return req, nil
		}
	default:
		err = xerrors.Errorf("unsupported workflow identity: %v", w)
	}
	return nil, err
}
