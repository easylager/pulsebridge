package domain

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrMissingField marks an envelope that can never become valid, which is what
// separates a poison message from a transient handler failure.
var ErrMissingField = errors.New("missing required field")

// Event is the worker-side view of the wire envelope produced by the gateway.
// Field names mirror contracts/events.schema.json; TraceParent and SourceStream
// are transport metadata rather than part of the business contract.
type Event struct {
	StreamID      string
	EventID       string
	TenantID      string
	EventType     string
	PayloadJSON   string
	OccurredAt    string
	CorrelationID string
	TraceParent   string
	SourceStream  string

	// Attempt is the 1-based delivery attempt for this message. Handlers can use
	// it to distinguish a first try from a redelivery, e.g. to skip an expensive
	// warm-up or to log more loudly as the retry budget runs out.
	Attempt int64
}

// Validate reports whether the envelope carries the fields every downstream
// step depends on. Errors here are terminal: no number of retries adds a
// missing tenant id.
func (e Event) Validate() error {
	switch {
	case e.EventID == "":
		return fmt.Errorf("%w: event_id", ErrMissingField)
	case e.TenantID == "":
		return fmt.Errorf("%w: tenant_id", ErrMissingField)
	case e.EventType == "":
		return fmt.Errorf("%w: event_type", ErrMissingField)
	}
	return nil
}

func (e Event) PayloadMap() (map[string]any, error) {
	var m map[string]any
	if e.PayloadJSON == "" {
		return map[string]any{}, nil
	}
	if err := json.Unmarshal([]byte(e.PayloadJSON), &m); err != nil {
		return nil, err
	}
	return m, nil
}
