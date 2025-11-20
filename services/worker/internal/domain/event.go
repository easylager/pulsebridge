package domain

import "encoding/json"

type Event struct {
	StreamID      string
	EventID       string
	TenantID      string
	EventType     string
	PayloadJSON   string
	OccurredAt    string
	CorrelationID string
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
