package domain

import (
	"errors"
	"testing"
)

func TestValidateRejectsIncompleteEnvelopes(t *testing.T) {
	cases := map[string]Event{
		"empty":              {},
		"missing event_id":   {TenantID: "acme", EventType: "order.created"},
		"missing tenant_id":  {EventID: "evt-1", EventType: "order.created"},
		"missing event_type": {EventID: "evt-1", TenantID: "acme"},
	}

	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			if err := event.Validate(); !errors.Is(err, ErrMissingField) {
				t.Fatalf("err = %v, want ErrMissingField", err)
			}
		})
	}
}

func TestValidateAcceptsCompleteEnvelope(t *testing.T) {
	event := Event{EventID: "evt-1", TenantID: "acme", EventType: "order.created"}
	if err := event.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestPayloadMapDecodesJSON(t *testing.T) {
	event := Event{PayloadJSON: `{"order_id":"o-1","total":42}`}

	payload, err := event.PayloadMap()
	if err != nil {
		t.Fatalf("PayloadMap: %v", err)
	}
	if payload["order_id"] != "o-1" {
		t.Fatalf("order_id = %v, want o-1", payload["order_id"])
	}
}

// An absent payload is a valid event, not a decode failure — the schema allows
// notification-style events that carry no body.
func TestPayloadMapTreatsEmptyPayloadAsEmptyObject(t *testing.T) {
	payload, err := Event{}.PayloadMap()
	if err != nil {
		t.Fatalf("PayloadMap: %v", err)
	}
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty", payload)
	}
}

func TestPayloadMapRejectsMalformedJSON(t *testing.T) {
	if _, err := (Event{PayloadJSON: "{oops"}).PayloadMap(); err == nil {
		t.Fatal("expected a decode error")
	}
}
