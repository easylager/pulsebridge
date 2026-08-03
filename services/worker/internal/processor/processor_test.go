package processor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maksimvasilenko/pulsebridge/worker/internal/domain"
)

type fakeStore struct {
	mu       sync.Mutex
	claimed  map[string]bool
	applied  []domain.Event
	claimErr error
	applyErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{claimed: map[string]bool{}}
}

func (s *fakeStore) Claim(_ context.Context, eventID string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return false, s.claimErr
	}
	if s.claimed[eventID] {
		return false, nil
	}
	s.claimed[eventID] = true
	return true, nil
}

func (s *fakeStore) Apply(_ context.Context, event domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyErr != nil {
		return s.applyErr
	}
	s.applied = append(s.applied, event)
	return nil
}

func (s *fakeStore) Release(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.claimed, eventID)
	return nil
}

func (s *fakeStore) appliedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

func validEvent(id string) domain.Event {
	return domain.Event{
		EventID:     id,
		TenantID:    "acme",
		EventType:   "order.created",
		PayloadJSON: `{"order_id":"o-1"}`,
		Attempt:     1,
	}
}

func TestProcessAppliesValidEvent(t *testing.T) {
	store := newFakeStore()
	p := New(store, time.Hour)

	if err := p.Process(context.Background(), validEvent("evt-1")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if got := store.appliedCount(); got != 1 {
		t.Fatalf("applied = %d, want 1", got)
	}
}

// This is the property that turns at-least-once delivery into an effectively
// once side effect: a redelivered event is acknowledged without being applied
// a second time.
func TestRedeliveryIsAppliedOnlyOnce(t *testing.T) {
	store := newFakeStore()
	p := New(store, time.Hour)
	event := validEvent("evt-1")

	for i := 0; i < 3; i++ {
		if err := p.Process(context.Background(), event); err != nil {
			t.Fatalf("Process attempt %d: %v", i+1, err)
		}
	}
	if got := store.appliedCount(); got != 1 {
		t.Fatalf("applied = %d, want 1 across three deliveries", got)
	}
}

func TestConcurrentDeliveriesApplyOnce(t *testing.T) {
	store := newFakeStore()
	p := New(store, time.Hour)
	event := validEvent("evt-race")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Process(context.Background(), event)
		}()
	}
	wg.Wait()

	if got := store.appliedCount(); got != 1 {
		t.Fatalf("applied = %d, want 1 under concurrent delivery", got)
	}
}

// A failed apply must not leave the claim behind, otherwise the retry is
// mistaken for a duplicate, acked, and the event is silently dropped.
func TestFailedApplyReleasesTheClaim(t *testing.T) {
	store := newFakeStore()
	store.applyErr = errors.New("database down")
	p := New(store, time.Hour)
	event := validEvent("evt-1")

	if err := p.Process(context.Background(), event); err == nil {
		t.Fatal("expected apply failure to surface")
	}

	store.applyErr = nil
	if err := p.Process(context.Background(), event); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
	if got := store.appliedCount(); got != 1 {
		t.Fatalf("applied = %d, want 1 after a successful retry", got)
	}
}

func TestInvalidEnvelopeIsTerminal(t *testing.T) {
	p := New(newFakeStore(), time.Hour)

	cases := map[string]domain.Event{
		"missing event_id":   {TenantID: "acme", EventType: "order.created"},
		"missing tenant_id":  {EventID: "evt-1", EventType: "order.created"},
		"missing event_type": {EventID: "evt-1", TenantID: "acme"},
		"broken payload":     {EventID: "evt-1", TenantID: "acme", EventType: "t", PayloadJSON: "{oops"},
	}

	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			err := p.Process(context.Background(), event)
			if !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("err = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

func TestCancelledContextIsReported(t *testing.T) {
	p := New(newFakeStore(), time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.Process(ctx, validEvent("evt-1")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestClaimErrorIsRetryable(t *testing.T) {
	store := newFakeStore()
	store.claimErr = errors.New("redis unreachable")
	p := New(store, time.Hour)

	err := p.Process(context.Background(), validEvent("evt-1"))
	if err == nil {
		t.Fatal("expected claim failure to surface")
	}
	// Infrastructure failures must stay retryable, unlike envelope errors which
	// go straight to the dead-letter stream.
	if errors.Is(err, ErrInvalidEnvelope) {
		t.Fatal("a claim failure must not be classified as a terminal envelope error")
	}
}
