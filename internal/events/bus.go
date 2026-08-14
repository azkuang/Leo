// Package events is the fan-out from the control plane to connected UIs.
//
// Server-sent events rather than WebSockets: only server-to-client is needed,
// and SSE reconnects for free.
package events

import (
	"encoding/json"
	"sync"
	"time"
)

// Kind labels an event so the UI can route it without inspecting the payload.
type Kind string

const (
	KindNodeChanged  Kind = "node.changed"
	KindSlotChanged  Kind = "slot.changed"
	KindLeaseGranted Kind = "lease.granted"
	KindLeaseEnded   Kind = "lease.ended"
	KindTaskChanged  Kind = "task.changed"
	KindJobChanged   Kind = "job.changed"
	KindPoolChanged  Kind = "pool.changed"
	KindPolicy       Kind = "policy.changed"
	KindPlacement    Kind = "placement.decided"
	KindTrigger      Kind = "trigger.fired"
	KindSchedulerLog Kind = "scheduler.log"
)

// Event is one message on the stream.
type Event struct {
	Kind Kind            `json:"kind"`
	At   time.Time       `json:"at"`
	Data json.RawMessage `json:"data,omitempty"`
	// Text is a human-readable line for the demo's activity log.
	Text string `json:"text,omitempty"`
}

// Bus fans events out to subscribers.
//
// Subscribers that fall behind lose events rather than blocking the publisher.
// A stalled browser tab must never be able to wedge the placement loop.
type Bus struct {
	mu   sync.RWMutex
	subs map[int]chan Event
	next int

	mu2    sync.Mutex
	recent []Event
}

// NewBus creates an empty bus.
func NewBus() *Bus {
	return &Bus{subs: map[int]chan Event{}}
}

// Subscribe returns a channel of events and a function to unsubscribe.
func (b *Bus) Subscribe() (<-chan Event, func()) {
	b.mu.Lock()
	id := b.next
	b.next++
	ch := make(chan Event, 256)
	b.subs[id] = ch
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		if c, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(c)
		}
		b.mu.Unlock()
	}
}

// Publish delivers an event to every current subscriber.
func (b *Bus) Publish(e Event) {
	if e.At.IsZero() {
		e.At = time.Now()
	}

	b.mu2.Lock()
	b.recent = append(b.recent, e)
	if len(b.recent) > 200 {
		b.recent = b.recent[len(b.recent)-200:]
	}
	b.mu2.Unlock()

	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subs {
		select {
		case ch <- e:
		default:
			// Subscriber is not keeping up. Dropping is correct here: the UI
			// re-reads full state on reconnect anyway.
		}
	}
}

// Emit is a convenience for publishing an event with a JSON payload.
func (b *Bus) Emit(kind Kind, text string, data any) {
	e := Event{Kind: kind, Text: text, At: time.Now()}
	if data != nil {
		if raw, err := json.Marshal(data); err == nil {
			e.Data = raw
		}
	}
	b.Publish(e)
}

// Recent returns the buffered backlog, so a UI that connects mid-demo still
// sees what just happened.
func (b *Bus) Recent() []Event {
	b.mu2.Lock()
	defer b.mu2.Unlock()
	out := make([]Event, len(b.recent))
	copy(out, b.recent)
	return out
}
