package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/alexk/orch/internal/events"
)

// SSEHandler streams control-plane events to the UI.
//
// Server-sent events rather than WebSockets: only server-to-client is needed
// here, and SSE reconnects for free -- which matters more than it sounds during
// a demo on conference wifi.
func SSEHandler(bus *events.Bus) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		// Disable proxy buffering, which otherwise holds events until a buffer
		// fills and makes a live view look frozen.
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		ch, unsubscribe := bus.Subscribe()
		defer unsubscribe()

		// Replay the backlog so a tab opened mid-demo is not staring at an
		// empty log until the next thing happens.
		for _, e := range bus.Recent() {
			writeEvent(w, e)
		}
		flusher.Flush()

		keepalive := time.NewTicker(15 * time.Second)
		defer keepalive.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				writeEvent(w, e)
				flusher.Flush()
			case <-keepalive.C:
				fmt.Fprint(w, ": keepalive\n\n")
				flusher.Flush()
			}
		}
	}
}

func writeEvent(w http.ResponseWriter, e events.Event) {
	payload, err := json.Marshal(e)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Kind, payload)
}
