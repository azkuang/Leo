package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// cmdWatch follows the SSE event stream.
//
// This is the terminal equivalent of the fleet view: during a demo it is the
// running commentary on what the scheduler just decided and why.
func cmdWatch(ctx context.Context, server string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(server, "/")+"/events", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

	// No client timeout: this connection is meant to stay open.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	type event struct {
		Kind string    `json:"kind"`
		At   time.Time `json:"at"`
		Text string    `json:"text"`
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var e event
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e); err != nil {
			continue
		}
		fmt.Printf("%s  %-18s %s\n", e.At.Format("15:04:05"), e.Kind, e.Text)
	}
	return scanner.Err()
}
