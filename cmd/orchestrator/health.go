package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Health serves /healthz: the three signals an operator needs for the one
// stateful component (AUD-013) — the process being up is the endpoint
// answering at all, the lease held is the single-instance guarantee, and
// each loop's last error shows a blip the loop retried through. HTTP 200
// means the lease is held; 503 means another instance may own the queues.
type Health struct {
	// LeaseLost reports whether the single-instance lease is gone.
	LeaseLost func() bool

	// Loops maps a loop name to its last-error record.
	Loops map[string]*orchestrator.LoopStatus
}

// loopErrorJSON is one loop's health line: its last serve error, empty
// while it serves cleanly.
type loopErrorJSON struct {
	LastError string `json:"last_error"`
	At        string `json:"at,omitempty"`
}

func (h *Health) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	leaseHeld := h.LeaseLost != nil && !h.LeaseLost()

	loops := make(map[string]loopErrorJSON, len(h.Loops))
	for name, s := range h.Loops {
		err, at := s.LastError()
		// Every loop is represented, cleanly serving ones with an empty
		// error — the body self-describes what is being watched.
		entry := loopErrorJSON{LastError: err}
		if err != "" {
			entry.At = at.UTC().Format(time.RFC3339)
		}
		loops[name] = entry
	}

	status, code := "ok", http.StatusOK
	if !leaseHeld {
		status, code = "degraded", http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":     status,
		"lease_held": leaseHeld,
		"loops":      loops,
	})
}
