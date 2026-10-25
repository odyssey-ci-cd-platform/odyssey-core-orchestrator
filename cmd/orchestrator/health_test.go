package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/odyssey-ci-cd-platform/odyssey-core-orchestrator/internal/orchestrator"
)

// Requirement trace: /healthz reports the three signals an operator needs
// for the one stateful component — process up (the endpoint answering),
// the single-instance lease held, and each loop's last error (issue
// gh-24; AUD-013).

func TestHealthzOKWhenLeaseHeld(t *testing.T) {
	status := &orchestrator.LoopStatus{}
	h := &Health{
		LeaseLost: func() bool { return false },
		Loops:     map[string]*orchestrator.LoopStatus{"pipeline_queue": status},
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with the lease held", rec.Code)
	}
	var body struct {
		Status    string `json:"status"`
		LeaseHeld bool   `json:"lease_held"`
		Loops     map[string]struct {
			LastError string `json:"last_error"`
			At        string `json:"at"`
		} `json:"loops"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body %q: %v", rec.Body.String(), err)
	}
	if body.Status != "ok" || !body.LeaseHeld {
		t.Errorf("body = %+v, want status ok with the lease held", body)
	}
	loop, ok := body.Loops["pipeline_queue"]
	if !ok {
		t.Fatalf("body has no pipeline_queue loop entry: %v", body.Loops)
	}
	if loop.LastError != "" {
		t.Errorf("last_error = %q with no error recorded, want empty", loop.LastError)
	}
}

func TestHealthzDegradedWhenLeaseLost(t *testing.T) {
	h := &Health{
		LeaseLost: func() bool { return true },
		Loops:     map[string]*orchestrator.LoopStatus{},
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 with the lease lost", rec.Code)
	}
	var body struct {
		Status    string `json:"status"`
		LeaseHeld bool   `json:"lease_held"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body %q: %v", rec.Body.String(), err)
	}
	if body.Status != "degraded" || body.LeaseHeld {
		t.Errorf("body = %+v, want degraded with the lease lost", body)
	}
}

func TestHealthzCarriesLoopErrors(t *testing.T) {
	status := &orchestrator.LoopStatus{}
	status.Record(errString("reading pipeline queue: MINIREDIS-DOWN"))
	h := &Health{
		LeaseLost: func() bool { return false },
		Loops:     map[string]*orchestrator.LoopStatus{"pipeline_queue": status},
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	var body struct {
		Loops map[string]struct {
			LastError string `json:"last_error"`
			At        string `json:"at"`
		} `json:"loops"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding body %q: %v", rec.Body.String(), err)
	}
	loop, ok := body.Loops["pipeline_queue"]
	if !ok || loop.LastError != "reading pipeline queue: MINIREDIS-DOWN" {
		t.Fatalf("loop entry = %+v (present: %v), want the recorded last error", loop, ok)
	}
	if _, err := time.Parse(time.RFC3339, loop.At); err != nil {
		t.Errorf("at = %q, want an RFC3339 timestamp", loop.At)
	}
}

// errString satisfies error without importing errors at every use.
type errString string

func (e errString) Error() string { return string(e) }
