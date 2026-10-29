package haapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// countingServer answers every request with status and records how many
// requests of each method it saw.
func countingServer(t *testing.T, status int) (*Client, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, "token"), &calls
}

// TestPostNotRetriedOn5xx is the defect this file exists for.
//
// doWithRetry retried on any 5xx with no method check, so a service call that
// Home Assistant executed and then failed to report on was sent three times.
// `hactl svc call notify.mobile_app --confirm` against an HA that delivers the
// push and *then* raises produced three notifications; the same shape applies
// to lock.unlock, cover.open_cover and counter.increment.
//
// The rule is INVARIANTS.md H-1, which was enforced only against the companion
// client while this one — the one that actually talks to Home Assistant — was
// never checked.
func TestPostNotRetriedOn5xx(t *testing.T) {
	client, calls := countingServer(t, http.StatusInternalServerError)

	if _, err := client.CallService(context.Background(), "notify", "mobile_app", map[string]any{"message": "hi"}); err == nil {
		t.Fatal("a 500 from HA must surface as an error")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("CallService issued %d POSTs against a 5xx; want exactly 1 — HA may already have run the service", got)
	}
}

// TestNonIdempotentWritesAreIssuedOnce covers the rest of the POST surface that
// routed through the same helper.
func TestNonIdempotentWritesAreIssuedOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*Client) error
	}{
		{"CallService", func(c *Client) error {
			_, err := c.CallService(context.Background(), "light", "turn_on", nil)
			return err
		}},
		{"CallServiceWithResponse", func(c *Client) error {
			_, err := c.CallServiceWithResponse(context.Background(), "calendar", "get_events", nil)
			return err
		}},
		{"RenderTemplate", func(c *Client) error {
			_, err := c.RenderTemplate(context.Background(), "{{ 1 }}")
			return err
		}},
		{"StartConfigFlow", func(c *Client) error {
			_, err := c.StartConfigFlow(context.Background(), "mqtt")
			return err
		}},
		{"StartOptionsFlow", func(c *Client) error {
			_, err := c.StartOptionsFlow(context.Background(), "entry1")
			return err
		}},
		{"StepFlow", func(c *Client) error {
			_, err := c.StepFlow(context.Background(), "flow1", false, json.RawMessage(`{"k":"v"}`))
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, calls := countingServer(t, http.StatusInternalServerError)
			if err := tc.call(client); err == nil {
				t.Fatal("a 500 must surface as an error")
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("%s issued %d requests against a 5xx; want exactly 1", tc.name, got)
			}
		})
	}
}

// TestGetStillRetriedOn5xx is the negative control. Without it, deleting the
// retry entirely would satisfy the test above.
func TestGetStillRetriedOn5xx(t *testing.T) {
	client, calls := countingServer(t, http.StatusInternalServerError)

	if _, err := client.GetStates(context.Background()); err == nil {
		t.Fatal("a 500 from HA must surface as an error")
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("GetStates issued %d requests against a 5xx; want 3 — a read is safe to repeat and the retry is worth having", got)
	}
}

// TestRetriesShareTheCallerTimeoutBudget — finding #155.
//
// Every attempt used to start a fresh per-request deadline, so a 5xx the
// server answers slowly was retried with the full --timeout again: a GET
// behind a proxy that 504s after 90s took 272s at --timeout 180s, while the
// same call at the 30s default stopped at 30.02s. D-34 already decided the
// caller's bound covers "every connection, including a retry the caller did
// not ask for", and WSClient.Connect runs its retry inside the budget — this
// transport now does the same.
func TestRetriesShareTheCallerTimeoutBudget(t *testing.T) {
	withTimeout(t, 500*time.Millisecond)

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		// Slow enough to eat most of the 500ms budget, then a 5xx — the shape
		// that used to buy every attempt a fresh full deadline.
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	start := time.Now()
	_, err := New(srv.URL, "token").GetStates(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a 502 from HA must surface as an error")
	}
	// Three 400ms attempts plus the two backoff sleeps used to add up to ~2.7s.
	// The margin is generous on purpose: this must fail for the reported reason
	// (a fresh budget per attempt) and never for a slow machine.
	if elapsed > 2*time.Second {
		t.Errorf("the call took %v at --timeout 500ms: a retry was handed a fresh full budget", elapsed)
	}
	if got := calls.Load(); got > 2 {
		t.Errorf("the server saw %d requests at --timeout 500ms: a retry started with no budget left", got)
	}
}

// TestRetryGetsOnlyTheRemainingBudget — the second half of D-34's posture: a
// retry that does start gets only what is left of the caller's budget, not a
// fresh full one.
func TestRetryGetsOnlyTheRemainingBudget(t *testing.T) {
	withTimeout(t, 1200*time.Millisecond)

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway) // fails fast, leaving budget
			return
		}
		// The retry answers 502 after a delay that fits inside one fresh
		// 1.2s budget but exceeds the ~700ms that remain of the caller's.
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)

	start := time.Now()
	_, err := New(srv.URL, "token").GetStates(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a 502 from HA must surface as an error")
	}
	// The first attempt fails instantly, the 500ms backoff spends a third of
	// the budget, and the second attempt gets only the ~700ms that remain. The
	// server answers after 1s, so the whole call must end at the 1.2s the
	// caller allowed — not at ~3.5s, what two fresh budgets plus backoffs used
	// to add up to.
	if elapsed > 2*time.Second {
		t.Errorf("the call took %v at --timeout 1.2s: the retry ran on a fresh full budget", elapsed)
	}
	if got := calls.Load(); got > 2 {
		t.Errorf("the server saw %d requests at --timeout 1.2s: the retry ran on a fresh full budget", got)
	}
}
