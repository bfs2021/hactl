package companion

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hemm-ems/hactl/internal/haapi"
)

func TestShouldRetry_Idempotency(t *testing.T) {
	transportErr := errors.New("boom")
	dialErr := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}

	cases := []struct {
		name   string
		err    error
		status int
		signed bool
		method string
		want   bool
	}{
		{"GET transport error retries", transportErr, 0, false, http.MethodGet, true},
		{"PUT transport error retries", transportErr, 0, false, http.MethodPut, true},
		{"DELETE transport error retries", transportErr, 0, false, http.MethodDelete, true},
		{"POST generic transport error does NOT retry", transportErr, 0, false, http.MethodPost, false},
		{"POST connection-refused retries (never sent)", dialErr, 0, false, http.MethodPost, true},
		{"GET 5xx retries", nil, 503, false, http.MethodGet, true},
		{"POST 5xx does NOT retry", nil, 503, false, http.MethodPost, false},
		{"POST signed 401 retries", nil, http.StatusUnauthorized, true, http.MethodPost, true},
		{"GET 200 does not retry", nil, 200, false, http.MethodGet, false},
		{"unsigned 401 does not retry", nil, http.StatusUnauthorized, false, http.MethodGet, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRetry(tc.err, tc.status, tc.signed, tc.method); got != tc.want {
				t.Errorf("shouldRetry(%v, %d, %v, %s) = %v, want %v", tc.err, tc.status, tc.signed, tc.method, got, tc.want)
			}
		})
	}
}

// TestPostNotRetriedOn5xx proves a non-idempotent create is issued exactly once
// even when the server 5xxs — retrying could duplicate the create.
func TestPostNotRetriedOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := New(srv.URL, "t")
	if _, err := c.CreateScriptDef(context.Background(), "probe:\n  alias: x\n"); err == nil {
		t.Fatal("expected error from 500")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("POST create issued %d times, want exactly 1 (no retry on 5xx)", got)
	}
}

// TestGetRetriedOn5xx confirms idempotent reads still retry (recovering after a
// transient 5xx).
func TestGetRetriedOn5xx(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"files":[]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "t")
	if _, err := c.ListConfigFiles(context.Background()); err != nil {
		t.Fatalf("GET should recover after one 5xx: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("GET issued %d times, want 2 (one retry)", got)
	}
}

// TestRetriesShareTheCallerTimeoutBudget — finding #155 on the companion
// transport: every retry used to start a fresh per-request deadline, so a 5xx
// the server answers slowly was retried with the full budget again. D-34
// already decided the caller's bound covers "every connection, including a
// retry the caller did not ask for", and both the HA REST client and the WS
// connect run their retries inside the budget — this one now does too.
func TestRetriesShareTheCallerTimeoutBudget(t *testing.T) {
	prev := haapi.DefaultTimeout
	haapi.DefaultTimeout = 500 * time.Millisecond
	t.Cleanup(func() { haapi.DefaultTimeout = prev })

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		// Slow enough to eat most of the 500ms budget, then a 5xx — the shape
		// that used to buy every attempt a fresh full deadline.
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	start := time.Now()
	_, err := New(srv.URL, "t").ListConfigFiles(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a 502 from the companion must surface as an error")
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

// TestContentTypePerEndpoint pins the Content-Type each write endpoint sends so
// it matches the OpenAPI spec (json for ref-replace + helpers, text/plain else).
func TestContentTypePerEndpoint(t *testing.T) {
	seen := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.Method+" "+r.URL.Path] = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","changes":[]}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "t")
	ctx := context.Background()
	_, _ = c.RefReplace(ctx, "a", "b", true)
	_, _ = c.CreateHelper(ctx, "x:\n  name: y\n", "input_boolean")
	_, _ = c.UpdateHelper(ctx, "x", "name: y\n")
	_, _ = c.CreateTemplate(ctx, "x", "sensor")
	_, _ = c.WriteConfigFile(ctx, "a.yaml", "x: 1\n", true)

	wantJSON := []string{"POST /v1/ref/replace", "POST /v1/config/helper", "PUT /v1/config/helper"}
	for _, key := range wantJSON {
		if seen[key] != mimeJSON {
			t.Errorf("%s Content-Type = %q, want %q", key, seen[key], mimeJSON)
		}
	}
	wantText := []string{"POST /v1/config/template", "PUT /v1/config/file"}
	for _, key := range wantText {
		if seen[key] != mimeText {
			t.Errorf("%s Content-Type = %q, want %q", key, seen[key], mimeText)
		}
	}
}
