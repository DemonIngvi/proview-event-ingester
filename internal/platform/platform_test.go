package platform

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadiness(t *testing.T) {
	var r Readiness
	if err := r.Check(); err != nil {
		t.Fatalf("empty readiness: %v", err)
	}
	fail := true
	r.AddCheck(func() error {
		if fail {
			return errors.New("dep down")
		}
		return nil
	})
	if r.Check() == nil {
		t.Fatal("failing check ignored")
	}
	fail = false
	if r.Check() != nil {
		t.Fatal("passing check reported not ready")
	}
	r.SetShuttingDown()
	if r.Check() == nil {
		t.Fatal("ready while shutting down")
	}
}

func TestOpsHandler(t *testing.T) {
	var ready Readiness
	h := OpsHandler(NewRegistry(), &ready)
	for path, want := range map[string]int{"/healthz": 200, "/readyz": 200, "/metrics": 200} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Errorf("%s = %d", path, rec.Code)
		}
	}
	ready.SetShuttingDown()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz while shutting down = %d", rec.Code)
	}
}

func TestCORS(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := CORS([]string{"https://a.example"}, "GET", ok)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://a.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://a.example" || rec.Code != http.StatusTeapot {
		t.Fatalf("allowed origin: %v %d", rec.Header(), rec.Code)
	}

	req.Header.Set("Origin", "https://evil.example")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin got CORS header")
	}

	pre := httptest.NewRequest(http.MethodOptions, "/", nil)
	rec = httptest.NewRecorder()
	CORS([]string{"*"}, "GET", ok).ServeHTTP(rec, pre)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight = %d", rec.Code)
	}
}

func TestRecoverAndMetrics(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := NewRegistry()
	m := NewHTTPMetrics(reg, "test")
	h := Recover(log, m.Wrap("/boom", http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("panic status = %d", rec.Code)
	}

	ok := m.Wrap("/ok", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	ok.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	rec = httptest.NewRecorder()
	OpsHandler(reg, &Readiness{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `test_http_requests_total{code="202",route="/ok"} 1`) {
		t.Fatalf("metrics missing counter:\n%s", rec.Body.String())
	}
}

func TestEnvHelpers(t *testing.T) {
	m := map[string]string{"A": " x ", "D": "2s", "BAD": "zz", "L": "a, ,b"}
	lookup := func(k string) (string, bool) { v, ok := m[k]; return v, ok }
	if Env(lookup, "A", "d") != "x" || Env(lookup, "missing", "d") != "d" {
		t.Error("Env")
	}
	if d, err := EnvDuration(lookup, "D", time.Second); err != nil || d != 2*time.Second {
		t.Error("EnvDuration")
	}
	if _, err := EnvDuration(lookup, "BAD", time.Second); err == nil {
		t.Error("EnvDuration accepted garbage")
	}
	if l := EnvList(lookup, "L", nil); len(l) != 2 || l[1] != "b" {
		t.Errorf("EnvList = %v", l)
	}
}
