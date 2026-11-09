package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/2Wgmqra4ZuDMex/cpa-account-queue/internal/admission"
)

func setupRuntime(t *testing.T) *runtime {
	t.Helper()
	r := &runtime{}
	raw, _ := json.Marshal(lifecycleInput{SchemaVersion: 9, ConfigYAML: []byte("state_path: " + filepath.ToSlash(filepath.Join(t.TempDir(), "policies.json")) + "\ndefaults:\n  max_concurrency: 1\n  max_queue: 2\n  wait_timeout_ms: 1000\n")})
	if _, err := r.handle("plugin.register", raw); err != nil {
		t.Fatal(err)
	}
	return r
}
func requestJSON(id, account string) []byte {
	b, _ := json.Marshal(requestInput{RequestID: id, Metadata: map[string]any{"selected_auth_id": account}})
	return b
}
func decodeResult[T any](t *testing.T, b []byte) T {
	t.Helper()
	var env struct {
		OK     bool `json:"ok"`
		Result T    `json:"result"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	if !env.OK {
		t.Fatalf("RPC failed: %s", b)
	}
	return env.Result
}
func waitRuntimeQueued(t *testing.T, r *runtime, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.engine.Snapshot(nil).Waiting == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("waiter did not enqueue")
}

func TestProtocolAdmissionQueueCompletion(t *testing.T) {
	r := setupRuntime(t)
	b, err := r.handle("request.intercept_after", requestJSON("a", "account"))
	if err != nil {
		t.Fatal(err)
	}
	if decodeResult[interceptOutput](t, b).Terminate {
		t.Fatal("first request rejected")
	}
	result := make(chan []byte, 1)
	go func() { b, _ := r.handle("request.intercept_after", requestJSON("b", "account")); result <- b }()
	waitRuntimeQueued(t, r, 1)
	if _, err := r.handle("request.complete", requestJSON("a", "account")); err != nil {
		t.Fatal(err)
	}
	if decodeResult[interceptOutput](t, <-result).Terminate {
		t.Fatal("queued request rejected")
	}
	if r.engine.Snapshot(nil).Accounts[0].InFlight != 1 {
		t.Fatal("stream slot released too early")
	}
	r.handle("request.complete", requestJSON("b", "account"))
}

func TestProtocolCancellationViaCompletion(t *testing.T) {
	r := setupRuntime(t)
	r.handle("request.intercept_after", requestJSON("a", "account"))
	result := make(chan []byte, 1)
	go func() { b, _ := r.handle("request.intercept_after", requestJSON("b", "account")); result <- b }()
	waitRuntimeQueued(t, r, 1)
	r.handle("request.complete", requestJSON("b", "account"))
	out := decodeResult[interceptOutput](t, <-result)
	if !out.Terminate || out.StatusCode != 409 {
		t.Fatalf("canceled request not rejected: %+v", out)
	}
	if r.engine.Snapshot(nil).Waiting != 0 {
		t.Fatal("canceled waiter leaked")
	}
}

func TestMetadataOnlyRegistrationAndNoStreamInterceptor(t *testing.T) {
	b, _ := ok(registration())
	var env struct {
		Result struct {
			SchemaVersion int             `json:"schema_version"`
			Capabilities  map[string]bool `json:"capabilities"`
		} `json:"result"`
	}
	json.Unmarshal(b, &env)
	if env.Result.SchemaVersion != 7 {
		t.Fatal("schema must support omit-body")
	}
	for _, key := range []string{"request_interceptor_omit_body_before", "request_interceptor_omit_body_after", "request_lifecycle_plugin"} {
		if !env.Result.Capabilities[key] {
			t.Fatal(key)
		}
	}
	if env.Result.Capabilities["response_stream_interceptor"] {
		t.Fatal("registered per-chunk work")
	}
}

func TestSettingsPersistAndRejectStaleRevision(t *testing.T) {
	r := setupRuntime(t)
	read := func() struct {
		Revision string           `json:"revision"`
		Config   admission.Config `json:"config"`
	} {
		b, _ := r.management(managementInput{Method: http.MethodGet, Path: managementBase + "/policies"})
		out := decodeResult[managementOutput](t, b)
		var data struct {
			Revision string           `json:"revision"`
			Config   admission.Config `json:"config"`
		}
		json.Unmarshal(out.Body, &data)
		return data
	}
	data := read()
	data.Config.Accounts["account"] = admission.Policy{MaxConcurrency: 5, MaxQueue: 9, WaitTimeoutMS: 30000}
	body, _ := json.Marshal(data)
	b, err := r.management(managementInput{Method: http.MethodPut, Path: managementBase + "/policies", Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if out := decodeResult[managementOutput](t, b); out.StatusCode != 200 {
		t.Fatalf("save: %s", out.Body)
	}
	b, _ = r.management(managementInput{Method: http.MethodPut, Path: managementBase + "/policies", Body: body})
	if out := decodeResult[managementOutput](t, b); out.StatusCode != 409 {
		t.Fatal("stale write accepted")
	}
	loaded, err := admission.Load(r.statePath, admission.DefaultConfig())
	if err != nil || loaded.Policy("account").MaxConcurrency != 5 {
		t.Fatal("policy not persisted")
	}
}

func TestPublicUIContainsNoLiveStateOrSavedSecrets(t *testing.T) {
	r := setupRuntime(t)
	b, err := r.management(managementInput{Method: http.MethodGet, Path: "/v0/resource/plugins/cpa-account-queue/ui"})
	if err != nil {
		t.Fatal(err)
	}
	out := decodeResult[managementOutput](t, b)
	if out.StatusCode != 200 {
		t.Fatal(out.StatusCode)
	}
	for _, forbidden := range []string{"localStorage", "innerHTML", "account-secret", "access_token"} {
		if strings.Contains(string(out.Body), forbidden) {
			t.Fatal(forbidden)
		}
	}
	for _, required := range []string{"X-Management-Key", "textContent", "revision", "保存策略", "诊断记录", "recent_events", "orphaned_complete"} {
		if !strings.Contains(string(out.Body), required) {
			t.Fatal(required)
		}
	}
}

func TestInvalidHostRejectedAndEmptyReloadRecovers(t *testing.T) {
	r := &runtime{}
	raw, _ := json.Marshal(lifecycleInput{SchemaVersion: 2, ConfigYAML: []byte("enabled: true")})
	if _, err := r.handle("plugin.register", raw); err == nil {
		t.Fatal("unsupported schema accepted")
	}
	r = setupRuntime(t)
	for i := 0; i < 20; i++ {
		if _, err := r.handle("plugin.quiesce", []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		raw, _ = json.Marshal(lifecycleInput{SchemaVersion: 9, ConfigYAML: []byte("state_path: " + filepath.ToSlash(r.statePath))})
		if _, err := r.handle("plugin.register", raw); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("reload-%d", i)
		b, err := r.handle("request.intercept_after", requestJSON(id, "account"))
		if err != nil || decodeResult[interceptOutput](t, b).Terminate {
			t.Fatal("first admission after reload failed")
		}
		r.handle("request.complete", requestJSON(id, "account"))
	}
}
