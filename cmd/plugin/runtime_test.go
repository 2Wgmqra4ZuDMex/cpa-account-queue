package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/2Wgmqra4ZuDMex/cpa-account-queue/internal/admission"
)

func setupRuntime(t *testing.T) *runtime {
	t.Helper()
	stateDir := t.TempDir()
	r := &runtime{}
	t.Cleanup(r.closeDiagnostics)
	raw, _ := json.Marshal(lifecycleInput{SchemaVersion: 9, ConfigYAML: []byte("state_path: " + filepath.ToSlash(filepath.Join(stateDir, "policies.json")) + "\ndefaults:\n  max_concurrency: 1\n  max_queue: 2\n  wait_timeout_ms: 1000\n")})
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

// pick sends the host's scheduler request shape: every available credential
// across priority tiers, each with its priority, under one selection ID.
func pick(t *testing.T, r *runtime, selection string) string {
	t.Helper()
	raw := []byte(`{"Provider":"codex","Model":"m","Options":{"Metadata":{"selection_id":"` + selection + `"}},"Candidates":[{"ID":"team","Provider":"codex","Priority":0},{"ID":"prolite","Provider":"codex","Priority":-10}]}`)
	b, err := r.handle("scheduler.pick", raw)
	if err != nil {
		t.Fatal(err)
	}
	out := decodeResult[struct {
		Handled bool
		AuthID  string
	}](t, b)
	if !out.Handled {
		t.Fatal("scheduler did not handle the pick")
	}
	return out.AuthID
}

// admitJSON is the after-auth interception the host sends for a pick.
func admitJSON(id, account, selection string) []byte {
	b, _ := json.Marshal(requestInput{RequestID: id, Metadata: map[string]any{"selected_auth_id": account, "selection_id": selection}})
	return b
}

func setBehavior(t *testing.T, r *runtime, behavior string) {
	t.Helper()
	c := r.config.Clone()
	c.PinnedFullBehavior = behavior
	body, _ := json.Marshal(map[string]any{"revision": revision(r.config), "config": c})
	b, err := r.management(managementInput{Method: http.MethodPut, Path: managementBase + "/policies", Body: body})
	if err != nil || decodeResult[managementOutput](t, b).StatusCode != 200 {
		t.Fatalf("save behavior: %v %s", err, b)
	}
}

func occupancy(r *runtime) map[string]admission.AccountSnapshot {
	rows := map[string]admission.AccountSnapshot{}
	for _, row := range r.engine.Snapshot(nil).Accounts {
		rows[row.ID] = row
	}
	return rows
}

func admit(t *testing.T, r *runtime, id, account string) {
	t.Helper()
	b, err := r.handle("request.intercept_after", admitJSON(id, account, id))
	if err != nil || decodeResult[interceptOutput](t, b).Terminate {
		t.Fatalf("%s not admitted on %s: %v %s", id, account, err, b)
	}
}

// Default mode keeps the host's own rule: only the highest tier is used.
func TestProtocolSchedulerQueueModeKeepsHighestTier(t *testing.T) {
	r := setupRuntime(t)
	if got := pick(t, r, "first"); got != "team" {
		t.Fatalf("idle pick=%q", got)
	}
	admit(t, r, "first", "team")
	if got := pick(t, r, "second"); got != "team" {
		t.Fatalf("queue mode pick=%q", got)
	}
	r.handle("request.complete", requestJSON("first", "team"))
}

func TestProtocolSchedulerOverflowsAcrossPriorityTiers(t *testing.T) {
	r := setupRuntime(t)
	setBehavior(t, r, "fallback")
	if got := pick(t, r, "first"); got != "team" {
		t.Fatalf("idle pick=%q", got)
	}
	admit(t, r, "first", "team")
	got := pick(t, r, "second")
	if got != "prolite" {
		t.Fatalf("fallback pick=%q", got)
	}
	admit(t, r, "second", got)
	rows := occupancy(r)
	if rows["team"].InFlight != 1 || rows["team"].Queued != 0 || rows["prolite"].InFlight != 1 {
		t.Fatalf("unexpected occupancy: %+v", rows)
	}
	r.handle("request.complete", requestJSON("first", "team"))
	r.handle("request.complete", requestJSON("second", "prolite"))
	// With Team free again the next request returns to the top tier.
	if got := pick(t, r, "third"); got != "team" {
		t.Fatalf("pick after release=%q", got)
	}
}

// sessionPick is one scheduler pick over two interchangeable accounts, with
// the session metadata the host attaches before selection.
func sessionPick(t *testing.T, r *runtime, selection, model string, metadata map[string]any) string {
	t.Helper()
	metadata["selection_id"] = selection
	raw, _ := json.Marshal(map[string]any{"Model": model, "Options": map[string]any{"Metadata": metadata}, "Candidates": []map[string]any{{"ID": "a"}, {"ID": "b"}}})
	b, err := r.handle("scheduler.pick", raw)
	if err != nil {
		t.Fatal(err)
	}
	return decodeResult[struct{ AuthID string }](t, b).AuthID
}

// runTurn picks, admits and completes one request, returning its account.
func runTurn(t *testing.T, r *runtime, id, model string, metadata map[string]any) string {
	t.Helper()
	account := sessionPick(t, r, id, model, metadata)
	admit(t, r, id, account)
	r.handle("request.complete", requestJSON(id, account))
	return account
}

func TestProtocolSessionAffinityFollowsHostSessionIdentity(t *testing.T) {
	r := setupRuntime(t)
	explicit := func() map[string]any { return map[string]any{"canonical_session_id": "header:conversation-one"} }
	derived := func() map[string]any { return map[string]any{"derived_session_id": "0f3c-derived-two"} }
	if got := runTurn(t, r, "one-1", "m", explicit()); got != "a" {
		t.Fatalf("first session started on %q", got)
	}
	if got := runTurn(t, r, "two-1", "m", derived()); got != "b" {
		t.Fatalf("second session started on %q", got)
	}
	for i := 2; i <= 3; i++ {
		if got := runTurn(t, r, fmt.Sprintf("two-%d", i), "m", derived()); got != "b" {
			t.Fatalf("derived session moved to %q", got)
		}
		if got := runTurn(t, r, fmt.Sprintf("one-%d", i), "m", explicit()); got != "a" {
			t.Fatalf("explicit session moved to %q", got)
		}
	}
	// A reasoning-effort suffix is the same model, so the same binding.
	if got := runTurn(t, r, "two-effort", "m(high)", derived()); got != "b" {
		t.Fatalf("effort suffix broke the binding: %q", got)
	}
	// The host scopes affinity by model; another model is another binding,
	// placed on the account holding fewer sessions.
	if got := runTurn(t, r, "one-other", "other", explicit()); got != "a" {
		t.Fatalf("other model started on %q", got)
	}
	c := r.engine.Snapshot(nil).Counters
	if c.AffinityNew != 3 || c.AffinityKept != 5 || c.AffinityMoved != 0 {
		t.Fatalf("unexpected affinity counters: %+v", c)
	}
}

func TestProtocolSessionAffinityCanBeSwitchedOff(t *testing.T) {
	r := setupRuntime(t)
	meta := func(id string) map[string]any { return map[string]any{"canonical_session_id": id} }
	runTurn(t, r, "one-1", "m", meta("one"))
	if got := runTurn(t, r, "two-1", "m", meta("two")); got != "b" {
		t.Fatalf("second session started on %q", got)
	}
	c := r.config.Clone()
	c.DisableSessionAffinity = true
	body, _ := json.Marshal(map[string]any{"revision": revision(r.config), "config": c})
	b, err := r.management(managementInput{Method: http.MethodPut, Path: managementBase + "/policies", Body: body})
	if err != nil || decodeResult[managementOutput](t, b).StatusCode != 200 {
		t.Fatalf("save: %v %s", err, b)
	}
	if got := runTurn(t, r, "two-2", "m", meta("two")); got != "a" {
		t.Fatalf("affinity still applied after being switched off: %q", got)
	}
	loaded, err := admission.Load(r.statePath, admission.DefaultConfig())
	if err != nil || !loaded.DisableSessionAffinity {
		t.Fatalf("setting not persisted: %+v %v", loaded, err)
	}
}

// A state file saved with affinity on must stay loadable by 0.1.9, which
// rejects unknown fields, so that downgrading does not break the plugin.
func TestStateFileWithAffinityOnHasNoNewFields(t *testing.T) {
	r := setupRuntime(t)
	setBehavior(t, r, "fallback")
	raw, err := os.ReadFile(r.statePath)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for name := range fields {
		switch name {
		case "version", "defaults", "accounts", "pinned_full_behavior":
		default:
			t.Fatalf("state file gained field %q: %s", name, raw)
		}
	}
}

// The host executes on selected_auth_id regardless of what this callback
// returns, so admission must never account for a different account.
func TestProtocolAdmissionNeverSwitchesAccount(t *testing.T) {
	r := setupRuntime(t)
	setBehavior(t, r, "fallback")
	r.handle("request.intercept_after", admitJSON("first", "team", "first"))
	result := make(chan []byte, 1)
	go func() {
		b, _ := r.handle("request.intercept_after", admitJSON("second", "team", "second"))
		result <- b
	}()
	waitRuntimeQueued(t, r, 1)
	if rows := occupancy(r); rows["team"].Queued != 1 || rows["prolite"].InFlight != 0 || rows["prolite"].Queued != 0 {
		t.Fatalf("admission moved to an account the host did not select: %+v", rows)
	}
	r.handle("request.complete", requestJSON("first", "team"))
	if decodeResult[interceptOutput](t, <-result).Terminate {
		t.Fatal("queued request rejected")
	}
	r.handle("request.complete", requestJSON("second", "team"))
}

// The host quiesces a version only to replace it. With a request running the
// plugin must refuse and stay exactly as it was, so that the host's rollback
// (plugin.reconfigure on the old version) succeeds and nothing is rejected.
func TestProtocolQuiesceUnderLoadKeepsThisVersionServing(t *testing.T) {
	r := setupRuntime(t)
	admit(t, r, "active", "team")
	queued := make(chan []byte, 1)
	go func() {
		b, _ := r.handle("request.intercept_after", admitJSON("queued", "team", "queued"))
		queued <- b
	}()
	waitRuntimeQueued(t, r, 1)

	quiesce, _ := json.Marshal(lifecycleInput{DeadlineUnixNano: time.Now().Add(40 * time.Millisecond).UnixNano()})
	_, err := r.handle("plugin.quiesce", quiesce)
	if err == nil {
		t.Fatal("quiesce succeeded with a request running")
	}
	var failure struct {
		Error wireError `json:"error"`
	}
	if errDecode := json.Unmarshal(wireFailure(err), &failure); errDecode != nil || failure.Error.Code != "plugin_busy" {
		t.Fatalf("quiesce failure = %s", wireFailure(err))
	}
	if s := r.engine.Snapshot(nil); s.Draining || s.Waiting != 1 || s.Counters.DrainingRejected != 0 {
		t.Fatalf("refused quiesce disturbed the runtime: %+v", s)
	}

	// The host's rollback re-sends the configuration it registered with.
	raw, _ := json.Marshal(lifecycleInput{SchemaVersion: 9, ConfigYAML: []byte("state_path: " + filepath.ToSlash(r.statePath) + "\ndefaults:\n  max_concurrency: 1\n  max_queue: 2\n  wait_timeout_ms: 1000\n")})
	if _, err := r.handle("plugin.reconfigure", raw); err != nil {
		t.Fatalf("rollback reconfigure failed: %v", err)
	}
	// A request arriving afterwards is picked and queued as usual.
	if got := pick(t, r, "later"); got != "team" {
		t.Fatalf("pick after rollback = %q", got)
	}
	later := make(chan []byte, 1)
	go func() {
		b, _ := r.handle("request.intercept_after", admitJSON("later", "team", "later"))
		later <- b
	}()
	waitRuntimeQueued(t, r, 2)
	r.handle("request.complete", requestJSON("active", "team"))
	if decodeResult[interceptOutput](t, <-queued).Terminate {
		t.Fatal("request queued across the refused quiesce was rejected")
	}
	r.handle("request.complete", requestJSON("queued", "team"))
	if decodeResult[interceptOutput](t, <-later).Terminate {
		t.Fatal("request arriving after the refused quiesce was rejected")
	}
	r.handle("request.complete", requestJSON("later", "team"))

	type report struct {
		Version    string `json:"version"`
		DeferredAt string `json:"replacement_deferred_at"`
		Runtime    struct {
			RecentEvents []admission.Event `json:"recent_events"`
		} `json:"runtime"`
	}
	read := func() report {
		t.Helper()
		b, err := r.management(managementInput{Method: http.MethodGet, Path: managementBase + "/status"})
		if err != nil {
			t.Fatal(err)
		}
		var out report
		if err := json.Unmarshal(decodeResult[managementOutput](t, b).Body, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	status := read()
	kinds := map[string]int{}
	for _, event := range status.Runtime.RecentEvents {
		kinds[event.Kind]++
	}
	if status.DeferredAt == "" || kinds["replacement_requested"] != 1 || kinds["replacement_deferred"] != 1 || status.Version != pluginVersion {
		t.Fatalf("deferred replacement not reported: at=%q events=%v version=%q", status.DeferredAt, kinds, status.Version)
	}

	// Idle now, so the host's next attempt goes through. When this image is
	// attached again, no replacement is pending any more.
	idle, _ := json.Marshal(lifecycleInput{DeadlineUnixNano: time.Now().Add(time.Second).UnixNano()})
	if _, err := r.handle("plugin.quiesce", idle); err != nil {
		t.Fatalf("quiesce while idle: %v", err)
	}
	if _, err := r.handle("plugin.register", raw); err != nil {
		t.Fatalf("register after an idle quiesce: %v", err)
	}
	if status := read(); status.DeferredAt != "" {
		t.Fatalf("pending replacement still reported after a new attachment: %q", status.DeferredAt)
	}
}

func setHostSelection(t *testing.T, r *runtime, on bool) {
	t.Helper()
	c := r.config.Clone()
	c.HostSelection = on
	body, _ := json.Marshal(map[string]any{"revision": revision(r.config), "config": c})
	b, err := r.management(managementInput{Method: http.MethodPut, Path: managementBase + "/policies", Body: body})
	if err != nil || decodeResult[managementOutput](t, b).StatusCode != 200 {
		t.Fatalf("save host selection: %v %s", err, b)
	}
}

// With host selection the scheduler answer is "not handled", which makes the
// host run its own selector; admission still limits the account it chose.
func TestProtocolHostSelectionDeclinesPickButStillLimits(t *testing.T) {
	r := setupRuntime(t)
	setHostSelection(t, r, true)
	raw := []byte(`{"Model":"m","Options":{"Metadata":{"selection_id":"one","canonical_session_id":"conversation"}},"Candidates":[{"ID":"team","Priority":0},{"ID":"prolite","Priority":-10}]}`)
	b, err := r.handle("scheduler.pick", raw)
	if err != nil {
		t.Fatal(err)
	}
	out := decodeResult[map[string]any](t, b)
	if handled, _ := out["Handled"].(bool); handled || len(out) != 1 {
		t.Fatalf("pick answer = %v, want only Handled=false", out)
	}
	admit(t, r, "first", "team")
	result := make(chan []byte, 1)
	go func() {
		b, _ := r.handle("request.intercept_after", admitJSON("second", "team", "two"))
		result <- b
	}()
	waitRuntimeQueued(t, r, 1)
	r.handle("request.complete", requestJSON("first", "team"))
	if decodeResult[interceptOutput](t, <-result).Terminate {
		t.Fatal("queued request rejected")
	}
	loaded, err := admission.Load(r.statePath, admission.DefaultConfig())
	if err != nil || !loaded.HostSelection {
		t.Fatalf("setting not persisted: %+v %v", loaded, err)
	}
	// Back to plugin selection: the pick is handled again (pick fails the
	// test otherwise) and follows the plugin's own rules.
	r.handle("request.complete", requestJSON("second", "team"))
	setHostSelection(t, r, false)
	if got := pick(t, r, "three"); got != "team" {
		t.Fatalf("pick after switching back = %q, want the idle top tier account", got)
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
	for _, key := range []string{"scheduler", "scheduler_across_priorities", "request_interceptor_omit_body_before", "request_interceptor_omit_body_after", "request_lifecycle_plugin"} {
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

func TestPinnedFullBehaviorConfigRoundTrip(t *testing.T) {
	r := setupRuntime(t)
	b, _ := r.management(managementInput{Method: http.MethodGet, Path: managementBase + "/policies"})
	out := decodeResult[managementOutput](t, b)
	var data struct {
		Revision string           `json:"revision"`
		Config   admission.Config `json:"config"`
	}
	if err := json.Unmarshal(out.Body, &data); err != nil {
		t.Fatal(err)
	}
	data.Config.PinnedFullBehavior = "fallback"
	body, _ := json.Marshal(data)
	b, err := r.management(managementInput{Method: http.MethodPut, Path: managementBase + "/policies", Body: body})
	if err != nil || decodeResult[managementOutput](t, b).StatusCode != 200 {
		t.Fatalf("save fallback: %v %s", err, b)
	}
	if r.config.PinnedFullBehavior != "fallback" {
		t.Fatalf("runtime config=%q", r.config.PinnedFullBehavior)
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
	for _, required := range []string{"X-Management-Key", "textContent", "revision", "保存策略", "诊断记录", "recent_events", "orphaned_complete", "日志：", "查看实时计数和最近事件", "priority_overflow", "accountPlatform", "session-affinity", "affinity_kept", "selection-mode", "host_selection", "affinity_moved_not_offered", "queued_other_free", "replacement_deferred_at", "handover_passed", "handed_over"} {
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
	r.closeDiagnostics()
}
