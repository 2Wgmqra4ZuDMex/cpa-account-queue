package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/2Wgmqra4ZuDMex/cpa-account-queue/internal/admission"
	"gopkg.in/yaml.v3"
)

type runtime struct {
	mu          sync.Mutex
	engine      *admission.Engine
	diagnostics *diagnosticsWriter
	config      admission.Config
	statePath   string
	configured  bool
	// deferredAt is when the host last asked to replace this version while
	// requests were still active.
	deferredAt time.Time
}

var app = &runtime{}

func (r *runtime) closeDiagnostics() {
	r.mu.Lock()
	d := r.diagnostics
	e := r.engine
	r.diagnostics = nil
	r.mu.Unlock()
	if e != nil {
		e.SetEventSink(nil)
	}
	if d != nil {
		d.close()
	}
}

func (r *runtime) configure(raw []byte) error {
	var in lifecycleInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return err
	}
	if in.SchemaVersion < 7 {
		return fmt.Errorf("CPA schema 7 or newer is required (metadata-only admission)")
	}
	cfg := struct {
		StatePath          string            `yaml:"state_path"`
		Defaults           *admission.Policy `yaml:"defaults"`
		PinnedFullBehavior string            `yaml:"pinned_full_behavior"`
	}{StatePath: "plugins/state/cpa-account-queue/policies.json"}
	if err := yaml.Unmarshal(in.ConfigYAML, &cfg); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.StatePath) == "" {
		return fmt.Errorf("state_path must not be empty")
	}
	fallback := admission.DefaultConfig()
	if cfg.Defaults != nil {
		fallback.Defaults = *cfg.Defaults
	}
	if cfg.PinnedFullBehavior != "" {
		fallback.PinnedFullBehavior = cfg.PinnedFullBehavior
	}
	path, err := filepath.Abs(cfg.StatePath)
	if err != nil {
		return err
	}
	c, err := admission.Load(path, fallback)
	if err != nil {
		return fmt.Errorf("load policies: %w", err)
	}
	if err = c.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.configured && r.statePath != path {
		return fmt.Errorf("state_path cannot change during hot reconfiguration")
	}
	if r.engine == nil {
		r.engine, err = admission.New(c)
		if err != nil {
			return err
		}
	} else {
		// A real drain with live executions may not be undone by merely toggling
		// enabled. Never reset counters to recover from lifecycle uncertainty.
		if err = r.engine.Resume(); err != nil {
			return err
		}
		if err = r.engine.Update(c); err != nil {
			return err
		}
	}
	r.config = c
	r.statePath = path
	r.configured = true
	if r.diagnostics == nil {
		if d, diagErr := newDiagnosticsWriter(filepath.Join(filepath.Dir(path), "diagnostics.jsonl")); diagErr == nil {
			r.diagnostics = d
			r.engine.SetEventSink(d.Enqueue)
			r.engine.RecordEvent("plugin_started", "diagnostics writer enabled")
		}
	} else {
		r.engine.RecordEvent("plugin_reconfigured", "policies reloaded")
	}
	return nil
}

func (r *runtime) current() (*admission.Engine, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.configured || r.engine == nil {
		return nil, fmt.Errorf("plugin is not configured")
	}
	return r.engine, nil
}

// selectionID returns the host's per-pick identifier, which it sends with the
// scheduler pick and again with that pick's after-auth interception.
func selectionID(metadata map[string]any) string {
	id, _ := metadata["selection_id"].(string)
	return strings.TrimSpace(id)
}

// sessionKey identifies the conversation a pick belongs to, from the identity
// the host derives before selection, scoped by model as the host's own
// affinity is. Only this one-way hash is kept.
func sessionKey(model string, metadata map[string]any) string {
	id := ""
	for _, key := range []string{"canonical_session_id", "derived_session_id"} {
		if value, _ := metadata[key].(string); strings.TrimSpace(value) != "" {
			id = strings.TrimSpace(value)
			break
		}
	}
	if id == "" {
		return ""
	}
	// A suffix such as "model(high)" selects reasoning effort, not a model.
	if open := strings.LastIndex(model, "("); open > 0 && strings.HasSuffix(model, ")") {
		model = model[:open]
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(model) + "\x00" + id))
	return hex.EncodeToString(sum[:16])
}

func (r *runtime) handle(method string, raw []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := r.configure(raw); err != nil {
			return nil, err
		}
		if method == "plugin.register" {
			// A new attachment of this image: whatever replacement was
			// pending has been settled by the host.
			r.mu.Lock()
			r.deferredAt = time.Time{}
			r.mu.Unlock()
		}
		return ok(registration())
	case "management.register":
		return ok(managementRegistration())
	case "management.handle":
		var in managementInput
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		return r.management(in)
	case "request.intercept_before":
		return ok(interceptOutput{})
	}
	e, err := r.current()
	if err != nil {
		return nil, err
	}
	switch method {
	case "scheduler.pick":
		var in schedulerInput
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		// The host already limits Candidates to a pinned credential when one
		// applies, so the list is the complete set this request may run on.
		candidates := make([]admission.Candidate, 0, len(in.Candidates))
		for _, c := range in.Candidates {
			if c.ID != "" {
				candidates = append(candidates, admission.Candidate{ID: c.ID, Priority: c.Priority})
			}
		}
		id, handled, err := e.Pick(candidates, selectionID(in.Options.Metadata), sessionKey(in.Model, in.Options.Metadata))
		if err != nil {
			return nil, err
		}
		if !handled {
			// The host then runs its own configured selector.
			return ok(map[string]any{"Handled": false})
		}
		return ok(map[string]any{"Handled": true, "AuthID": id})
	case "request.intercept_after":
		var in requestInput
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		// The host has already committed to this credential and this callback
		// cannot change it, so admission must account for exactly that one.
		id, _ := in.Metadata["selected_auth_id"].(string)
		if err := e.AcquirePicked(context.Background(), strings.TrimSpace(in.RequestID), strings.TrimSpace(id), selectionID(in.Metadata)); err != nil {
			ae := admission.AsError(err)
			body, _ := json.Marshal(map[string]any{"error": map[string]any{"code": ae.Code, "type": ae.Code, "message": ae.Message, "retryable": ae.Status == 503}})
			return ok(interceptOutput{Terminate: true, StatusCode: ae.Status, ResponseBody: body, ResponseHeaders: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}})
		}
		return ok(interceptOutput{})
	case "request.complete":
		var in requestInput
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		e.Complete(strings.TrimSpace(in.RequestID))
		return ok(struct{}{})
	case "plugin.quiesce":
		var in lifecycleInput
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, err
			}
		}
		deadline := time.Now().Add(2 * time.Second)
		if in.DeadlineUnixNano > 0 {
			deadline = time.Unix(0, in.DeadlineUnixNano)
		}
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		// The host quiesces a version only to replace it. Refusing leaves
		// this version serving; the host retries on its next config reload.
		e.RecordEvent("replacement_requested", "host asked to replace this version")
		if err := e.Drain(ctx); err != nil {
			e.RecordEvent("replacement_deferred", "requests are active; this version keeps serving")
			r.mu.Lock()
			r.deferredAt = time.Now().UTC()
			r.mu.Unlock()
			return nil, err
		}
		e.RecordEvent("handed_over", "idle; requests pass unlimited until the host switches versions")
		return ok(struct{}{})
	case "plugin.shutdown":
		e.RecordEvent("plugin_shutdown", "shutdown requested")
		e.BeginDrain()
		r.mu.Lock()
		d := r.diagnostics
		r.diagnostics = nil
		r.mu.Unlock()
		e.SetEventSink(nil)
		if d != nil {
			d.close()
		}
		return ok(struct{}{})
	default:
		return nil, fmt.Errorf("unsupported plugin method")
	}
}

func revision(c admission.Config) string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (r *runtime) management(in managementInput) ([]byte, error) {
	// Resource routes are public: serve a static shell only, never live data.
	if strings.HasPrefix(in.Path, "/v0/resource/plugins/"+pluginID+"/") || in.Path == "/ui" {
		if in.Method != "GET" || !strings.HasSuffix(in.Path, "/ui") {
			return managementJSON(404, map[string]string{"error": "not found"})
		}
		return ok(managementOutput{StatusCode: 200, Headers: http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"}}, Body: uiHTML})
	}
	e, err := r.current()
	if err != nil {
		return managementJSON(503, map[string]string{"error": "plugin is not configured"})
	}
	switch {
	case in.Method == "GET" && strings.HasSuffix(in.Path, managementBase+"/status"):
		entries, listErr := listHostAccounts(in.HostCallbackID)
		ids := []string{}
		for _, v := range entries {
			ids = append(ids, v.ID)
		}
		s := e.Snapshot(ids)
		r.mu.Lock()
		diagnostics := diagnosticsStatus{}
		if r.diagnostics != nil {
			diagnostics = r.diagnostics.status()
		}
		deferred := ""
		if !r.deferredAt.IsZero() {
			deferred = r.deferredAt.Format(time.RFC3339)
		}
		r.mu.Unlock()
		return managementJSON(200, map[string]any{"version": pluginVersion, "authority": "local", "runtime": s, "replacement_deferred_at": deferred, "diagnostics": diagnostics, "account_index": entries, "account_index_available": listErr == nil, "account_index_error": func() string {
			if listErr != nil {
				return "CPA account listing unavailable; active and configured accounts remain visible"
			}
			return ""
		}()})
	case in.Method == "GET" && strings.HasSuffix(in.Path, managementBase+"/diagnostics"):
		r.mu.Lock()
		diagnostics := diagnosticsStatus{}
		if r.diagnostics != nil {
			diagnostics = r.diagnostics.status()
		}
		r.mu.Unlock()
		return managementJSON(200, diagnostics)
	case in.Method == "GET" && strings.HasSuffix(in.Path, managementBase+"/policies"):
		r.mu.Lock()
		c := r.config.Clone()
		r.mu.Unlock()
		return managementJSON(200, map[string]any{"revision": revision(c), "config": c})
	case in.Method == "PUT" && strings.HasSuffix(in.Path, managementBase+"/policies"):
		var update struct {
			Revision string          `json:"revision"`
			Config   json.RawMessage `json:"config"`
		}
		if len(in.Body) > admission.MaxConfigBytes {
			return managementJSON(413, map[string]string{"error": "configuration too large"})
		}
		if err := json.Unmarshal(in.Body, &update); err != nil {
			return managementJSON(400, map[string]string{"error": "invalid JSON"})
		}
		c, err := admission.DecodeConfig(update.Config)
		if err != nil {
			return managementJSON(400, map[string]string{"error": err.Error()})
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if c.PinnedFullBehavior == "" {
			c.PinnedFullBehavior = r.config.PinnedFullBehavior
			if c.PinnedFullBehavior == "" {
				c.PinnedFullBehavior = "queue"
			}
		}
		if update.Revision != revision(r.config) {
			return managementJSON(409, map[string]string{"error": "configuration changed; reload before saving"})
		}
		if err := admission.Save(r.statePath, c); err != nil {
			return managementJSON(500, map[string]string{"error": "could not persist policies; no policy was changed"})
		}
		if err := e.Update(c); err != nil {
			return nil, err
		}
		r.config = c
		return managementJSON(200, map[string]any{"revision": revision(c), "config": c})
	default:
		return managementJSON(404, map[string]string{"error": "not found"})
	}
}

func managementJSON(status int, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return ok(managementOutput{StatusCode: status, Headers: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"}}, Body: b})
}

func wireFailure(err error) []byte {
	ae := admission.AsError(err)
	var typed *admission.Error
	if !errors.As(err, &typed) {
		ae = &admission.Error{Code: "plugin_error", Message: "plugin operation failed", Status: 503}
	}
	b, _ := json.Marshal(envelope{Error: &wireError{Code: ae.Code, Message: ae.Message, HTTPStatus: ae.Status, Retryable: ae.Status == 503}})
	return b
}
