package main

import (
	"encoding/json"
	"net/http"
	"net/url"
)

const pluginID = "cpa-account-queue"
const pluginVersion = "0.1.0-dev"
const managementBase = "/plugins/" + pluginID

type envelope struct {
	OK     bool       `json:"ok"`
	Result any        `json:"result,omitempty"`
	Error  *wireError `json:"error,omitempty"`
}
type wireError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status"`
	Retryable  bool   `json:"retryable"`
}

func ok(v any) ([]byte, error) { return json.Marshal(envelope{OK: true, Result: v}) }

type lifecycleInput struct {
	ConfigYAML       []byte `json:"config_yaml"`
	SchemaVersion    uint32 `json:"schema_version"`
	DeadlineUnixNano int64  `json:"deadline_unix_nano"`
}
type requestInput struct {
	RequestID      string
	Metadata       map[string]any
	HostCallbackID string `json:"host_callback_id"`
}
type interceptOutput struct {
	Terminate       bool
	StatusCode      int
	ResponseHeaders http.Header
	ResponseBody    []byte
}
type schedulerInput struct {
	Options    struct{ Metadata map[string]any }
	Candidates []struct{ ID string }
}
type managementInput struct {
	Method         string
	Path           string
	Headers        http.Header
	Query          url.Values
	Body           []byte
	HostCallbackID string `json:"host_callback_id"`
}
type managementOutput struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}
type authEntry struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Label       string `json:"label"`
	Provider    string `json:"provider"`
	Disabled    bool   `json:"disabled"`
	Unavailable bool   `json:"unavailable"`
}

func registration() any {
	return map[string]any{
		"schema_version": 7,
		"metadata": map[string]any{"Name": pluginID, "Version": pluginVersion, "Author": "2Wgmqra4ZuDMex", "GitHubRepository": "https://github.com/2Wgmqra4ZuDMex/cpa-account-queue", "ConfigFields": []any{
			map[string]any{"Name": "state_path", "Type": "string", "Description": "Persistent account policy JSON file; mount its directory."},
		}},
		"capabilities": map[string]bool{"scheduler": true, "request_interceptor": true, "request_interceptor_omit_body_before": true, "request_interceptor_omit_body_after": true, "request_lifecycle_plugin": true, "management_api": true},
	}
}

func managementRegistration() any {
	routes := []any{}
	for _, r := range []struct{ method, path string }{{"GET", "/status"}, {"GET", "/policies"}, {"PUT", "/policies"}} {
		routes = append(routes, map[string]string{"Method": r.method, "Path": managementBase + r.path})
	}
	return map[string]any{"routes": routes, "resources": []any{map[string]string{"Path": "/ui", "Menu": "账号并发与排队", "Description": "逐账号并发、有限队列及运行状态"}}}
}
