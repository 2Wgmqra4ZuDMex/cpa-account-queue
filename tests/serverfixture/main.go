// Loopback-published fake upstream for isolated server acceptance. No external I/O.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex
var active = map[string]int{}
var peak = map[string]int{}
var arrivals = []string{}
var release = make(chan struct{})

func main() {
	http.HandleFunc("/control", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Query().Get("action") {
		case "release":
			select {
			case <-release:
			default:
				close(release)
			}
		case "reset":
			for _, n := range active {
				if n != 0 {
					http.Error(w, "active", 409)
					return
				}
			}
			release = make(chan struct{})
			peak = map[string]int{}
			arrivals = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"active": active, "peak": peak, "arrivals": arrivals})
	})
	http.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Model    string `json:"model"`
			Stream   bool   `json:"stream"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in); err != nil {
			http.Error(w, "bad fixture request", 400)
			return
		}
		text := ""
		if len(in.Messages) > 0 {
			text = in.Messages[len(in.Messages)-1].Content
		}
		account := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		mu.Lock()
		active[account]++
		if active[account] > peak[account] {
			peak[account] = active[account]
		}
		arrivals = append(arrivals, text)
		gate := release
		mu.Unlock()
		defer func() { mu.Lock(); active[account]--; mu.Unlock() }()
		if strings.Contains(text, "hold") {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Second):
				http.Error(w, "fixture hold expired", 504)
				return
			}
		}
		if in.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, "data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"},\"finish_reason\":null}]}\n\n", in.Model)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(100 * time.Millisecond)
			fmt.Fprint(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		} else {
			time.Sleep(100 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"id": "fixture", "object": "chat.completion", "model": in.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": "OK"}, "finish_reason": "stop"}}, "usage": map[string]int{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
		}
	})
	if err := http.ListenAndServe(":8319", nil); err != nil {
		panic(err)
	}
}
