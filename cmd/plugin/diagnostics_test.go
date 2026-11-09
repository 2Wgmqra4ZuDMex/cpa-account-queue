package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/2Wgmqra4ZuDMex/cpa-account-queue/internal/admission"
)

func TestDiagnosticsWriterPersistsRedactedJSONLAndCloses(t *testing.T) {
	d, err := newDiagnosticsWriter(filepath.Join(t.TempDir(), "state", "diagnostics.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	d.Enqueue(admission.Event{At: time.Now().UTC(), Kind: "account_queue_timeout", AccountTag: "account-tag", RequestTag: "request-tag", WaitMS: 60000})
	d.close()

	f, err := os.Open(d.path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var event admission.Event
	if err := json.NewDecoder(bufio.NewReader(f)).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event.Kind != "account_queue_timeout" || event.AccountTag != "account-tag" || event.WaitMS != 60000 {
		t.Fatalf("unexpected persisted event: %+v", event)
	}
	if d.status().EventsWritten != 1 {
		t.Fatalf("unexpected status: %+v", d.status())
	}
}
