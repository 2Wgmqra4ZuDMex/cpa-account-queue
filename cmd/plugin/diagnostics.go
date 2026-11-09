package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/2Wgmqra4ZuDMex/cpa-account-queue/internal/admission"
)

const (
	diagnosticsQueueSize = 512
	diagnosticsMaxBytes  = 20 << 20
	diagnosticsMaxFiles  = 5
)

type diagnosticsWriter struct {
	path    string
	events  chan admission.Event
	done    chan struct{}
	started chan struct{}
	mu      sync.RWMutex
	file    *os.File
	writer  *bufio.Writer
	lastAt  time.Time
	bytes   int64
	count   uint64
	dropped atomic.Uint64
	errors  atomic.Uint64
	closed  atomic.Bool
}

type diagnosticsStatus struct {
	Enabled       bool   `json:"enabled"`
	Path          string `json:"path"`
	Bytes         int64  `json:"bytes"`
	MaxBytes      int64  `json:"max_bytes"`
	MaxFiles      int    `json:"max_files"`
	EventsWritten uint64 `json:"events_written"`
	EventsDropped uint64 `json:"events_dropped"`
	WriteErrors   uint64 `json:"write_errors"`
	LastWriteAt   string `json:"last_write_at,omitempty"`
}

func newDiagnosticsWriter(path string) (*diagnosticsWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	d := &diagnosticsWriter{path: path, events: make(chan admission.Event, diagnosticsQueueSize), done: make(chan struct{}), started: make(chan struct{})}
	go d.run()
	<-d.started
	return d, nil
}

func (d *diagnosticsWriter) Enqueue(event admission.Event) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return
	}
	select {
	case d.events <- event:
	default:
		d.dropped.Add(1)
	}
}

func (d *diagnosticsWriter) open() error {
	f, err := os.OpenFile(d.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	d.file = f
	d.writer = bufio.NewWriterSize(f, 32*1024)
	if info, statErr := f.Stat(); statErr == nil {
		d.bytes = info.Size()
	}
	return nil
}

func (d *diagnosticsWriter) rotate() error {
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			return err
		}
	}
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return err
		}
	}
	for i := diagnosticsMaxFiles - 1; i >= 1; i-- {
		old, next := fmt.Sprintf("%s.%d", d.path, i), fmt.Sprintf("%s.%d", d.path, i+1)
		_ = os.Remove(next)
		_ = os.Rename(old, next)
	}
	_ = os.Remove(d.path)
	d.file, d.writer, d.bytes = nil, nil, 0
	return d.open()
}

func (d *diagnosticsWriter) write(event admission.Event) error {
	if d.writer == nil {
		if err := d.open(); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if d.bytes+int64(len(raw)) > diagnosticsMaxBytes {
		if err := d.rotate(); err != nil {
			return err
		}
	}
	n, err := d.writer.Write(raw)
	if err != nil {
		return err
	}
	if err = d.writer.Flush(); err != nil {
		return err
	}
	d.bytes += int64(n)
	d.count++
	d.lastAt = event.At
	closeErr := d.file.Close()
	d.file, d.writer = nil, nil
	return closeErr
}

func (d *diagnosticsWriter) run() {
	close(d.started)
	for event := range d.events {
		d.mu.Lock()
		if d.writer == nil {
			if err := d.open(); err != nil {
				d.errors.Add(1)
				d.mu.Unlock()
				continue
			}
		}
		if err := d.write(event); err != nil {
			d.errors.Add(1)
		}
		d.mu.Unlock()
	}
	d.mu.Lock()
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			d.errors.Add(1)
		}
	}
	if d.file != nil {
		_ = d.file.Close()
	}
	d.mu.Unlock()
	close(d.done)
}

func (d *diagnosticsWriter) status() diagnosticsStatus {
	if d == nil {
		return diagnosticsStatus{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	last := ""
	if !d.lastAt.IsZero() {
		last = d.lastAt.UTC().Format(time.RFC3339Nano)
	}
	return diagnosticsStatus{Enabled: true, Path: d.path, Bytes: d.bytes, MaxBytes: diagnosticsMaxBytes, MaxFiles: diagnosticsMaxFiles, EventsWritten: d.count, EventsDropped: d.dropped.Load(), WriteErrors: d.errors.Load(), LastWriteAt: last}
}

func (d *diagnosticsWriter) close() {
	if d == nil || !d.closed.CompareAndSwap(false, true) {
		return
	}
	d.mu.Lock()
	close(d.events)
	d.mu.Unlock()
	<-d.done
}
