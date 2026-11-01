//go:build cgo

package main

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct { void *ptr; size_t len; } queue_buffer;
typedef int (*host_call)(void*, const char*, const uint8_t*, size_t, queue_buffer*);
typedef void (*buffer_free)(void*, size_t);
typedef struct { uint32_t abi_version; void *host_ctx; host_call call; buffer_free free_buffer; } queue_host;
typedef int (*plugin_call)(char*, uint8_t*, size_t, queue_buffer*);
typedef void (*plugin_shutdown)(void);
typedef struct { uint32_t abi_version; plugin_call call; buffer_free free_buffer; plugin_shutdown shutdown; } queue_plugin;
extern int queueCall(char*, uint8_t*, size_t, queue_buffer*);
extern void queueFree(void*, size_t);
extern void queueShutdown(void);
static queue_host host_copy;
static void set_host(queue_host *h) { host_copy = *h; }
static int invoke_host(char *m, uint8_t *p, size_t n, queue_buffer *out) {
  if (!host_copy.call) return 1;
  return host_copy.call(host_copy.host_ctx, m, p, n, out);
}
static void release_host(void *p, size_t n) { if (p && host_copy.free_buffer) host_copy.free_buffer(p,n); }
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"
)

var nativeMu sync.RWMutex

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.queue_host, plugin *C.queue_plugin) C.int {
	if host == nil || plugin == nil || host.abi_version != 1 {
		return 1
	}
	nativeMu.Lock()
	C.set_host(host)
	nativeMu.Unlock()
	plugin.abi_version = 1
	plugin.call = C.plugin_call(C.queueCall)
	plugin.free_buffer = C.buffer_free(C.queueFree)
	plugin.shutdown = C.plugin_shutdown(C.queueShutdown)
	return 0
}

//export queueCall
func queueCall(method *C.char, input *C.uint8_t, n C.size_t, out *C.queue_buffer) (rc C.int) {
	if out == nil {
		return 1
	}
	out.ptr = nil
	out.len = 0
	defer func() {
		if recover() != nil {
			writeNative(out, wireFailure(fmt.Errorf("plugin panic")))
			rc = 0
		}
	}()
	if method == nil || uint64(n) > 4<<20 {
		writeNative(out, wireFailure(fmt.Errorf("invalid native request")))
		return 0
	}
	var raw []byte
	if n > 0 {
		if input == nil {
			return 1
		}
		raw = C.GoBytes(unsafe.Pointer(input), C.int(n))
	}
	result, err := app.handle(C.GoString(method), raw)
	if err != nil {
		result = wireFailure(err)
	}
	writeNative(out, result)
	return 0
}

func writeNative(out *C.queue_buffer, b []byte) {
	if len(b) > 0 {
		out.ptr = C.CBytes(b)
		out.len = C.size_t(len(b))
	}
}

//export queueFree
func queueFree(p unsafe.Pointer, _ C.size_t) {
	if p != nil {
		C.free(p)
	}
}

//export queueShutdown
func queueShutdown() {
	if e, err := app.current(); err == nil {
		e.BeginDrain()
	}
}

func listHostAccounts(callbackID string) ([]authEntry, error) {
	nativeMu.RLock()
	defer nativeMu.RUnlock()
	method := C.CString("host.auth.list")
	defer C.free(unsafe.Pointer(method))
	b, _ := json.Marshal(map[string]string{"host_callback_id": callbackID})
	p := C.CBytes(b)
	defer C.free(p)
	var out C.queue_buffer
	rc := C.invoke_host(method, (*C.uint8_t)(p), C.size_t(len(b)), &out)
	if out.ptr != nil {
		defer C.release_host(out.ptr, out.len)
	}
	if rc != 0 || out.ptr == nil || uint64(out.len) > 8<<20 {
		return nil, fmt.Errorf("host account listing failed")
	}
	var env struct {
		OK     bool `json:"ok"`
		Result struct {
			Files []authEntry `json:"files"`
		} `json:"result"`
	}
	if err := json.Unmarshal(C.GoBytes(out.ptr, C.int(out.len)), &env); err != nil {
		return nil, err
	}
	if !env.OK {
		return nil, fmt.Errorf("host account listing failed")
	}
	return env.Result.Files, nil
}
