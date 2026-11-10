"""Exercise the compiled C ABI without credentials, network or model calls.

This is a wire-contract smoke test, not a substitute for pinned CPA integration.
"""
import base64
import ctypes as C
import json
import pathlib
import sys
import tempfile
import threading
import time


class Buffer(C.Structure):
    _fields_ = [("ptr", C.c_void_p), ("len", C.c_size_t)]


HOST_CALL = C.CFUNCTYPE(C.c_int, C.c_void_p, C.c_char_p, C.c_void_p, C.c_size_t, C.POINTER(Buffer))
FREE = C.CFUNCTYPE(None, C.c_void_p, C.c_size_t)
CALL = C.CFUNCTYPE(C.c_int, C.c_char_p, C.c_void_p, C.c_size_t, C.POINTER(Buffer))
SHUTDOWN = C.CFUNCTYPE(None)


class Host(C.Structure):
    _fields_ = [("abi", C.c_uint32), ("ctx", C.c_void_p), ("call", HOST_CALL), ("free", FREE)]


class Plugin(C.Structure):
    _fields_ = [("abi", C.c_uint32), ("call", CALL), ("free", FREE), ("shutdown", SHUTDOWN)]


buffers = {}


@HOST_CALL
def host_call(ctx, method, raw, size, out):
    if method != b"host.auth.list":
        return 1
    payload = json.dumps({"ok": True, "result": {"files": [{"id": "fixture-account", "name": "Fixture account"}]}}).encode()
    buf = C.create_string_buffer(payload)
    ptr = C.addressof(buf)
    buffers[ptr] = buf
    out.contents.ptr, out.contents.len = ptr, len(payload)
    return 0


@FREE
def host_free(ptr, size):
    buffers.pop(ptr, None)


lib = C.CDLL(str(pathlib.Path(sys.argv[1]).resolve()))
lib.cliproxy_plugin_init.argtypes = [C.POINTER(Host), C.POINTER(Plugin)]
lib.cliproxy_plugin_init.restype = C.c_int
host, plugin = Host(1, None, host_call, host_free), Plugin()
assert lib.cliproxy_plugin_init(C.byref(host), C.byref(plugin)) == 0
assert plugin.abi == 1


def call(method, value):
    raw = json.dumps(value).encode()
    inp, out = C.create_string_buffer(raw), Buffer()
    rc = plugin.call(method.encode(), inp, len(raw), C.byref(out))
    try:
        result = json.loads(C.string_at(out.ptr, out.len))
    finally:
        if out.ptr:
            plugin.free(out.ptr, out.len)
    assert rc == 0 and result["ok"], result
    return result["result"]


def request(id):
    return {"RequestID": id, "Metadata": {"selected_auth_id": "fixture-account"}}


with tempfile.TemporaryDirectory() as tmp:
    state_path = str(pathlib.Path(tmp) / "policies.json").replace("\\", "/")
    config = f"state_path: {state_path}\ndefaults:\n  max_concurrency: 1\n  max_queue: 2\n  wait_timeout_ms: 2000\n"
    lifecycle = {"schema_version": 9, "config_yaml": base64.b64encode(config.encode()).decode()}
    registration = call("plugin.register", lifecycle)
    assert registration["metadata"]["Name"] == "cpa-account-queue"
    assert registration["capabilities"]["request_interceptor_omit_body_after"]
    assert registration["capabilities"]["scheduler_across_priorities"]
    assert not call("request.intercept_after", request("first"))["Terminate"]
    result = []
    thread = threading.Thread(target=lambda: result.append(call("request.intercept_after", request("second"))))
    thread.start()
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        status = call("management.handle", {"Method": "GET", "Path": "/plugins/cpa-account-queue/status"})
        data = json.loads(base64.b64decode(status["Body"]))
        if data["runtime"]["waiting"] == 1:
            break
        time.sleep(.005)
    else:
        raise AssertionError("native waiter did not enter queue")
    assert data["account_index_available"]
    call("request.complete", request("first"))
    thread.join(3)
    assert not thread.is_alive() and not result[0]["Terminate"]
    call("request.complete", request("second"))
    for i in range(20):
        call("plugin.quiesce", {})
        plugin.shutdown()
        # Reuse the same mapped library, as native runtimes cannot be dlclosed.
        assert lib.cliproxy_plugin_init(C.byref(host), C.byref(plugin)) == 0
        call("plugin.register", lifecycle)
        assert not call("request.intercept_after", request(f"reload-{i}"))["Terminate"]
        call("request.complete", request(f"reload-{i}"))
    plugin.shutdown()
assert not buffers
print("Native ABI smoke passed: admission, FIFO wakeup, host account listing, 20 shutdown/reinit cycles")
