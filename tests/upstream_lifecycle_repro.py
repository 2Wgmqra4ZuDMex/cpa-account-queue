"""Read-only reproduction using the official v0.1.8 Windows DLL.

Usage: python tests/upstream_lifecycle_repro.py /path/to/upstream.dll
No host credentials, model requests, production configuration or daemon used.
Direct ABI calls isolate lifecycle state; they do not prove the production host
actually performed this exact sequence.
"""
import base64
import ctypes as C
import json
import pathlib
import sys


class Buffer(C.Structure):
    _fields_ = [('ptr', C.c_void_p), ('len', C.c_size_t)]


lib = C.CDLL(str(pathlib.Path(sys.argv[1]).resolve()))
call = lib.cliproxyPluginCall
call.argtypes = [C.c_char_p, C.c_void_p, C.c_size_t, C.POINTER(Buffer)]
call.restype = C.c_int
free = lib.cliproxyPluginFree
free.argtypes = [C.c_void_p, C.c_size_t]


def invoke(method, value):
    raw = json.dumps(value).encode()
    inp, out = C.create_string_buffer(raw), Buffer()
    rc = call(method.encode(), inp, len(raw), C.byref(out))
    try:
        result = json.loads(C.string_at(out.ptr, out.len))
    finally:
        if out.ptr:
            free(out.ptr, out.len)
    assert rc == 0 and result['ok'], result
    return result['result']


config = {'schema_version': 9, 'config_yaml': base64.b64encode(b'enabled: true\nmax_concurrency: 10\nauthority: local\n').decode()}


def request(id):
    return {'RequestID': id, 'Metadata': {'selected_auth_id': 'fixture-account'}}


invoke('plugin.register', config)
assert not invoke('request.intercept_after', request('normal'))['Terminate']
invoke('request.complete', request('normal'))
invoke('plugin.quiesce', {})
invoke('plugin.register', config)
assert not invoke('request.intercept_after', request('active-at-shutdown'))['Terminate']
lib.cliproxyPluginShutdown()
invoke('plugin.register', config)
rejected = invoke('request.intercept_after', request('after-reenable'))
error = json.loads(base64.b64decode(rejected['ResponseBody']))
assert rejected['Terminate'] and rejected['StatusCode'] == 503, rejected
assert error['error']['code'] == 'account_concurrency_authority_unavailable', error
print('REPRODUCED: register succeeds after active shutdown but new request returns authority_unavailable 503')
invoke('request.complete', request('active-at-shutdown'))
after = invoke('request.intercept_after', request('after-late-completion'))
print('ADMISSION_AFTER_LATE_COMPLETION', not after['Terminate'])
