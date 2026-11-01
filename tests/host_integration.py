"""Black-box pinned CPA integration against a loopback-only fake upstream.

Usage: python tests/host_integration.py /path/to/cli-proxy-api /path/to/plugin.so
Requires PyYAML. Uses temporary credentials and never contacts a model service.
"""
import concurrent.futures
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yaml

LOCK = threading.Lock()
ACTIVE = 0
PEAK = 0
ARRIVALS = []
RELEASE = threading.Event()


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        global ACTIVE, PEAK
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        text = body.get('messages', [{}])[-1].get('content', '')
        with LOCK:
            ACTIVE += 1
            PEAK = max(PEAK, ACTIVE)
            ARRIVALS.append(text)
        try:
            if 'hold' in text:
                assert RELEASE.wait(8), 'upstream fixture was not released'
            else:
                time.sleep(.1)
            if body.get('stream'):
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.end_headers()
                self.wfile.write(b'data: {"id":"fixture","object":"chat.completion.chunk","model":"queue-test","choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":null}]}\n\n')
                self.wfile.flush()
                time.sleep(.1)
                self.wfile.write(b'data: {"id":"fixture","object":"chat.completion.chunk","model":"queue-test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n')
            else:
                out = json.dumps({'id': 'fixture', 'object': 'chat.completion', 'model': 'queue-test', 'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': 'OK'}, 'finish_reason': 'stop'}], 'usage': {'prompt_tokens': 1, 'completion_tokens': 1, 'total_tokens': 2}}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(out)))
                self.end_headers()
                self.wfile.write(out)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with LOCK:
                ACTIVE -= 1


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def wait_until(fn, message, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except (OSError, KeyError, ValueError):
            pass
        time.sleep(.02)
    raise AssertionError(message)


upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()
binary = str(pathlib.Path(sys.argv[1]).resolve())
library = pathlib.Path(sys.argv[2]).resolve()
with tempfile.TemporaryDirectory(prefix='cpa-queue-integration-') as temp:
    root = pathlib.Path(temp)
    plugins = root / 'plugins' / 'linux' / 'amd64'
    plugins.mkdir(parents=True)
    shutil.copy2(library, plugins / 'cpa-account-queue-v0.1.0.so')
    port = free_port()
    policy_file = root / 'policies.json'
    policy_file.write_text(json.dumps({'version': 1, 'defaults': {'max_concurrency': 1, 'max_queue': 3, 'wait_timeout_ms': 5000, 'paused': False}, 'accounts': {}}))
    auths = root / 'auths'
    auths.mkdir()
    # Disabled dummy credential exercises account discovery without giving the
    # host a real token or allowing any traffic to an external provider.
    (auths / 'fixture-disabled.json').write_text(json.dumps({'type': 'claude', 'access_token': 'fixture-not-a-real-token', 'email': 'fixture@example.invalid', 'disabled': True}))
    config = {
        'config-version': 8,
        'server': {'host': '127.0.0.1', 'port': port, 'response-compression': {'enabled': False}},
        'management': {'allow-remote': False, 'secret-key': 'fixture-management-key', 'disable-control-panel': True},
        'access': {'api-keys': ['fixture-client-key']},
        'routing': {'strategy': 'round-robin', 'session-affinity': False, 'retry': {'request-retry': 0}},
        'plugins': {'enabled': True, 'dir': str(root / 'plugins'), 'configs': {'cpa-account-queue': {'enabled': True, 'state_path': str(policy_file)}}},
        'openai-compatibility': [{'name': 'fixture', 'base-url': f'http://127.0.0.1:{upstream.server_port}/v1', 'api-key-entries': [{'api-key': 'fixture-upstream-key'}], 'models': [{'name': 'queue-test', 'alias': 'queue-test'}]}],
        'auth-dir': str(root / 'auths'),
    }
    config_path = root / 'config.yaml'
    config_path.write_text(yaml.safe_dump(config))
    logfile = (root / 'host.log').open('w+')
    env = os.environ.copy()
    for key in list(env):
        if key.lower() in ('http_proxy', 'https_proxy', 'all_proxy'):
            env.pop(key)
    env['NO_PROXY'] = '127.0.0.1,localhost'
    proc = subprocess.Popen([binary, '--config', str(config_path)], cwd=root, stdout=logfile, stderr=subprocess.STDOUT, env=env)
    base = f'http://127.0.0.1:{port}'
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def api(path, method='GET', body=None, management=True):
        headers = {'Authorization': 'Bearer ' + ('fixture-management-key' if management else 'fixture-client-key'), 'Content-Type': 'application/json'}
        req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
        try:
            with opener.open(req, timeout=10) as response:
                raw = response.read()
                return response.status, raw
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def status():
        code, raw = api('/v0/management/plugins/cpa-account-queue/status')
        assert code == 200, (code, raw)
        return json.loads(raw)

    def chat(text, stream=False):
        return api('/v1/chat/completions', 'POST', {'model': 'queue-test', 'messages': [{'role': 'user', 'content': text}], 'stream': stream}, False)

    def set_defaults(queue, wait_ms):
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies')
        assert code == 200
        current = json.loads(raw)
        current['config']['defaults']['max_queue'] = queue
        current['config']['defaults']['wait_timeout_ms'] = wait_ms
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies', 'PUT', current)
        assert code == 200, (code, raw)

    try:
        wait_until(lambda: api('/v1/models', management=False)[0] == 200, 'CPA did not start')
        initial = status()
        print('HOST_INITIAL', json.dumps(initial))
        assert initial['account_index_available'], initial
        assert any(a['name'] == 'fixture-disabled.json' for a in initial['account_index']), initial
        # Public resource must not disclose state; management routes require auth.
        req = urllib.request.Request(base + '/v0/management/plugins/cpa-account-queue/status')
        try:
            opener.open(req, timeout=3)
            raise AssertionError('management route accepted no key')
        except urllib.error.HTTPError as error:
            assert error.code in (401, 403), error.code
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(chat, 'hold-first', True)
            wait_until(lambda: len(ARRIVALS) == 1, 'first request never reached fake upstream')
            second = pool.submit(chat, 'second')
            wait_until(lambda: status()['runtime']['waiting'] == 1, 'second request never queued')
            third = pool.submit(chat, 'third')
            wait_until(lambda: status()['runtime']['waiting'] == 2, 'third request never queued')
            with LOCK:
                assert len(ARRIVALS) == 1 and ACTIVE == 1, (ARRIVALS, ACTIVE)
            RELEASE.set()
            for future in (first, second, third):
                code, body = future.result(timeout=10)
                assert code == 200 and b'OK' in body, (code, body)
        wait_until(lambda: all(a['in_flight'] == 0 and a['queued'] == 0 for a in status()['runtime']['accounts']), 'slots did not release')
        assert PEAK == 1, ('upstream oversubscribed', PEAK)
        assert ARRIVALS == ['hold-first', 'second', 'third'], ARRIVALS
        # A client closes its HTTP socket while still queued. CPA must deliver
        # terminal notification promptly, not after the active slot is freed.
        RELEASE.clear()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            held = pool.submit(chat, 'hold-cancellation')
            wait_until(lambda: len(ARRIVALS) == 4, 'cancellation fixture did not start')
            payload = json.dumps({'model': 'queue-test', 'messages': [{'role': 'user', 'content': 'canceled-waiter'}]}).encode()
            sock = socket.create_connection(('127.0.0.1', port), timeout=3)
            raw = (f'POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer fixture-client-key\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode() + payload
            sock.sendall(raw)
            wait_until(lambda: status()['runtime']['waiting'] == 1, 'cancel fixture did not queue')
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            wait_until(lambda: status()['runtime']['waiting'] == 0, 'disconnected waiter retained in queue', timeout=2)
            with LOCK:
                assert 'canceled-waiter' not in ARRIVALS, ARRIVALS
            RELEASE.set()
            assert held.result(timeout=10)[0] == 200
        wait_until(lambda: all(a['in_flight'] == 0 for a in status()['runtime']['accounts']), 'cancellation fixture leaked slots')
        # Capacity and timeout errors must not poison the whole plugin. Prove
        # the first free slot accepts work again after each rejection.
        for queue, wait_ms, label in [(0, 1000, 'full'), (2, 100, 'timeout')]:
            set_defaults(queue, wait_ms)
            RELEASE.clear()
            before = len(ARRIVALS)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                held = pool.submit(chat, 'hold-' + label)
                wait_until(lambda: len(ARRIVALS) == before + 1, 'rejection fixture did not start')
                code, body = chat('rejected-' + label)
                assert code == 503, (label, code, body)
                assert len(ARRIVALS) == before + 1, 'rejected request reached provider'
                wait_until(lambda: status()['runtime']['waiting'] == 0, 'rejected waiter leaked')
                RELEASE.set()
                assert held.result(timeout=10)[0] == 200
            wait_until(lambda: all(a['in_flight'] == 0 for a in status()['runtime']['accounts']), 'post-rejection release stuck')
            code, body = chat('after-' + label)
            assert code == 200 and b'OK' in body, (label, code, body)
        print('Pinned CPA integration passed: actual loading, authenticated management, stream-held slot, FIFO, disconnected waiter cleanup, queue-full/timeout rejection and recovery, peak upstream concurrency=1')
    except BaseException:
        logfile.flush()
        print('--- isolated CPA log ---')
        print((root / 'host.log').read_text(errors='replace')[-18000:])
        raise
    finally:
        RELEASE.set()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        logfile.close()
upstream.shutdown()
