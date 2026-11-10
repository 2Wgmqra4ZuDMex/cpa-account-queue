"""Black-box CPA integration for session affinity under the plugin scheduler.

Usage: python tests/host_affinity.py /path/to/cli-proxy-api /path/to/plugin.so

Two fake upstream credentials in the same priority tier serve one model. The
fake upstream records which credential each request really ran on, so a
conversation staying on one account is checked against what the host
executed, not against the plugin's own bookkeeping. Conversations are
identified the ways real clients do it: an explicit session header, a
prompt_cache_key, and nothing but the message history. Uses temporary
credentials, loopback only.
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

KEYS = {'fixture-key-a': 'a', 'fixture-key-b': 'b'}
LOCK = threading.Lock()
ACTIVE = {'a': 0, 'b': 0}
PEAK = {'a': 0, 'b': 0}
ARRIVALS = []
RELEASE = threading.Event()


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        key = self.headers.get('x-api-key') or self.headers.get('Authorization', '').removeprefix('Bearer ')
        account = KEYS.get(key.strip())
        if account is None or '/v1/messages' not in self.path or 'count_tokens' in self.path:
            self.send_response(404)
            self.end_headers()
            return
        text = raw.decode('utf-8', 'replace')
        # The label of the request being sent is the last one in the history.
        label = text.split('fixture-turn-')[-1].split('"')[0].split('\\')[0] if 'fixture-turn-' in text else '?'
        with LOCK:
            ACTIVE[account] += 1
            PEAK[account] = max(PEAK[account], ACTIVE[account])
            ARRIVALS.append((account, label))
        try:
            if label.endswith('-hold'):
                assert RELEASE.wait(20), 'upstream fixture was not released'
            else:
                time.sleep(.05)
            if json.loads(raw).get('stream'):
                events = [
                    ('message_start', {'type': 'message_start', 'message': {'id': 'msg_fixture', 'type': 'message', 'role': 'assistant', 'model': 'fixture-model', 'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': {'input_tokens': 1, 'output_tokens': 0}}}),
                    ('content_block_start', {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}}),
                    ('content_block_delta', {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'OK'}}),
                    ('content_block_stop', {'type': 'content_block_stop', 'index': 0}),
                    ('message_delta', {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn', 'stop_sequence': None}, 'usage': {'output_tokens': 1}}),
                    ('message_stop', {'type': 'message_stop'}),
                ]
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.end_headers()
                for name, data in events:
                    self.wfile.write(f'event: {name}\ndata: {json.dumps(data)}\n\n'.encode())
                self.wfile.flush()
            else:
                out = json.dumps({'id': 'msg_fixture', 'type': 'message', 'role': 'assistant', 'model': 'fixture-model', 'content': [{'type': 'text', 'text': 'OK'}], 'stop_reason': 'end_turn', 'stop_sequence': None, 'usage': {'input_tokens': 1, 'output_tokens': 1}}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(out)))
                self.end_headers()
                self.wfile.write(out)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with LOCK:
                ACTIVE[account] -= 1


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
with tempfile.TemporaryDirectory(prefix='cpa-queue-affinity-') as temp:
    root = pathlib.Path(temp)
    plugins = root / 'plugins' / 'linux' / 'amd64'
    plugins.mkdir(parents=True)
    shutil.copy2(library, plugins / 'cpa-account-queue-v0.1.0.so')
    port = free_port()
    policy_file = root / 'policies.json'
    policy_file.write_text(json.dumps({'version': 1, 'defaults': {'max_concurrency': 2, 'max_queue': 2, 'wait_timeout_ms': 6000, 'paused': False}, 'accounts': {}}))
    (root / 'auths').mkdir()
    base_url = f'http://127.0.0.1:{upstream.server_port}'
    models = [{'name': 'fixture-model', 'alias': 'queue-test'}]
    config = {
        'config-version': 8,
        'server': {'host': '127.0.0.1', 'port': port, 'response-compression': {'enabled': False}},
        'management': {'allow-remote': False, 'secret-key': 'fixture-management-key', 'disable-control-panel': True},
        'access': {'api-keys': ['fixture-client-key']},
        'routing': {'strategy': 'weighted-round-robin', 'session-affinity': True, 'retry': {'request-retry': 0}},
        'plugins': {'enabled': True, 'dir': str(root / 'plugins'), 'configs': {'cpa-account-queue': {'enabled': True, 'state_path': str(policy_file)}}},
        'api-keys': {'claude': [
            {'name': 'a', 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-key-a'}]},
            {'name': 'b', 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-key-b'}]},
        ]},
        'auth-dir': str(root / 'auths'),
    }
    config_path = root / 'config.yaml'
    config_path.write_text(yaml.safe_dump(config))
    logfile = (root / 'host.log').open('w+')
    env = os.environ.copy()
    for name in list(env):
        if name.lower() in ('http_proxy', 'https_proxy', 'all_proxy'):
            env.pop(name)
    env['NO_PROXY'] = '127.0.0.1,localhost'
    proc = subprocess.Popen([binary, '--config', str(config_path)], cwd=root, stdout=logfile, stderr=subprocess.STDOUT, env=env)
    base = f'http://127.0.0.1:{port}'
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def api(path, method='GET', body=None, management=True, headers=None):
        send = {'Authorization': 'Bearer ' + ('fixture-management-key' if management else 'fixture-client-key'), 'Content-Type': 'application/json'}
        send.update(headers or {})
        req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=send, method=method)
        try:
            with opener.open(req, timeout=30) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def status():
        code, raw = api('/v0/management/plugins/cpa-account-queue/status')
        assert code == 200, (code, raw)
        return json.loads(raw)

    def counters():
        return status()['runtime']['counters']

    def idle():
        return all(a['in_flight'] == 0 and a['queued'] == 0 for a in status()['runtime']['accounts'])

    def update(change):
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies')
        assert code == 200, (code, raw)
        current = json.loads(raw)
        change(current['config'])
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies', 'PUT', current)
        assert code == 200, (code, raw)

    class Conversation:
        """One client conversation; each turn resends the whole history."""

        def __init__(self, name, kind):
            self.name, self.kind, self.turns, self.accounts = name, kind, 0, []

        def send(self, hold=False):
            self.turns += 1
            label = f'{self.name}-{self.turns}' + ('-hold' if hold else '')
            history = []
            for index in range(1, self.turns):
                history += [{'role': 'user', 'content': f'fixture-turn-{self.name}-{index}'}, {'role': 'assistant', 'content': 'OK'}]
            history.append({'role': 'user', 'content': 'fixture-turn-' + label})
            if self.kind == 'header':
                # Explicit session header, as coding agents send.
                code, raw = api('/v1/messages', 'POST', {'model': 'queue-test', 'max_tokens': 16, 'messages': history}, False, {'anthropic-version': '2023-06-01', 'X-Session-ID': 'fixture-session-' + self.name})
            elif self.kind == 'cache-key':
                # Responses API with a prompt_cache_key, as Codex-style clients send.
                items = [{'role': m['role'], 'content': [{'type': 'input_text' if m['role'] == 'user' else 'output_text', 'text': m['content']}]} for m in history]
                code, raw = api('/v1/responses', 'POST', {'model': 'queue-test', 'input': items, 'prompt_cache_key': 'fixture-cache-' + self.name}, False)
            else:
                # No identifier at all: the host derives one from the history.
                code, raw = api('/v1/chat/completions', 'POST', {'model': 'queue-test', 'messages': [{'role': 'system', 'content': 'fixture-system-' + self.name}] + history}, False)
            assert code == 200 and b'OK' in raw, (self.name, self.kind, code, raw[:400])
            wait_until(lambda: any(l == label for _, l in ARRIVALS), f'{label} never reached the upstream')
            with LOCK:
                account = next(a for a, l in ARRIVALS if l == label)
            self.accounts.append(account)
            return account

    def settle():
        wait_until(idle, 'slots did not release')

    try:
        try:
            wait_until(lambda: b'queue-test' in api('/v1/models', management=False)[1], 'CPA did not publish the fixture model', timeout=20)
        except AssertionError:
            print('MODELS', api('/v1/models', management=False))
            raise

        # 1. Each conversation stays on the account it started on, however the
        #    client identifies it, while conversations spread over both accounts.
        talks = [Conversation('h1', 'header'), Conversation('h2', 'header'), Conversation('c1', 'cache-key'), Conversation('c2', 'cache-key'), Conversation('m1', 'history'), Conversation('m2', 'history')]
        for _ in range(4):
            for talk in talks:
                talk.send()
                settle()
        placement = {talk.name: ''.join(talk.accounts) for talk in talks}
        for talk in talks:
            assert len(set(talk.accounts)) == 1, ('conversation changed account', placement)
        used = {talk.accounts[0] for talk in talks}
        assert used == {'a', 'b'}, ('conversations did not spread over both accounts', placement)
        now = counters()
        assert now['affinity_new'] == 6 and now['affinity_kept'] == 18 and now['affinity_moved'] == 0 and now['affinity_none'] == 0, now
        print('STICKY', json.dumps(placement))

        # 2. A conversation whose account is full does not wait: it moves to
        #    the free account and its later turns stay there.
        update(lambda c: c['defaults'].update({'max_concurrency': 1}))
        mover = talks[0]
        home = mover.accounts[0]
        other = 'b' if home == 'a' else 'a'
        blocker = next(talk for talk in talks if talk.accounts[0] == home and talk is not mover)
        RELEASE.clear()
        before = counters()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            held = pool.submit(blocker.send, True)
            wait_until(lambda: ACTIVE[home] == 1, 'blocking turn did not start on the shared account')
            assert mover.send() == other, ('conversation waited behind its full account', mover.accounts)
            now = counters()
            assert now['affinity_moved'] == before['affinity_moved'] + 1 and now['enqueued'] == before['enqueued'], (before, now)
            RELEASE.set()
            assert held.result(timeout=15) == home
        settle()
        assert mover.send() == other, ('conversation did not follow its latest account', mover.accounts)
        settle()
        assert PEAK[home] <= 2 and PEAK[other] <= 2, PEAK
        print('MOVED', json.dumps({'conversation': mover.name, 'from': home, 'to': other, 'accounts': ''.join(mover.accounts)}))

        # 3. Switched off, the binding is ignored: every request starts on the
        #    first account the host offers, whichever conversation it belongs
        #    to. That is the account the very first conversation started on;
        #    which credential it is varies, as host IDs include the upstream port.
        update(lambda c: c.update({'disable_session_affinity': True}))
        now = counters()
        seen = len(ARRIVALS)
        for talk in talks:
            talk.send()
            settle()
        assert {a for a, _ in ARRIVALS[seen:]} == {home}, (home, ARRIVALS[seen:])
        after = counters()
        assert all(after[k] == now[k] for k in ('affinity_kept', 'affinity_moved', 'affinity_new', 'affinity_none')), (now, after)
        print('Session affinity integration passed: conversations identified by header, prompt_cache_key and message history each stay on one account and spread over both; a conversation moves instead of waiting when its account is full and then follows the new account; switching affinity off restores first-idle selection', json.dumps({'placement': placement, 'counters': after}))
    except BaseException:
        logfile.flush()
        print('--- isolated CPA log ---')
        print('\n'.join(line for line in (root / 'host.log').read_text(errors='replace').splitlines() if '"/v1/models"' not in line and '/status"' not in line)[-12000:])
        print('ARRIVALS', ARRIVALS, 'PEAK', PEAK)
        try:
            for event in status()['runtime']['recent_events'][-60:]:
                print('EVENT', event['at'][11:23], event['kind'], event.get('account_tag', '-'), event.get('request_tag', '-'), event.get('wait_ms', 0), event.get('detail', ''))
        except Exception as error:
            print('EVENTS unavailable', error)
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
