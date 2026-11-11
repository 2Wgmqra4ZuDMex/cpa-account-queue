"""Black-box CPA integration for "host selection": the host's own selector
picks the account and the plugin only limits and queues on it.

Usage: python tests/host_selection.py /path/to/cli-proxy-api /path/to/plugin.so

Three fake upstream credentials serve one model: "a" and "b" in the highest
priority tier and "c" one tier below. The fake upstream records which
credential each request really ran on. The host runs with the production
routing settings (weighted round robin with session affinity).

Checked with host selection on:
  - the host's session affinity works while the plugin is registered;
  - a sub-agent follows its parent's account;
  - a conversation without any identifier returns to its account after its
    history is compacted (synthetic compaction: one summary turn followed by
    the last turns verbatim);
  - the per-account limit still holds, so a request waits behind its busy
    account even while other accounts are idle, and a busy top tier does not
    overflow to the lower tier.
Then plugin selection is switched back on and the same busy top tier
overflows to the lower tier at once.

Uses temporary credentials, loopback only.
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

KEYS = {'fixture-key-a': 'a', 'fixture-key-b': 'b', 'fixture-key-c': 'c'}
LOCK = threading.Lock()
ACTIVE = {'a': 0, 'b': 0, 'c': 0}
PEAK = {'a': 0, 'b': 0, 'c': 0}
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
                assert RELEASE.wait(30), 'upstream fixture was not released'
            else:
                time.sleep(.03)
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
        except (OSError, KeyError, ValueError, TypeError):
            pass
        time.sleep(.02)
    raise AssertionError(message)


upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()
binary = str(pathlib.Path(sys.argv[1]).resolve())
library = pathlib.Path(sys.argv[2]).resolve()
with tempfile.TemporaryDirectory(prefix='cpa-queue-selection-') as temp:
    root = pathlib.Path(temp)
    plugins = root / 'plugins' / 'linux' / 'amd64'
    plugins.mkdir(parents=True)
    shutil.copy2(library, plugins / 'cpa-account-queue-v0.1.0.so')
    port = free_port()
    policy_file = root / 'policies.json'
    policy_file.write_text(json.dumps({'version': 1, 'defaults': {'max_concurrency': 1, 'max_queue': 4, 'wait_timeout_ms': 15000, 'paused': False}, 'accounts': {}, 'host_selection': True}))
    (root / 'auths').mkdir()
    base_url = f'http://127.0.0.1:{upstream.server_port}'
    models = [{'name': 'fixture-model', 'alias': 'queue-test'}]
    config = {
        'config-version': 8,
        'server': {'host': '127.0.0.1', 'port': port, 'response-compression': {'enabled': False}},
        'management': {'allow-remote': False, 'secret-key': 'fixture-management-key', 'disable-control-panel': True},
        'access': {'api-keys': ['fixture-client-key']},
        # The production routing settings.
        'routing': {'strategy': 'weighted-round-robin', 'session-affinity': True, 'retry': {'request-retry': 0}},
        'plugins': {'enabled': True, 'dir': str(root / 'plugins'), 'configs': {'cpa-account-queue': {'enabled': True, 'state_path': str(policy_file)}}},
        'api-keys': {'claude': [
            {'name': 'a', 'priority': 0, 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-key-a'}]},
            {'name': 'b', 'priority': 0, 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-key-b'}]},
            {'name': 'c', 'priority': -10, 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-key-c'}]},
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
            with opener.open(req, timeout=40) as response:
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

    def settle():
        wait_until(idle, 'slots did not release')

    def update(change):
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies')
        assert code == 200, (code, raw)
        current = json.loads(raw)
        change(current['config'])
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies', 'PUT', current)
        assert code == 200, (code, raw)

    def host_log():
        logfile.flush()
        return (root / 'host.log').read_text(errors='replace')

    def log_count(text):
        return host_log().count(text)

    def account_of(label):
        with LOCK:
            return next((a for a, l in ARRIVALS if l == label), None)

    class Conversation:
        """One client conversation; each turn resends the whole history."""

        def __init__(self, name, kind, parent=None):
            self.name, self.kind, self.parent = name, kind, parent
            self.turns, self.history, self.accounts = 0, [], []

        def post(self, label):
            messages = self.history + [{'role': 'user', 'content': 'fixture-turn-' + label}]
            if self.kind == 'header':
                # An explicit session header, as coding agents send; a
                # sub-agent also names its parent.
                headers = {'anthropic-version': '2023-06-01', 'X-Session-ID': 'fixture-session-' + self.name}
                if self.parent:
                    headers['X-Parent-Session-ID'] = 'fixture-session-' + self.parent
                return api('/v1/messages', 'POST', {'model': 'queue-test', 'max_tokens': 16, 'messages': messages}, False, headers)
            # No identifier at all: only the history tells conversations apart.
            return api('/v1/chat/completions', 'POST', {'model': 'queue-test', 'messages': [{'role': 'system', 'content': 'fixture-system-' + self.name}] + messages}, False)

        def send(self, hold=False):
            self.turns += 1
            label = f'{self.name}-{self.turns}' + ('-hold' if hold else '')
            code, raw = self.post(label)
            assert code == 200 and b'OK' in raw, (self.name, self.kind, code, raw[:400])
            self.history += [{'role': 'user', 'content': 'fixture-turn-' + label}, {'role': 'assistant', 'content': 'OK'}]
            account = account_of(label)
            assert account, f'{label} never reached the upstream'
            self.accounts.append(account)
            return account

        def compact(self, keep_exchanges=2):
            # What a client does when its context fills up: the early turns
            # become one summary turn, the most recent ones stay verbatim.
            self.history = [{'role': 'user', 'content': 'Summary of the earlier conversation: ' + self.name}] + self.history[-2 * keep_exchanges:]

    try:
        try:
            wait_until(lambda: b'queue-test' in api('/v1/models', management=False)[1], 'CPA did not publish the fixture model', timeout=20)
        except AssertionError:
            print('MODELS', api('/v1/models', management=False))
            raise

        # 1. The host's own affinity keeps each conversation on one account
        #    and spreads conversations over the top tier.
        parents = [Conversation(f'p{i}', 'header') for i in range(1, 5)]
        for _ in range(3):
            for talk in parents:
                talk.send()
                settle()
        placement = {talk.name: ''.join(talk.accounts) for talk in parents}
        for talk in parents:
            assert len(set(talk.accounts)) == 1, ('conversation changed account under host selection', placement)
        assert {talk.accounts[0] for talk in parents} == {'a', 'b'}, ('conversations did not spread over the top tier', placement)
        now = counters()
        assert now['host_picks'] >= 12 and now['affinity_new'] + now['affinity_kept'] + now['affinity_moved'] + now['affinity_none'] == 0, now
        print('HOST_AFFINITY', json.dumps(placement))

        # 2. A sub-agent runs on its parent's account. The order makes two
        #    consecutive new sessions expect the same account, which plain
        #    round robin would not give.
        inherited = log_count('session-affinity: fallback cache hit')
        children = {}
        for parent in sorted(parents, key=lambda talk: talk.accounts[0]):
            child = Conversation('child-of-' + parent.name, 'header', parent=parent.name)
            children[child.name] = (child.send(), parent.accounts[0])
            settle()
        assert all(got == want for got, want in children.values()), ('sub-agent left its parent account', children)
        assert log_count('session-affinity: fallback cache hit') >= inherited + 4, 'host log does not show the sub-agents inheriting'
        print('SUBAGENT', json.dumps({name: got for name, (got, _) in children.items()}))

        # 3. Conversations without any identifier: sticky by their history,
        #    and still on their account after the history is compacted.
        plain = [Conversation(f'm{i}', 'history') for i in range(1, 5)]
        for _ in range(4):
            for talk in plain:
                talk.send()
                settle()
        for talk in plain:
            assert len(set(talk.accounts)) == 1, ('identifier-less conversation changed account', {t.name: ''.join(t.accounts) for t in plain})
        compactions = log_count('session-affinity: LCP compaction hit')
        compacted = {}
        for talk in sorted(plain, key=lambda t: t.accounts[0]):
            before = talk.accounts[0]
            talk.compact()
            after_compaction = talk.send()
            settle()
            next_turn = talk.send()
            settle()
            compacted[talk.name] = (before, after_compaction, next_turn)
        assert all(len(set(v)) == 1 for v in compacted.values()), ('compaction moved a conversation', compacted)
        assert {v[0] for v in compacted.values()} == {'a', 'b'}, ('identifier-less conversations did not spread', compacted)
        assert log_count('session-affinity: LCP compaction hit') >= compactions + 4, 'host log does not show the compaction continuations'
        print('COMPACTION', json.dumps({name: ''.join(v) for name, v in compacted.items()}))

        # 4. The limit still holds on the account the host chose: a second
        #    request of the same conversation waits behind the first, even
        #    though the other accounts are idle.
        on_a = next(talk for talk in parents if talk.accounts[0] == 'a')
        on_b = next(talk for talk in parents if talk.accounts[0] == 'b')
        RELEASE.clear()
        before = counters()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            held = pool.submit(on_a.send, True)
            wait_until(lambda: ACTIVE['a'] == 1, 'held turn did not start on its account')
            label = f'{on_a.name}-second'
            second = pool.submit(on_a.post, label)
            wait_until(lambda: status()['runtime']['waiting'] == 1, 'second request of the conversation did not queue')
            with LOCK:
                assert ACTIVE == {'a': 1, 'b': 0, 'c': 0}, ACTIVE
            assert account_of(label) is None, 'queued request reached the upstream'
            now = counters()
            assert now['enqueued'] == before['enqueued'] + 1 and now['queued_other_free'] == before['queued_other_free'] + 1, (before, now)
            RELEASE.set()
            assert held.result(timeout=20) == 'a'
            code, raw = second.result(timeout=20)
            assert code == 200 and b'OK' in raw, (code, raw[:300])
            assert account_of(label) == 'a', ('queued request did not run on the account it waited for', account_of(label))
        settle()

        # 5. A busy top tier does not overflow: a new conversation waits for
        #    "a" or "b" while "c" stays idle.
        RELEASE.clear()
        seen_c = sum(1 for a, _ in ARRIVALS if a == 'c')
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            held = [pool.submit(on_a.send, True), pool.submit(on_b.send, True)]
            wait_until(lambda: ACTIVE['a'] == 1 and ACTIVE['b'] == 1, 'top tier accounts did not both become busy')
            newcomer = Conversation('newcomer-host', 'header')
            waiting = pool.submit(newcomer.send)
            wait_until(lambda: status()['runtime']['waiting'] == 1, 'new conversation did not queue behind the busy top tier')
            time.sleep(.3)
            with LOCK:
                assert ACTIVE == {'a': 1, 'b': 1, 'c': 0}, ACTIVE
            RELEASE.set()
            assert waiting.result(timeout=20) in ('a', 'b')
            for future in held:
                future.result(timeout=20)
        settle()
        assert sum(1 for a, _ in ARRIVALS if a == 'c') == seen_c == 0, ARRIVALS
        host_mode = counters()
        assert host_mode['timed_out'] == 0 and host_mode['priority_overflow'] == 0, host_mode

        # 6. Back to plugin selection with overflow: the same busy top tier
        #    now sends the new conversation to "c" at once.
        update(lambda c: (c.pop('host_selection', None), c.update({'pinned_full_behavior': 'fallback'})))
        RELEASE.clear()
        before = counters()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            first, second = Conversation('plugin-first', 'header'), Conversation('plugin-second', 'header')
            held = [pool.submit(first.send, True)]
            wait_until(lambda: ACTIVE['a'] + ACTIVE['b'] == 1, 'first held turn did not start in the top tier')
            held.append(pool.submit(second.send, True))
            wait_until(lambda: ACTIVE['a'] == 1 and ACTIVE['b'] == 1, 'plugin selection did not use both top tier accounts')
            newcomer = Conversation('newcomer-plugin', 'header')
            assert newcomer.send() == 'c', ('plugin selection did not overflow to the lower tier', newcomer.accounts)
            now = counters()
            assert now['priority_overflow'] == before['priority_overflow'] + 1 and now['enqueued'] == before['enqueued'] and now['host_picks'] == before['host_picks'], (before, now)
            RELEASE.set()
            for future in held:
                future.result(timeout=20)
        settle()
        assert PEAK == {'a': 1, 'b': 1, 'c': 1}, PEAK
        print('Host selection integration passed: with the plugin declining the pick, the host keeps conversations on their account, sub-agents follow their parent and compacted identifier-less conversations return to their account; the per-account limit still holds, so requests wait behind a busy account and a busy top tier does not overflow; switching back to plugin selection overflows to the lower tier', json.dumps({'peak': PEAK, 'host_mode_counters': {k: host_mode[k] for k in ('host_picks', 'enqueued', 'admitted', 'queued_other_free', 'priority_overflow', 'timed_out')}}))
    except BaseException:
        text = host_log()
        print('--- isolated CPA log: session-affinity lines ---')
        print('\n'.join(line[:260] for line in text.splitlines() if 'session-affinity' in line)[-9000:])
        print('--- isolated CPA log: tail ---')
        print('\n'.join(line for line in text.splitlines() if '"/v1/models"' not in line and '/status"' not in line and 'session-affinity' not in line)[-5000:])
        print('ARRIVALS', ARRIVALS[-40:], 'PEAK', PEAK)
        try:
            for event in status()['runtime']['recent_events'][-30:]:
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
