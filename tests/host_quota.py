"""Black-box CPA integration for an account that runs out of quota while a
request is queued behind it.

Usage: python tests/host_quota.py /path/to/cli-proxy-api /path/to/plugin.so [--host-selection] [--stream]

Two fake upstream credentials in one priority tier serve one model, each
limited to one request at a time. A request is queued behind a busy account;
while it waits, that account starts answering 429 (usage limit reached). The
plugin cannot know this and admits the request there once the slot frees.

Expected: the host fails over to the other account, the plugin releases the
slot on the exhausted account and admits (or queues) the request on the other
one, and the client gets a normal answer.

  default            the plugin selects accounts; both are busy, so the
                     request waits again behind the other account
  --host-selection   the host's own selector picks; the other account is idle
  --stream           clients ask for a streamed answer

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

KEYS = {'fixture-key-a': 'a', 'fixture-key-b': 'b'}
LOCK = threading.Lock()
ACTIVE = {'a': 0, 'b': 0}
PEAK = {'a': 0, 'b': 0}
ARRIVALS = []
EXHAUSTED = set()
RELEASE = {'a': threading.Event(), 'b': threading.Event()}


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
        label = text.split('fixture-turn-')[-1].split('"')[0].split('\\')[0] if 'fixture-turn-' in text else '?'
        with LOCK:
            exhausted = account in EXHAUSTED
            ARRIVALS.append((account, label, 'rejected' if exhausted else 'served'))
            if not exhausted:
                ACTIVE[account] += 1
                PEAK[account] = max(PEAK[account], ACTIVE[account])
        if exhausted:
            out = json.dumps({'type': 'error', 'error': {'type': 'rate_limit_error', 'message': 'fixture: usage limit reached'}}).encode()
            self.send_response(429)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Retry-After', '3600')
            self.send_header('Content-Length', str(len(out)))
            self.end_headers()
            self.wfile.write(out)
            return
        try:
            if label.endswith('-hold'):
                assert RELEASE[account].wait(30), 'upstream fixture was not released'
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


args = [a for a in sys.argv[1:] if not a.startswith('--')]
host_selection = '--host-selection' in sys.argv
stream = '--stream' in sys.argv
mode = ('host selection' if host_selection else 'plugin selection') + (', streamed' if stream else ', not streamed')
binary = str(pathlib.Path(args[0]).resolve())
library = pathlib.Path(args[1]).resolve()

upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()
with tempfile.TemporaryDirectory(prefix='cpa-queue-quota-') as temp:
    root = pathlib.Path(temp)
    plugins = root / 'plugins' / 'linux' / 'amd64'
    plugins.mkdir(parents=True)
    shutil.copy2(library, plugins / 'cpa-account-queue-v0.1.0.so')
    port = free_port()
    policy_file = root / 'policies.json'
    policy = {'version': 1, 'defaults': {'max_concurrency': 1, 'max_queue': 4, 'wait_timeout_ms': 15000, 'paused': False}, 'accounts': {}}
    if host_selection:
        policy['host_selection'] = True
    policy_file.write_text(json.dumps(policy))
    (root / 'auths').mkdir()
    base_url = f'http://127.0.0.1:{upstream.server_port}'
    models = [{'name': 'fixture-model', 'alias': 'queue-test'}]
    config = {
        'config-version': 8,
        'server': {'host': '127.0.0.1', 'port': port, 'response-compression': {'enabled': False}},
        'management': {'allow-remote': False, 'secret-key': 'fixture-management-key', 'disable-control-panel': True},
        'access': {'api-keys': ['fixture-client-key']},
        # The production routing settings, including its retry rounds.
        'routing': {'strategy': 'weighted-round-robin', 'session-affinity': True, 'retry': {'request-retry': 3}},
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
            with opener.open(req, timeout=40) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def status():
        code, raw = api('/v0/management/plugins/cpa-account-queue/status')
        assert code == 200, (code, raw)
        return json.loads(raw)

    def rows():
        return {a['id']: a for a in status()['runtime']['accounts']}

    def counters():
        return status()['runtime']['counters']

    def settle():
        wait_until(lambda: all(a['in_flight'] == 0 and a['queued'] == 0 for a in rows().values()), 'slots did not release')

    def arrivals(label):
        with LOCK:
            return [(a, outcome) for a, l, outcome in ARRIVALS if l == label]

    class Conversation:
        """One client conversation with an explicit session header."""

        def __init__(self, name):
            self.name, self.turns, self.history = name, 0, []

        def post(self, label):
            messages = self.history + [{'role': 'user', 'content': 'fixture-turn-' + label}]
            body = {'model': 'queue-test', 'max_tokens': 16, 'messages': messages}
            if stream:
                body['stream'] = True
            return api('/v1/messages', 'POST', body, False, {'anthropic-version': '2023-06-01', 'X-Session-ID': 'fixture-session-' + self.name})

        def send(self, hold=False, label=None):
            """Returns the account that finally served the turn."""
            if label is None:
                self.turns += 1
                label = f'{self.name}-{self.turns}' + ('-hold' if hold else '')
            code, raw = self.post(label)
            assert code == 200 and b'OK' in raw, (label, code, raw[:400])
            self.history += [{'role': 'user', 'content': 'fixture-turn-' + label}, {'role': 'assistant', 'content': 'OK'}]
            served = [a for a, outcome in arrivals(label) if outcome == 'served']
            assert len(served) == 1, (label, arrivals(label))
            return served[0]

    try:
        try:
            wait_until(lambda: b'queue-test' in api('/v1/models', management=False)[1], 'CPA did not publish the fixture model', timeout=20)
        except AssertionError:
            print('MODELS', api('/v1/models', management=False))
            raise

        s1, s2, s3 = Conversation('s1'), Conversation('s2'), Conversation('s3')
        home = s1.send()
        other = 'b' if home == 'a' else 'a'
        settle()
        before = counters()

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            held_home = pool.submit(s1.send, True)
            wait_until(lambda: ACTIVE[home] == 1, 'held turn did not start on the conversation account')
            home_id = next(i for i, a in rows().items() if a['in_flight'] == 1)
            held_other = None
            if host_selection:
                # The host keeps the conversation on its account, so a second
                # request of it waits there while the other account is idle.
                waiter_label, waiter = 's1-second', pool.submit(s1.send, False, 's1-second')
            else:
                # The plugin only queues once every offered account is full.
                held_other = pool.submit(s2.send, True)
                wait_until(lambda: ACTIVE[other] == 1, 'second held turn did not start on the other account')
                waiter_label, waiter = 's3-1', pool.submit(s3.send, False, 's3-1')
            wait_until(lambda: status()['runtime']['waiting'] == 1, 'request did not queue')
            assert arrivals(waiter_label) == [], ('queued request already reached the upstream', arrivals(waiter_label))
            queued_id = next(i for i, a in rows().items() if a['queued'] == 1)
            victim, survivor = (home, other) if queued_id == home_id else (other, home)
            if host_selection:
                assert victim == home, 'the host did not keep the second request on the conversation account'

            # The account runs out of quota while the request waits behind it.
            with LOCK:
                EXHAUSTED.add(victim)
            RELEASE[victim].set()
            wait_until(lambda: (victim, 'rejected') in arrivals(waiter_label), 'queued request was never sent to the exhausted account')
            if not host_selection:
                # The other account is still busy: the request waits again,
                # now there, and no longer holds the exhausted account.
                wait_until(lambda: any(a['queued'] == 1 and i != queued_id for i, a in rows().items()), 'request did not queue behind the other account after the failover')
                state = rows()
                assert state[queued_id]['queued'] == 0, state[queued_id]
                assert arrivals(waiter_label) == [(victim, 'rejected')], arrivals(waiter_label)
            RELEASE[survivor].set()
            assert waiter.result(timeout=30) == survivor
            for future in (held_home, held_other):
                if future is not None:
                    future.result(timeout=30)
        settle()
        assert arrivals(waiter_label) == [(victim, 'rejected'), (survivor, 'served')], arrivals(waiter_label)
        with LOCK:
            rejected_so_far = sum(1 for _, _, outcome in ARRIVALS if outcome == 'rejected')

        # A conversation that was on the exhausted account carries on elsewhere.
        follower = s1 if victim == home else s2
        assert follower.send() == survivor
        settle()
        with LOCK:
            rejected_later = sum(1 for _, _, outcome in ARRIVALS if outcome == 'rejected') - rejected_so_far
        after = counters()
        assert after['timed_out'] == before['timed_out'] and after['queue_full'] == before['queue_full'], after
        assert PEAK['a'] <= 1 and PEAK['b'] <= 1, PEAK
        if not host_selection:
            # Both moves were forced by the host no longer offering the account.
            moved = {k: after[k] - before[k] for k in ('affinity_moved', 'affinity_moved_not_offered', 'affinity_moved_full', 'affinity_moved_paused')}
            assert moved == {'affinity_moved': 2, 'affinity_moved_not_offered': 2, 'affinity_moved_full': 0, 'affinity_moved_paused': 0}, moved
        print(f'Quota exhaustion integration passed ({mode}): a request queued behind an account that ran out of quota was sent there once, failed over, and was served on the other account; the exhausted account kept no slot; a conversation bound to it carried on elsewhere', json.dumps({'requests_sent_to_exhausted_account_afterwards': rejected_later, 'enqueued': after['enqueued'] - before['enqueued'], 'peak': PEAK}))
    except BaseException:
        logfile.flush()
        print('--- isolated CPA log ---')
        print('\n'.join(line[:400] for line in (root / 'host.log').read_text(errors='replace').splitlines() if '"/v1/models"' not in line and '/status"' not in line)[-9000:])
        print('ARRIVALS', ARRIVALS, 'PEAK', PEAK, 'EXHAUSTED', EXHAUSTED)
        try:
            for event in status()['runtime']['recent_events'][-30:]:
                print('EVENT', event['at'][11:23], event['kind'], event.get('account_tag', '-'), event.get('request_tag', '-'), event.get('wait_ms', 0), event.get('detail', ''))
        except Exception as error:
            print('EVENTS unavailable', error)
        raise
    finally:
        for event in RELEASE.values():
            event.set()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        logfile.close()
upstream.shutdown()
