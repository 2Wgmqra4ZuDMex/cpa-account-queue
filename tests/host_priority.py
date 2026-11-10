"""Black-box CPA integration for scheduling across credential priority tiers.

Usage: python tests/host_priority.py /path/to/cli-proxy-api /path/to/plugin.so

Two fake upstream credentials serve one model: "team" in the highest priority
tier and "prolite" one tier below. The fake upstream records which credential
each request really ran on, so the plugin's accounting is checked against the
account the host executed on. Uses temporary credentials, loopback only.
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

KEYS = {'fixture-team-key': 'team', 'fixture-prolite-key': 'prolite'}
LOCK = threading.Lock()
ACTIVE = {'team': 0, 'prolite': 0}
PEAK = {'team': 0, 'prolite': 0}
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
        label = next((part.split('"')[0].split('\\')[0] for part in text.split('fixture-req-')[1:]), '?')
        with LOCK:
            ACTIVE[account] += 1
            PEAK[account] = max(PEAK[account], ACTIVE[account])
            ARRIVALS.append((account, label))
        try:
            if 'fixture-hold' in text:
                assert RELEASE.wait(20), 'upstream fixture was not released'
            else:
                time.sleep(.3)
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
with tempfile.TemporaryDirectory(prefix='cpa-queue-priority-') as temp:
    root = pathlib.Path(temp)
    plugins = root / 'plugins' / 'linux' / 'amd64'
    plugins.mkdir(parents=True)
    shutil.copy2(library, plugins / 'cpa-account-queue-v0.1.0.so')
    port = free_port()
    policy_file = root / 'policies.json'
    policy_file.write_text(json.dumps({'version': 1, 'defaults': {'max_concurrency': 1, 'max_queue': 2, 'wait_timeout_ms': 700, 'paused': False}, 'accounts': {}}))
    (root / 'auths').mkdir()
    base_url = f'http://127.0.0.1:{upstream.server_port}'
    models = [{'name': 'fixture-model', 'alias': 'queue-test'}]
    config = {
        'config-version': 8,
        'server': {'host': '127.0.0.1', 'port': port, 'response-compression': {'enabled': False}},
        'management': {'allow-remote': False, 'secret-key': 'fixture-management-key', 'disable-control-panel': True},
        'access': {'api-keys': ['fixture-client-key']},
        # The production routing settings under which the failure occurred.
        'routing': {'strategy': 'weighted-round-robin', 'session-affinity': True, 'retry': {'request-retry': 0}},
        'plugins': {'enabled': True, 'dir': str(root / 'plugins'), 'configs': {'cpa-account-queue': {'enabled': True, 'state_path': str(policy_file)}}},
        'api-keys': {'claude': [
            {'name': 'team', 'priority': 0, 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-team-key'}]},
            {'name': 'prolite', 'priority': -10, 'base-url': base_url, 'models': models, 'keys': [{'api-key': 'fixture-prolite-key'}]},
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
    sequence = iter(range(1, 1000))

    def api(path, method='GET', body=None, management=True):
        headers = {'Authorization': 'Bearer ' + ('fixture-management-key' if management else 'fixture-client-key'), 'Content-Type': 'application/json'}
        if path == '/v1/messages':
            headers['anthropic-version'] = '2023-06-01'
        req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
        try:
            with opener.open(req, timeout=30) as response:
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

    def message(hold=False):
        # Every request is a distinct conversation, as separate clients would be.
        label = f'fixture-req-{next(sequence)}' + ('-fixture-hold' if hold else '')
        return api('/v1/messages', 'POST', {'model': 'queue-test', 'max_tokens': 16, 'messages': [{'role': 'user', 'content': label}]}, False)

    def update(change):
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies')
        assert code == 200, (code, raw)
        current = json.loads(raw)
        change(current['config'])
        code, raw = api('/v0/management/plugins/cpa-account-queue/policies', 'PUT', current)
        assert code == 200, (code, raw)

    def arrivals(account):
        with LOCK:
            return sum(1 for a, _ in ARRIVALS if a == account)

    def idle():
        return all(a['in_flight'] == 0 and a['queued'] == 0 for a in rows().values())

    try:
        try:
            wait_until(lambda: b'queue-test' in api('/v1/models', management=False)[1], 'CPA did not publish the fixture model', timeout=20)
        except AssertionError:
            print('MODELS', api('/v1/models', management=False))
            raise

        # 1. Default "queue" mode keeps the host rule that only the highest
        #    priority tier is used: a second request waits behind Team and times
        #    out even though ProLite is idle. This is the production failure.
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            held = pool.submit(message, True)
            wait_until(lambda: arrivals('team') == 1, 'idle request did not run on the highest priority credential')
            team_id = next(i for i, a in rows().items() if a['in_flight'] == 1)
            started = time.monotonic()
            code, body = message()
            waited = time.monotonic() - started
            assert code == 503 and b'account_queue_timeout' in body, (code, body)
            assert .5 < waited < 3, waited
            assert arrivals('prolite') == 0, ARRIVALS
            RELEASE.set()
            assert held.result(timeout=10)[0] == 200
        wait_until(idle, 'queue-mode slots did not release')
        assert counters()['priority_overflow'] == 0 and counters()['timed_out'] == 1, counters()
        print('QUEUE_MODE', json.dumps({'second_status': code, 'waited_s': round(waited, 2), 'arrivals': ARRIVALS}))

        # 2. "fallback" mode: the same situation now overflows to ProLite at
        #    once, and the plugin accounts for the credential that really ran.
        update(lambda c: c.update({'pinned_full_behavior': 'fallback'}) or c['defaults'].update({'wait_timeout_ms': 6000}))
        RELEASE.clear()
        before = counters()
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            first = pool.submit(message, True)
            wait_until(lambda: arrivals('team') == 2, 'first fallback-mode request did not run on Team')
            second = pool.submit(message, True)
            wait_until(lambda: arrivals('prolite') == 1, 'request did not overflow to the lower priority credential', timeout=4)
            state = rows()
            prolite_id = next(i for i, a in state.items() if i != team_id and a['in_flight'] == 1)
            assert state[team_id]['in_flight'] == 1 and state[team_id]['queued'] == 0, state
            with LOCK:
                assert ACTIVE == {'team': 1, 'prolite': 1}, ACTIVE
            now = counters()
            assert now['priority_overflow'] == before['priority_overflow'] + 1 and now['enqueued'] == before['enqueued'], now

            # 3. Per-account limits still hold on the overflow account, and with
            #    every tier full the request queues instead of oversubscribing.
            update(lambda c: c['accounts'].update({prolite_id: {'max_concurrency': 2, 'max_queue': 2, 'wait_timeout_ms': 6000, 'paused': False}}))
            third = pool.submit(message, True)
            wait_until(lambda: arrivals('prolite') == 2, 'second overflow request did not use the free ProLite slot', timeout=4)
            fourth = pool.submit(message, True)
            wait_until(lambda: status()['runtime']['waiting'] == 1, 'request did not queue when every account was full', timeout=4)
            with LOCK:
                assert ACTIVE == {'team': 1, 'prolite': 2}, ACTIVE
            assert counters()['no_free_account'] == before['no_free_account'] + 1, counters()
            RELEASE.set()
            for future in (first, second, third, fourth):
                code, body = future.result(timeout=15)
                assert code == 200 and b'OK' in body, (code, body)
        wait_until(idle, 'fallback-mode slots did not release')
        assert PEAK == {'team': 1, 'prolite': 2}, PEAK
        print('FALLBACK_MODE', json.dumps({'peak': PEAK, 'counters': counters()}))

        # 4. Simultaneous requests must spread over the free slots instead of
        #    all choosing the one free Team slot and queueing behind it. Every
        #    request is held upstream, so no slot frees before all have arrived.
        RELEASE.clear()
        before = counters()
        seen = len(ARRIVALS)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            burst = [pool.submit(message, True) for _ in range(3)]
            wait_until(lambda: len(ARRIVALS) == seen + 3, 'simultaneous requests did not all reach the upstream', timeout=4)
            with LOCK:
                assert ACTIVE == {'team': 1, 'prolite': 2}, ACTIVE
            now = counters()
            assert now['enqueued'] == before['enqueued'] and now['timed_out'] == before['timed_out'], (before, now)
            RELEASE.set()
            for future in burst:
                code, body = future.result(timeout=15)
                assert code == 200 and b'OK' in body, (code, body)
        wait_until(idle, 'burst slots did not release')
        assert PEAK == {'team': 1, 'prolite': 2}, PEAK

        # 5. The highest tier is preferred again as soon as it is free. The
        #    host reports completion asynchronously, so wait for each release.
        seen = len(ARRIVALS)
        for _ in range(3):
            assert message()[0] == 200
            wait_until(idle, 'sequential slot did not release')
        assert [a for a, _ in ARRIVALS[seen:]] == ['team'] * 3, ARRIVALS[seen:]
        wait_until(lambda: counters()['completed'] == 11, 'completion callbacks did not all arrive')
        final = counters()
        assert final['admitted'] == 11 and final['timed_out'] == 1, final
        print('Priority tier integration passed: queue mode stays in the highest tier and times out; fallback mode overflows to the lower tier, keeps per-account limits, queues when all are full, spreads simultaneous requests, and returns to the highest tier', json.dumps({'peak': PEAK, 'counters': final}))
    except BaseException:
        logfile.flush()
        print('--- isolated CPA log ---')
        print('\n'.join(line for line in (root / 'host.log').read_text(errors='replace').splitlines() if '"/v1/models"' not in line and '/status"' not in line)[-18000:])
        print('ARRIVALS', ARRIVALS, 'PEAK', PEAK)
        try:
            for event in status()['runtime']['recent_events']:
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
