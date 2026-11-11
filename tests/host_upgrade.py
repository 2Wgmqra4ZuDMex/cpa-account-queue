"""Black-box CPA integration for replacing the plugin while requests arrive.

Usage: python tests/host_upgrade.py /path/to/cli-proxy-api FROM.so TO.so [--between-requests | --expect-unregistered]

The host is started with FROM.so installed as one version. TO.so is then
installed as a newer version and the host is told to reload, as a store
install does; the host asks the running version to quiesce.

FROM.so and TO.so must differ in content: the host maps byte-identical
libraries to one in-memory image, which would not be a replacement.

  default               One request is held upstream and another is queued
                        when the new version is installed. The running
                        version refuses, keeps limiting and queueing, serves
                        the requests arriving while the host waits, and no
                        request fails. A config reload once it is idle
                        completes the replacement.
  --between-requests    Short requests arrive with small gaps. The running
                        version agrees in a gap, and the requests arriving
                        until the host has switched to the new version are
                        let through instead of failing.
  --expect-unregistered FROM.so is the published 0.2.0-dev. The defect is
                        reproduced: the plugin ends up unregistered and the
                        limit is gone.

Uses temporary credentials, loopback only.
"""
import calendar
import collections
import concurrent.futures
import hashlib
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
                assert RELEASE.wait(30), 'upstream fixture was not released'
            else:
                time.sleep(.05)
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
        except (OSError, KeyError, ValueError, TypeError):
            pass
        time.sleep(.02)
    raise AssertionError(message)


def event_time(event):
    # Event times are RFC 3339 in UTC with a variable number of fraction digits.
    whole, _, fraction = event['at'].rstrip('Z').partition('.')
    return calendar.timegm(time.strptime(whole, '%Y-%m-%dT%H:%M:%S')) + float('0.' + (fraction or '0'))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


args = [a for a in sys.argv[1:] if not a.startswith('--')]
expect_unregistered = '--expect-unregistered' in sys.argv
between_requests = '--between-requests' in sys.argv
binary = str(pathlib.Path(args[0]).resolve())
old_library = pathlib.Path(args[1]).resolve()
new_library = pathlib.Path(args[2]).resolve()
assert digest(old_library) != digest(new_library), 'FROM.so and TO.so are byte-identical; the host would reuse one image'
OLD, NEW = '0.1.0', '0.1.1'

upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()
with tempfile.TemporaryDirectory(prefix='cpa-queue-upgrade-') as temp:
    root = pathlib.Path(temp)
    plugins = root / 'plugins' / 'linux' / 'amd64'
    plugins.mkdir(parents=True)
    shutil.copy2(old_library, plugins / f'cpa-account-queue-v{OLD}.so')
    port = free_port()
    policy_file = root / 'policies.json'
    policy_file.write_text(json.dumps({'version': 1, 'defaults': {'max_concurrency': 1, 'max_queue': 20, 'wait_timeout_ms': 20000, 'paused': False}, 'accounts': {}}))
    (root / 'auths').mkdir()
    config = {
        'config-version': 8,
        'server': {'host': '127.0.0.1', 'port': port, 'response-compression': {'enabled': False}},
        'management': {'allow-remote': False, 'secret-key': 'fixture-management-key', 'disable-control-panel': True},
        'access': {'api-keys': ['fixture-client-key']},
        'routing': {'strategy': 'round-robin', 'session-affinity': False, 'retry': {'request-retry': 0}},
        'plugins': {'enabled': True, 'dir': str(root / 'plugins'), 'configs': {'cpa-account-queue': {'enabled': True, 'state_path': str(policy_file), 'store': {'version': OLD}}}},
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
            with opener.open(req, timeout=40) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def status():
        code, raw = api('/v0/management/plugins/cpa-account-queue/status')
        return (json.loads(raw) if code == 200 else None), code

    def chat(text):
        return api('/v1/chat/completions', 'POST', {'model': 'queue-test', 'messages': [{'role': 'user', 'content': text}]}, False)

    def host_log():
        logfile.flush()
        return (root / 'host.log').read_text(errors='replace')

    def set_enabled(value):
        code, raw = api('/v0/management/plugins/cpa-account-queue/enabled', 'PATCH', {'enabled': value})
        assert code == 200, (code, raw)

    def patch_config(change):
        # Saving the plugin's host configuration makes the host reload, which
        # is also what a store install does after writing the new file.
        code, raw = api('/v0/management/plugins/cpa-account-queue/config', 'PATCH', change)
        assert code == 200, (code, raw)

    def install_new_version():
        shutil.copy2(new_library, plugins / f'cpa-account-queue-v{NEW}.so')
        patch_config({'store': {'version': NEW}})

    def idle():
        return all(a['in_flight'] == 0 and a['queued'] == 0 for a in status()[0]['runtime']['accounts'])

    def new_version_limits(pool):
        RELEASE.clear()
        held = pool.submit(chat, 'hold-on-new-version')
        wait_until(lambda: 'hold-on-new-version' in ARRIVALS, 'request on the new version never reached the upstream')
        queued = pool.submit(chat, 'queued-on-new-version')
        wait_until(lambda: status()[0]['runtime']['waiting'] == 1, 'new version did not queue')
        with LOCK:
            assert ACTIVE == 1, ACTIVE
        RELEASE.set()
        assert held.result(timeout=30)[0] == 200 and queued.result(timeout=30)[0] == 200

    def reproduce_defect(pool, started_as):
        # The published 0.2.0-dev: the plugin is gone, so nothing limits the
        # account any more.
        held = pool.submit(chat, 'hold-during-upgrade')
        wait_until(lambda: len(ARRIVALS) == 1, 'held request never reached the upstream')
        queued = pool.submit(chat, 'queued-during-upgrade')
        wait_until(lambda: status()[0]['runtime']['waiting'] == 1, 'second request never queued')
        install_new_version()
        wait_until(lambda: 'plugin.reconfigure failed' in host_log(), 'host log never showed the failed rollback', timeout=15)
        time.sleep(.5)
        during_code = status()[1]
        assert during_code != 200, ('plugin still registered', during_code)
        late = pool.submit(chat, 'hold-after-upgrade')
        wait_until(lambda: len(ARRIVALS) >= 2, 'request after the failed upgrade never reached the upstream', timeout=10)
        with LOCK:
            oversubscribed = ACTIVE
        RELEASE.set()
        results = {'held': held.result(timeout=30)[0], 'queued': queued.result(timeout=30), 'late': late.result(timeout=30)[0]}
        assert oversubscribed >= 2, ('limit unexpectedly still enforced', oversubscribed)
        assert results['queued'][0] == 503 and b'plugin_draining' in results['queued'][1], results['queued']
        # Toggling the plugin off and on loads the new version.
        set_enabled(False)
        wait_until(lambda: 'pluginhost: plugin unloaded' in host_log(), 'plugin was not unloaded', timeout=10)
        set_enabled(True)
        wait_until(lambda: status()[1] == 200, 'plugin did not come back after toggling', timeout=15)
        print('REPRODUCED with the published 0.2.0-dev:', json.dumps({'started_as': started_as, 'status_code_after_upgrade': during_code, 'queued_request': results['queued'][0], 'upstream_concurrency_with_limit_1': oversubscribed, 'version_after_toggle': status()[0].get('version')}))

    def under_load(pool, started_as):
        held = pool.submit(chat, 'hold-during-upgrade')
        wait_until(lambda: len(ARRIVALS) == 1, 'held request never reached the upstream')
        queued = pool.submit(chat, 'queued-during-upgrade')
        wait_until(lambda: status()[0]['runtime']['waiting'] == 1, 'second request never queued')
        install_new_version()

        # Keep sending requests while the host waits for the running version
        # to quiesce.
        refused = 'pluginhost: plugin quiesce failed'
        extra = []
        give_up = time.monotonic() + 15
        while refused not in host_log():
            assert time.monotonic() < give_up, 'host log never showed the refused quiesce'
            if len(extra) < 12:
                extra.append(pool.submit(chat, f'during-upgrade-{len(extra)}'))
            time.sleep(.25)
        time.sleep(.3)

        # The running version refused and still serves.
        during, during_code = status()
        assert during_code == 200 and during['version'] == started_as, (during_code, during and during.get('version'))
        assert during['replacement_deferred_at'] and not during['runtime']['draining'] and not during['runtime']['handed_over'], during
        events = during['runtime']['recent_events']
        deferred = [event_time(e) for e in events if e['kind'] == 'replacement_deferred']
        assert deferred, [e['kind'] for e in events]
        asked = max(event_time(e) for e in events if e['kind'] == 'replacement_requested' and event_time(e) < deferred[-1])
        in_window = [e for e in events if e['kind'] == 'enqueued' and asked < event_time(e) < deferred[-1]]
        assert in_window, 'no request was queued while the host waited for the plugin to quiesce'
        with LOCK:
            assert ACTIVE == 1 and len(ARRIVALS) == 1, (ACTIVE, ARRIVALS)

        # A request arriving after the refused replacement queues too.
        later = pool.submit(chat, 'later-during-upgrade')
        wait_until(lambda: status()[0]['runtime']['waiting'] == 2 + len(extra), 'request after the refused replacement did not queue')
        RELEASE.set()
        for name, future in [('held', held), ('queued', queued), ('later', later)] + [(f'during-{i}', f) for i, f in enumerate(extra)]:
            code, body = future.result(timeout=30)
            assert code == 200 and b'OK' in body, (name, code, body)
        wait_until(idle, 'slots did not release')
        assert 'pluginhost: plugin hot reloaded' not in host_log(), 'the host replaced the plugin before it was asked to again'
        counters = status()[0]['runtime']['counters']
        assert counters['admitted'] == 3 + len(extra) and counters['draining_rejected'] == 0 and counters['handover_passed'] == 0 and counters['timed_out'] == 0, counters
        assert PEAK == 1, ('upstream oversubscribed', PEAK)

        # Idle now: the next config reload completes the replacement.
        patch_config({'pinned_full_behavior': 'queue'})
        wait_until(lambda: 'pluginhost: plugin hot reloaded' in host_log(), 'a config reload while idle did not replace the plugin', timeout=15)
        wait_until(lambda: status()[1] == 200, 'replacement did not register', timeout=15)
        after = status()[0]
        # A different library is a fresh image with none of the old state.
        assert after['runtime']['counters']['admitted'] == 0 and not after['replacement_deferred_at'], after
        new_version_limits(pool)
        assert PEAK == 1, ('upstream oversubscribed after the upgrade', PEAK)
        print('Upgrade under load passed: the running version refused the replacement and kept limiting and queueing; requests arriving while the host waited were queued and served; no request failed; a config reload while idle loaded the new library, which limits and queues', json.dumps({'started_as': started_as, 'requests_sent_while_host_waited': len(extra), 'queued_inside_quiesce_window': len(in_window), 'peak': PEAK}))

    def swap_between_requests(pool, started_as):
        # One client sending short requests back to back, 20 ms apart.
        statuses = collections.Counter()
        failures = []
        stop = threading.Event()

        def traffic():
            n = 0
            while not stop.is_set():
                n += 1
                code, body = chat(f'steady-{n}')
                statuses[code] += 1
                if code != 200:
                    failures.append((n, code, body[:200]))
                time.sleep(.02)

        worker = pool.submit(traffic)
        wait_until(lambda: statuses[200] >= 5, 'steady traffic did not start')
        install_new_version()
        wait_until(lambda: 'pluginhost: plugin hot reloaded' in host_log(), 'the host did not replace the plugin between requests', timeout=15)
        sent_until_swap = sum(statuses.values())
        wait_until(lambda: sum(statuses.values()) >= sent_until_swap + 5, 'traffic stopped after the replacement')
        stop.set()
        worker.result(timeout=30)
        assert not failures and set(statuses) == {200}, ('requests failed while the plugin was replaced', dict(statuses), failures)
        assert 'pluginhost: plugin quiesce failed' not in host_log(), 'the running version refused although it was idle between requests'
        # Both versions append to the same diagnostics file. The old one
        # records every request it let through while the new one loaded.
        events = [json.loads(line) for line in (root / 'diagnostics.jsonl').read_text().splitlines() if line.strip()]
        passed = sum(1 for e in events if e['kind'] == 'handover_passed')
        rejected = sum(1 for e in events if e['kind'] == 'plugin_draining')
        assert passed >= 1, 'no request arrived between the quiesce and the switch; the handover was not exercised'
        assert rejected == 0, rejected
        wait_until(lambda: status()[1] == 200 and idle(), 'new version did not settle', timeout=15)
        after = status()[0]
        assert not after['runtime']['handed_over'] and not after['runtime']['draining'], after['runtime']
        new_version_limits(pool)
        assert PEAK == 1, ('upstream oversubscribed', PEAK)
        print('Upgrade between requests passed: the running version agreed in a gap; the requests arriving until the host switched were let through; no request failed; the new library limits and queues', json.dumps({'started_as': started_as, 'requests': sum(statuses.values()), 'let_through_during_switch': passed, 'peak': PEAK}))

    try:
        wait_until(lambda: b'queue-test' in api('/v1/models', management=False)[1], 'CPA did not publish the fixture model', timeout=20)
        wait_until(lambda: status()[1] == 200, 'plugin did not register', timeout=10)
        started_as = status()[0].get('version')
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            if expect_unregistered:
                reproduce_defect(pool, started_as)
            elif between_requests:
                swap_between_requests(pool, started_as)
            else:
                under_load(pool, started_as)
    except BaseException:
        RELEASE.set()
        print('--- isolated CPA log ---')
        print('\n'.join(line for line in host_log().splitlines() if '"/v1/models"' not in line and '/status"' not in line and 'gin_logger' not in line)[-12000:])
        print('ARRIVALS', ARRIVALS[-30:], 'PEAK', PEAK)
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
