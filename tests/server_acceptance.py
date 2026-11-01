"""Run on the server against ONLY cpa-account-queue-test; always stop fixtures."""
import concurrent.futures
import hashlib
import json
import pathlib
import socket
import subprocess
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path('/opt/1panel/docker/compose/cpa-account-queue-test')
HOST = 'cpa-account-queue-test-host'
MOCK = 'cpa-account-queue-test-mock'
BASE = 'http://127.0.0.1:18317'
HOST_IP = '127.0.0.1'
HOST_PORT = 18317
MOCK_BASE = 'http://127.0.0.1:18319'
KEYS = json.loads((ROOT / 'test-keys.json').read_text())
BASELINE = json.loads((ROOT / 'baseline.json').read_text())
RESULT = {'tests': [], 'passed': False}
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def docker(*args):
    return subprocess.check_output(['docker', *args], stderr=subprocess.STDOUT).decode()


def record(name, **data):
    RESULT['tests'].append({'name': name, **data})
    print(json.dumps({'test': name, **data}), flush=True)


def api(path, method='GET', body=None, management=True):
    headers = {'Authorization': 'Bearer ' + KEYS['management' if management else 'client'], 'Content-Type': 'application/json'}
    req = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(), method=method, headers=headers)
    try:
        with opener.open(req, timeout=12) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def data(path):
    code, raw = api(path)
    assert code == 200, (code, raw[:300])
    return json.loads(raw)


def status():
    return data('/v0/management/plugins/cpa-account-queue/status')


def control(action=''):
    with opener.open(MOCK_BASE + '/control?action=' + action, timeout=3) as r:
        return json.load(r)


def wait(fn, msg, timeout=8):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            if fn():
                return
        except (OSError, ValueError):
            pass
        time.sleep(.025)
    raise AssertionError(msg)


def chat(text, model='queue-a', stream=False):
    return api('/v1/chat/completions', 'POST', {'model': model, 'messages': [{'role': 'user', 'content': text}], 'stream': stream}, False)


def idle():
    return all(a['in_flight'] == 0 and a['queued'] == 0 for a in status()['runtime']['accounts'])


def save(config):
    current = data('/v0/management/plugins/cpa-account-queue/policies')
    code, raw = api('/v0/management/plugins/cpa-account-queue/policies', 'PUT', {'revision': current['revision'], 'config': config})
    assert code == 200, (code, raw[:300])
    return json.loads(raw)


def policy(limit, queue=3, timeout=5000, paused=False):
    return {'max_concurrency': limit, 'max_queue': queue, 'wait_timeout_ms': timeout, 'paused': paused}


def assert_fixture_boundary():
    inspected = json.loads(docker('inspect', HOST, MOCK))
    for d in inspected:
        assert d['Config']['Labels']['com.docker.compose.project'] == 'cpa-account-queue-test'
        assert d['Image'] == BASELINE['image']
        assert d['HostConfig']['RestartPolicy']['Name'] == 'no'
        for mount in d['Mounts']:
            assert mount['Source'].startswith(str(ROOT) + '/'), mount
        for bindings in d['HostConfig']['PortBindings'].values():
            assert all(b['HostIp'] == '127.0.0.1' for b in bindings)
        networks = d['NetworkSettings']['Networks']
        assert len(networks) == 1 and 'app' not in networks
        network = json.loads(docker('network', 'inspect', next(iter(networks))))[0]
        assert network['Internal'] is True
    record('isolation', fixed_image=True, internal_network=True, loopback_only=True, private_mounts=True)


try:
    assert_fixture_boundary()
    # Docker's internal networks may not install published-port NAT rules.
    # Access the isolated bridge from its host instead of changing firewall or
    # attaching these fixtures to a public/existing network.
    host_net = json.loads(docker('inspect', HOST))[0]['NetworkSettings']['Networks']
    mock_net = json.loads(docker('inspect', MOCK))[0]['NetworkSettings']['Networks']
    HOST_IP = next(iter(host_net.values()))['IPAddress']
    HOST_PORT = 8317
    BASE = f'http://{HOST_IP}:{HOST_PORT}'
    MOCK_BASE = 'http://' + next(iter(mock_net.values()))['IPAddress'] + ':8319'
    wait(lambda: api('/v1/models', management=False)[0] == 200, 'test host not ready', 15)
    initial = status()
    assert initial['account_index_available']
    assert any(a['name'] == 'fixture-disabled.json' for a in initial['account_index'])
    assert api('/v0/resource/plugins/cpa-account-queue/ui')[0] == 200
    try:
        opener.open(BASE + '/v0/management/plugins/cpa-account-queue/status', timeout=3)
        raise AssertionError('unauthenticated management accepted')
    except urllib.error.HTTPError as e:
        assert e.code in (401, 403)
    record('load_and_management', account_file_discovered=True, unauthenticated_rejected=True, ui_http=200)

    control('reset')
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(chat, 'hold-first', 'queue-a', True)
        wait(lambda: len(control()['arrivals']) == 1, 'first did not enter')
        second = pool.submit(chat, 'second')
        wait(lambda: status()['runtime']['waiting'] == 1, 'second did not queue')
        third = pool.submit(chat, 'third')
        wait(lambda: status()['runtime']['waiting'] == 2, 'third did not queue')
        assert control()['peak']['fixture-key-a'] == 1
        control('release')
        for f in (first, second, third):
            code, raw = f.result(timeout=12)
            assert code == 200 and b'OK' in raw, (code, raw[:300])
    wait(idle, 'FIFO did not drain')
    assert control()['arrivals'] == ['hold-first', 'second', 'third']
    record('fifo_and_stream', peak=control()['peak'], order=control()['arrivals'])

    # Observe selected_auth_id through the plugin's actual execution records.
    seen_a = {a['id'] for a in status()['runtime']['accounts'] if a['id'] != 'fixture-disabled.json'}
    assert len(seen_a) == 1, seen_a
    id_a = next(iter(seen_a))
    control('reset')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        held = pool.submit(chat, 'hold-identify-b', 'queue-b')
        wait(lambda: any(a['in_flight'] for a in status()['runtime']['accounts'] if a['id'] not in seen_a), 'b identity missing')
        id_b = next(a['id'] for a in status()['runtime']['accounts'] if a['in_flight'] and a['id'] != id_a)
        control('release')
        assert held.result(timeout=12)[0] == 200
    wait(idle, 'identify did not drain')
    config = {'version': 1, 'defaults': policy(1), 'accounts': {id_a: policy(1), id_b: policy(2)}}
    save(config)
    assert json.loads((ROOT / 'state/policies.json').read_text())['accounts'][id_b]['max_concurrency'] == 2
    control('reset')
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        running = [pool.submit(chat, 'hold-a', 'queue-a'), pool.submit(chat, 'hold-b1', 'queue-b'), pool.submit(chat, 'hold-b2', 'queue-b')]
        wait(lambda: control()['active'].get('fixture-key-a') == 1 and control()['active'].get('fixture-key-b') == 2, 'per-account limits not reached')
        queued = [pool.submit(chat, 'queued-a', 'queue-a'), pool.submit(chat, 'queued-b', 'queue-b')]
        wait(lambda: status()['runtime']['waiting'] == 2, 'independent queues missing')
        assert control()['peak'] == {'fixture-key-a': 1, 'fixture-key-b': 2}, control()
        control('release')
        for f in running + queued:
            assert f.result(timeout=12)[0] == 200
    wait(idle, 'multi-account did not drain')
    record('per_account_limits', configured={'A': 1, 'B': 2}, observed_peak=control()['peak'], independent_queues=True, persisted=True)

    control('reset')
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        held = pool.submit(chat, 'hold-cancel')
        wait(lambda: len(control()['arrivals']) == 1, 'cancel holder missing')
        payload = json.dumps({'model': 'queue-a', 'messages': [{'role': 'user', 'content': 'canceled-waiter'}]}).encode()
        sock = socket.create_connection((HOST_IP, HOST_PORT), timeout=3)
        try:
            sock.sendall((f'POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {KEYS["client"]}\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode() + payload)
            wait(lambda: status()['runtime']['waiting'] == 1, 'cancel waiter not queued')
            started = time.monotonic()
            sock.shutdown(socket.SHUT_RDWR)
        finally:
            sock.close()
        wait(lambda: status()['runtime']['waiting'] == 0, 'cancel waiter leaked', 2)
        elapsed = round((time.monotonic() - started) * 1000)
        assert 'canceled-waiter' not in control()['arrivals']
        control('release')
        assert held.result(timeout=12)[0] == 200
    wait(idle, 'cancel did not drain')
    record('queued_disconnect', removed_within_ms=elapsed, reached_upstream=False)

    for queue, timeout, label in [(0, 1000, 'full'), (2, 150, 'timeout')]:
        config['accounts'][id_a] = policy(1, queue, timeout)
        save(config)
        control('reset')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            held = pool.submit(chat, 'hold-' + label)
            wait(lambda: len(control()['arrivals']) == 1, 'reject holder missing')
            code, raw = chat('rejected-' + label)
            assert code == 503, (code, raw[:300])
            assert len(control()['arrivals']) == 1
            control('release')
            assert held.result(timeout=12)[0] == 200
        wait(idle, 'rejection did not drain')
        assert chat('after-' + label)[0] == 200
        wait(idle, 'recovery did not drain')
        record('reject_' + label, http_status=code, subsequent_request=200)

    # Pause and resume only one account through the real authenticated API.
    config['accounts'][id_a] = policy(1, 3, 5000, True)
    save(config)
    assert chat('paused-a')[0] == 503
    assert chat('unpaused-b', 'queue-b')[0] == 200
    config['accounts'][id_a]['paused'] = False
    save(config)
    assert chat('resumed-a')[0] == 200
    wait(idle, 'pause test did not drain')
    record('pause_one_account', other_account_works=True, resume_works=True)

    # Empty-process restart is scoped to the fixture container only.
    docker('restart', '--time', '10', HOST)
    wait(lambda: api('/v1/models', management=False)[0] == 200, 'test restart failed', 15)
    saved = data('/v0/management/plugins/cpa-account-queue/policies')['config']
    assert saved == config, 'policies lost after restart'
    assert chat('after-restart')[0] == 200
    wait(idle, 'restart did not drain')
    record('empty_restart', policy_preserved=True, first_request=200)
    RESULT['passed'] = True
except BaseException as e:
    RESULT['error'] = str(e)
    raise
finally:
    try:
        control('release')
    except Exception:
        pass
    (ROOT / 'test-host.log').write_text(docker('logs', '--tail', '500', HOST))
    # Stop only the two exact containers created by this test.
    docker('stop', '--time', '10', HOST, MOCK)
    after = json.loads(docker('inspect', 'cli-proxy-api'))[0]
    unchanged = after['Id'] == BASELINE['container_id'] and after['State']['StartedAt'] == BASELINE['started'] and after['RestartCount'] == BASELINE['restarts']
    hashes = {p: hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() == h for p, h in BASELINE['hashes'].items()}
    RESULT['production_unchanged'] = {'container_identity_start_restarts': unchanged, 'file_hashes': hashes, 'running': after['State']['Status'] == 'running'}
    RESULT['fixture_states'] = {d['Name'].lstrip('/'): d['State']['Status'] for d in json.loads(docker('inspect', HOST, MOCK))}
    (ROOT / 'result.json').write_text(json.dumps(RESULT, indent=2))
    print(json.dumps(RESULT), flush=True)
    assert unchanged and all(hashes.values()), 'Production baseline changed; investigate before any further operation'
