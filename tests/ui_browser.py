"""Browser acceptance with fake API, parent theme bridge and session storage."""
import copy
import json
import pathlib
import os
from playwright.sync_api import sync_playwright, expect

html = (pathlib.Path(__file__).resolve().parents[1] / 'cmd/plugin/ui.html').read_text(encoding='utf-8')
policy = {'max_concurrency': 2, 'max_queue': 20, 'wait_timeout_ms': 60000, 'paused': False}
config = {'version': 1, 'defaults': policy, 'accounts': {}}
hits = []
reject = False
value = 0
providers = [('a', 'Antigravity', 'antigravity'), ('b', 'Claude', 'claude'), ('c', 'Kimi', 'kimi'), ('d', 'custom-provider', 'custom-provider'), ('e', '未知平台', '')]


def route(req):
    global config, value
    path = req.request.url.split('test.local', 1)[-1]
    if path == '/shell':
        req.fulfill(content_type='text/html', body='''<html data-theme="dark"><head><style>:root{--bg-secondary:#151412;--bg-primary:#1d1b18;--text-primary:#f6f4f1;--text-secondary:#c9c3bb;--border-color:#3a3530;--primary-color:#8b8680}body{margin:0}iframe{width:100%;height:100vh;border:0}</style></head><body><iframe src="/ui"></iframe></body></html>''')
        return
    if path == '/ui':
        req.fulfill(content_type='text/html', body=html)
        return
    hits.append(path)
    if reject or req.request.headers.get('x-management-key') != 'fixture-key':
        req.fulfill(status=401, json={'error': 'unauthorized'})
    elif path.endswith('/policies'):
        if req.request.method == 'PUT':
            config = req.request.post_data_json['config']
        req.fulfill(json={'revision': 'fixture-revision', 'config': config})
    else:
        value += 1
        req.fulfill(json={'runtime': {'draining': False, 'waiting': value, 'global_queue_limit': 1024, 'accounts': [{'id': aid, 'policy': config['accounts'].get(aid, policy), 'in_flight': 1, 'queued': value, 'oldest_wait_ms': 1000} for aid, _, _ in providers], 'counters': {'enqueued': 0, 'admitted': 0, 'completed': 0, 'cancelled': 0, 'timed_out': 0, 'queue_full': 0, 'global_queue_full': 0, 'paused_rejected': 0, 'draining_rejected': 0, 'orphaned_complete': 0, 'pinned_account_full': 0, 'affinity_fallback': 0}, 'recent_events': []}, 'account_index_available': True, 'account_index': [{'id': aid, 'name': 'Account A' if aid == 'a' else ('same-email@example.invalid' if aid in ('b', 'c') else 'other@example.invalid'), 'provider': provider} for aid, _, provider in providers]})


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, **({'channel': 'msedge'} if os.name == 'nt' else {}))
    context = browser.new_context()
    context.route('https://test.local/**', route)
    page = context.new_page()
    errors = []
    page.on('pageerror', lambda e: errors.append(str(e)))
    page.goto('https://test.local/shell')
    frame = page.frame_locator('iframe')
    f = page.frames[1]
    assert f.evaluate('getComputedStyle(document.body).backgroundColor') == 'rgb(21, 20, 18)'
    page.evaluate("document.documentElement.dataset.theme='white';document.documentElement.style.setProperty('--bg-secondary','#ffffff')")
    expect(frame.locator('html')).to_have_attribute('data-theme', 'white')
    assert f.evaluate('getComputedStyle(document.body).backgroundColor') == 'rgb(255, 255, 255)'
    frame.locator('#key').fill('fixture-key')
    frame.locator('#remember').check()
    frame.locator('#connect').click()
    expect(frame.locator('#status')).to_contain_text('已连接')
    expect(frame.locator('.provider-badge')).to_have_text([providers[i][1] for i in (0, 1, 3, 2, 4)])
    expect(frame.locator('#pinned-full-behavior')).to_have_value('queue')
    frame.locator('#pinned-full-behavior').select_option('fallback')
    frame.locator('#save').click()
    expect(frame.locator('#draft-note')).to_contain_text('已保存')
    assert config['pinned_full_behavior'] == 'fallback'
    frame.locator('#interval').fill('1')
    frame.locator('#interval').press('Tab')
    frame.locator('input[aria-label="为 Account A 使用独立策略"]').check()
    field = frame.locator('input[aria-label="Account A并发上限"]')
    field.fill('7')
    before = value
    page.wait_for_timeout(1300)
    assert value > before, 'automatic refresh did not run'
    expect(field).to_have_value('7')
    expect(frame.locator('#draft-note')).to_contain_text('未保存')
    frame.locator('#save').click()
    expect(frame.locator('#draft-note')).to_contain_text('已保存')
    assert config['accounts']['a']['max_concurrency'] == 7
    page.reload()
    frame = page.frame_locator('iframe')
    expect(frame.locator('#status')).to_contain_text('已连接')
    expect(frame.locator('#remember')).to_be_checked()
    expect(frame.locator('#interval')).to_have_value('1')
    frame.locator('#auto').uncheck()
    before = value
    page.wait_for_timeout(1300)
    assert value == before, 'disabled auto refresh still fired'
    frame.locator('#remember').uncheck()
    page.reload()
    expect(frame.locator('#key')).to_have_value('')
    expect(frame.locator('#status')).to_contain_text('尚未连接')
    frame.locator('#key').fill('fixture-key')
    frame.locator('#remember').check()
    frame.locator('#connect').click()
    expect(frame.locator('#status')).to_contain_text('已连接')
    reject = True
    frame.locator('#refresh').click()
    expect(frame.locator('#status')).to_contain_text('密钥无效')
    assert page.frames[1].evaluate("sessionStorage.getItem('cpa-account-queue.session-key')") is None
    reject = False
    frame.locator('#key').fill('fixture-key')
    frame.locator('#connect').click()
    expect(frame.locator('#status')).to_contain_text('已连接')
    frame.locator('#disconnect').click()
    page.reload()
    expect(frame.locator('#status')).to_contain_text('尚未连接')
    assert not errors, errors
    page.set_viewport_size({'width': 390, 'height': 844})
    assert page.frames[1].evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    browser.close()
print('PASS: parent theme sync, live polling with preserved drafts, interval settings, session restore/opt-out/clear, 401 cleanup, mobile width')
