"""Loopback-only development fixture. Never use as a production management API."""
import copy
import json
import pathlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CONFIG = {'version': 1, 'defaults': {'max_concurrency': 2, 'max_queue': 20, 'wait_timeout_ms': 60000, 'paused': False}, 'accounts': {}}
REVISION = 1


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def answer(self, code, body, content_type='application/json'):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == '/':
            self.answer(200, (pathlib.Path(__file__).resolve().parents[1] / 'cmd/plugin/ui.html').read_bytes(), 'text/html; charset=utf-8')
            return
        if self.headers.get('X-Management-Key') != 'fixture-key':
            self.answer(401, {'error': 'unauthorized'})
            return
        if self.path.endswith('/config'):
            self.answer(200, {'config': CONFIG, 'revision': str(REVISION)})
        elif self.path.endswith('/status'):
            accounts = []
            for id, active, queued in [('fixture-a', 1, 0), ('fixture-b', 2, 3)]:
                accounts.append({'id': id, 'policy': CONFIG['accounts'].get(id, CONFIG['defaults']), 'override': id in CONFIG['accounts'], 'in_flight': active, 'queued': queued, 'oldest_wait_ms': queued * 1200})
            self.answer(200, {'runtime': {'draining': False, 'waiting': 3, 'global_queue_limit': 1024, 'accounts': accounts}, 'account_index_available': True, 'account_index': [{'id': 'fixture-a', 'name': 'Claude 账号 A'}, {'id': 'fixture-b', 'name': 'Claude 账号 B'}]})
        else:
            self.answer(404, {})

    def do_PUT(self):
        global CONFIG, REVISION
        if self.headers.get('X-Management-Key') != 'fixture-key':
            self.answer(401, {'error': 'unauthorized'})
            return
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if body['revision'] != str(REVISION):
            self.answer(409, {'error': 'configuration changed; reload before saving'})
            return
        CONFIG = copy.deepcopy(body['config'])
        REVISION += 1
        self.answer(200, {'config': CONFIG, 'revision': str(REVISION)})


if __name__ == '__main__':
    ThreadingHTTPServer(('127.0.0.1', int(sys.argv[1]) if len(sys.argv) > 1 else 18765), Handler).serve_forever()
