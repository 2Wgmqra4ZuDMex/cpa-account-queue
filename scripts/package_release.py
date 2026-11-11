"""Package a tested Linux amd64 library and its CPA schema-2 store registry."""
import hashlib
import json
import pathlib
import re
import sys
import zipfile

root = pathlib.Path(__file__).resolve().parents[1]
version = re.search(r'const pluginVersion = "([^"]+)"', (root / 'cmd/plugin/protocol.go').read_text(encoding='utf-8')).group(1)
if not re.fullmatch(r'[0-9][0-9A-Za-z.+-]*', version):
    raise SystemExit('Invalid release version')
library = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
name = f'cpa-account-queue_{version}_linux_amd64.zip'
archive = out / name
with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as z:
    info = zipfile.ZipInfo('cpa-account-queue.so', (2026, 1, 1, 0, 0, 0))
    info.external_attr = 0o100644 << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    z.writestr(info, library.read_bytes())
sha = hashlib.sha256(archive.read_bytes()).hexdigest()
repo = 'https://github.com/2Wgmqra4ZuDMex/cpa-account-queue'
registry = {'schema_version': 2, 'plugins': [{
    'id': 'cpa-account-queue', 'name': 'CPA Account Queue (Development Preview)',
    'description': 'Local per-account concurrency and bounded FIFO queues. Development preview. Linux amd64 only.',
    'author': '2Wgmqra4ZuDMex', 'version': version, 'repository': repo,
    'homepage': repo, 'license': 'MIT', 'tags': ['concurrency', 'queue', 'local', 'preview'],
    'install': {'type': 'direct', 'artifacts': [{
        'goos': 'linux', 'goarch': 'amd64',
        'url': f'{repo}/releases/download/v{version}/{name}',
        'sha256': sha, 'size': archive.stat().st_size,
    }]},
}]}
(out / 'registry.json').write_text(json.dumps(registry, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
(out / 'checksums.txt').write_text(f'{sha}  {name}\n', encoding='ascii')
with zipfile.ZipFile(archive) as z:
    assert z.namelist() == ['cpa-account-queue.so'] and z.testzip() is None
print(json.dumps({'version': version, 'archive': name, 'sha256': sha, 'size': archive.stat().st_size}))
