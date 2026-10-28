import argparse
import hashlib
import json
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('cache', type=Path)
parser.add_argument('--base-url', default='https://huggingface.co/datasets/CRUISEResearchGroup/TripWorld/resolve/421bc1dc63068bb398055b1ce987265fe22415db')
args = parser.parse_args()
args.cache.mkdir(parents=True, exist_ok=True)
paths = ['MANIFEST.json', 'README.md', '_scripts/01_consolidate_filter.py',
         '_scripts/02_hometown_discovery.py', '_scripts/04_build_travel_behaviors.py',
         'region_labels.parquet', 'pois.parquet', 'metadata/metadata_all.parquet',
         'metadata/place_attributes.parquet', 'travel_behaviors.parquet']
with (args.cache / 'requests.log').open('a') as log:
    for name in paths:
        path = args.cache / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            url = f'{args.base_url}/{name}'
            print(url, file=log, flush=True)
            subprocess.run(['curl', '-sSL', '--fail', '--max-time', '900', '--continue-at', '-', url,
                            '-o', str(path) + '.part'], check=True, stderr=log)
            Path(str(path) + '.part').rename(path)
        if name != 'MANIFEST.json':
            manifest = json.loads((args.cache / 'MANIFEST.json').read_text())
            entry = next(x for x in manifest['files'] if x['path'] == name)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != entry['sha256'] or path.stat().st_size != entry['size_bytes']:
                raise ValueError(f'Checksum or size mismatch: {name}')
        print(f'OK {name}: {path.stat().st_size} bytes', flush=True)
