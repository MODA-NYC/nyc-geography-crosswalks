import argparse
import json
import logging
import os
import subprocess
import sys
import zipfile
from pathlib import Path


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run(cmd: list[str]) -> None:
    logging.info("$ %s", " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(proc.stdout)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def zip_dir(zip_path: Path, folder: Path, include_meta: Path = None) -> None:
    folder = folder.resolve()
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for p in folder.rglob('*'):
            if p.is_file():
                zf.write(p, p.relative_to(folder.parent))
        if include_meta and include_meta.is_file():
            # Add run_meta.json again at root of the zip for convenience
            zf.write(include_meta, include_meta.name)
    logging.info("Wrote %s", zip_path)


def find_latest_run(outputs_root: Path):
    runs = [p for p in outputs_root.glob('*') if p.is_dir()]
    if not runs:
        return None
    return sorted(runs)[-1]


def main() -> None:
    ap = argparse.ArgumentParser(description="Orchestrate a full run: bounds -> crosswalks -> optional zips")
    ap.add_argument('--auto-detect-latest', action='store_true', help='Use auto-detect in bounds (already default)')
    ap.add_argument('--preferred-cycle', help='Pin a cycle letter (e.g., c) for DCP datasets', default=None)
    ap.add_argument('--buffer-feet', type=float, default=-50.0)
    ap.add_argument('--min-area-final', type=float, default=100.0)
    ap.add_argument('--epsilon', type=float, default=1e-6)
    ap.add_argument('--exclude-ids', nargs='*', default=['cc_upcoming'])
    ap.add_argument('--zip-artifacts', action='store_true', help='Create raw and crosswalks zip archives in the run folder')
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    outputs_root = repo_root / 'outputs'

    # Step 1: Bounds
    # Note: generate_all_bounds.py uses internal config; preferred-cycle is informational until exposed as arg.
    bounds_script = repo_root / 'scripts' / 'generate_all_bounds.py'
    run(['python', str(bounds_script)])

    # Find latest run directory made by the script
    latest = find_latest_run(outputs_root)
    if not latest:
        raise SystemExit('No outputs run directory found')
    logging.info('Latest run dir: %s', latest)

    # Step 2: Crosswalks
    crosswalks_script = repo_root / 'scripts' / 'build_crosswalks.py'
    run([
        'python', str(crosswalks_script),
        '--boundaries', str(latest / 'all_boundaries.geojson'),
        '--run-dir', str(latest),
        '--buffer-feet', str(args.buffer_feet),
        '--min-area-final', str(args.min_area_final),
        '--epsilon', str(args.epsilon),
        '--exclude-ids', *args.exclude_ids,
    ])

    if args.zip_artifacts:
        # Create crosswalks zip (longform + wide + crosswalks_meta.json)
        crosswalks_zip = latest / f'crosswalks__{latest.name}.zip'
        zip_dir(crosswalks_zip, latest / 'longform')
        with zipfile.ZipFile(crosswalks_zip, 'a', compression=zipfile.ZIP_DEFLATED) as zf:
            # also include wide
            wide_dir = latest / 'wide'
            for p in wide_dir.rglob('*'):
                if p.is_file():
                    zf.write(p, p.relative_to(latest.parent))
            xmeta = latest / 'crosswalks_meta.json'
            if xmeta.is_file():
                zf.write(xmeta, xmeta.name)
        logging.info('Wrote %s', crosswalks_zip)

        # Create raw geographies zip (data/processed/*.geojson + all_boundaries + run_meta)
        raw_zip = latest / f'raw_geographies__{latest.name}.zip'
        with zipfile.ZipFile(raw_zip, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            processed = repo_root / 'data' / 'processed'
            for p in processed.glob('*.geojson'):
                zf.write(p, p.relative_to(repo_root))
            zf.write(latest / 'all_boundaries.geojson', (latest / 'all_boundaries.geojson').relative_to(repo_root))
            zf.write(latest / 'run_meta.json', (latest / 'run_meta.json').relative_to(repo_root))
        logging.info('Wrote %s', raw_zip)

    logging.info('Done.')


if __name__ == '__main__':
    main()


