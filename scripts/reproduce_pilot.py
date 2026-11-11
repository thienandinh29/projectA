"""Regenerate the pilot score table from the saved inputs and check it against the saved copy.

One command: reads an inputs file (paths, seed, resamples, expected SHA-256 of each output), builds
the report twice in memory with `scripts.score`, and requires both builds to equal each other, the
hashes recorded in the inputs file and the copies saved under docs/. Writes nothing, opens the
prediction ledger read-only, and exits 1 on any difference. A pilot check, not a result about the model.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

from scripts import score


def _sha256(data):
    return hashlib.sha256(data).hexdigest()


def reproduce(inputs_file, build=score.build_report):
    """Return {'reproduced': bool, 'files': {...}}. Input problems raise exactly as `score` does."""
    inputs_file = Path(inputs_file)
    spec = json.loads(inputs_file.read_text(encoding='utf-8'))
    paths = spec['inputs']
    runs = []
    for _ in range(2):
        report = score._rounded(build(Path(paths['snapshot_dir']), Path(paths['eligibility_selection']),
                                      Path(paths['sentiment_selection']), Path(paths['story_selection']),
                                      Path(paths['predictions']), Path(paths['model_manifest']),
                                      seed=spec['seed'], resamples=spec['resamples']))
        runs.append({name: text.encode('utf-8') for name, text in score.report_files(report).items()})
    files = {}
    for name, expected in spec['expected'].items():
        saved = (inputs_file.parent / expected['saved_copy']).read_bytes()
        sha = _sha256(runs[0][name])
        files[name] = {'sha256': sha, 'second_build_identical': runs[0][name] == runs[1][name],
                       'matches_recorded_hash': sha == expected['sha256'],
                       'matches_saved_copy': runs[0][name] == saved}
    unexpected = sorted(set(runs[0]) - set(spec['expected']))
    ok = not unexpected and all(all(v for k, v in f.items() if k != 'sha256') for f in files.values())
    return {'reproduced': ok, 'files': files, 'files_without_expected_hash': unexpected}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs', type=Path, default=Path('docs/pilot-inputs-20261111.json'))
    result = reproduce(parser.parse_args().inputs)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result['reproduced'] else 1


if __name__ == '__main__':
    sys.exit(main())
