"""A frozen model is exact, checked bytes at a full SHA, with labels read from its own config."""
import hashlib
import json
import struct
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts import freeze_model as freeze

REVISION = 'a' * 40
CONFIG = {'architectures': ['BertForSequenceClassification'], 'hidden_size': 4,
          'id2label': {'0': 'neutral', '1': 'positive', '2': 'negative'},
          'label2id': {'neutral': 0, 'positive': 1, 'negative': 2}}


def safetensors(shape=(3, 4)):
    header = json.dumps({'__metadata__': {'format': 'pt'}, 'classifier.weight': {'shape': list(shape)}}).encode()
    return struct.pack('<Q', len(header)) + header + b'weights'


def blob_id(data):
    return hashlib.sha1(f'blob {len(data)}\0'.encode() + data).hexdigest()


class Response:
    def __init__(self, body=b'', payload=None):
        self.body, self.payload = body, payload

    def json(self):
        return self.payload

    def iter_content(self, _size):
        yield self.body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class Hub:
    """Serves one revision. `served` overrides bytes after the hub's hashes were computed."""

    def __init__(self, config=CONFIG, weights=None, sha=REVISION):
        self.files = {'README.md': b'card', 'config.json': json.dumps(config).encode(),
                      'model.safetensors': weights or safetensors(), 'special_tokens_map.json': b'{}',
                      'tokenizer_config.json': json.dumps({'do_lower_case': True, 'model_max_length': 512}).encode(),
                      'vocab.txt': b'[PAD]\n'}
        self.sha, self.served, self.requested = sha, {}, []

    def siblings(self):
        result = []
        for name, data in self.files.items():
            entry = {'rfilename': name, 'size': len(data), 'blobId': blob_id(data)}
            if name == 'model.safetensors':
                entry['lfs'] = {'sha256': hashlib.sha256(data).hexdigest()}
            result.append(entry)
        return result

    def __call__(self, url, stream=False):
        self.requested.append(url)
        if '/api/models/' in url:
            return Response(payload={'sha': self.sha, 'lastModified': '2026-10-12', 'cardData': {'license': 'x'},
                                     'siblings': self.siblings()})
        name = url.rsplit('/', 1)[1]
        return Response(self.served.get(name, self.files[name]))


class FreezeCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.target = self.root / f'org__model@{REVISION}'
        self.manifest_path = self.target.with_name(self.target.name + '.manifest.json')

    def tearDown(self):
        self.tmp.cleanup()

    def run_freeze(self, hub, revision=REVISION):
        return freeze.freeze('org/model', revision, self.root, get=hub, tool_revision='tool',
                             now=datetime(2026, 11, 4, tzinfo=timezone.utc))

    def assert_nothing_published(self):
        self.assertEqual(list(self.root.iterdir()), [])


class TestFreeze(FreezeCase):
    def test_writes_checked_files_and_a_manifest_that_verifies(self):
        self.run_freeze(Hub())
        manifest = json.loads(self.manifest_path.read_text(encoding='utf-8'))
        self.assertEqual(manifest['revision'], REVISION)
        self.assertEqual(manifest['files']['vocab.txt']['sha256'], hashlib.sha256(b'[PAD]\n').hexdigest())
        self.assertEqual(manifest['frozen_at_utc'], '2026-11-04T00:00:00+00:00')
        self.assertEqual(freeze.verify_frozen(self.target, manifest), [])

    def test_label_map_comes_from_config_not_from_a_default_order(self):
        self.run_freeze(Hub())
        manifest = json.loads(self.manifest_path.read_text(encoding='utf-8'))
        self.assertEqual(manifest['label_map'], {'0': 'neutral', '1': 'positive', '2': 'negative'})

    def test_never_requests_a_pickle_weight_file(self):
        hub = Hub()
        self.run_freeze(hub)
        self.assertFalse([url for url in hub.requested if url.endswith(('.bin', '.h5', '.msgpack'))])

    def test_branch_names_and_short_shas_are_refused(self):
        for revision in ('main', 'a' * 10, 'A' * 40):
            with self.assertRaisesRegex(ValueError, 'full 40-character'):
                self.run_freeze(Hub(), revision)
        self.assert_nothing_published()

    def test_hub_resolving_to_another_revision_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'resolved'):
            self.run_freeze(Hub(sha='b' * 40))
        self.assert_nothing_published()

    def test_tampered_weights_and_config_publish_nothing(self):
        for name, data in (('model.safetensors', safetensors()[:-1] + b'X'), ('config.json', b'{"x": 1}')):
            hub = Hub()
            hub.served[name] = data
            with self.assertRaises(ValueError):
                self.run_freeze(hub)
            self.assert_nothing_published()

    def test_truncated_download_publishes_nothing(self):
        hub = Hub()
        hub.served['model.safetensors'] = safetensors()[:-3]
        with self.assertRaisesRegex(ValueError, 'size'):
            self.run_freeze(hub)
        self.assert_nothing_published()

    def test_a_frozen_model_is_never_replaced(self):
        self.run_freeze(Hub())
        before = self.manifest_path.read_bytes()
        with self.assertRaises(FileExistsError):
            self.run_freeze(Hub())
        self.assertEqual(self.manifest_path.read_bytes(), before)

    def test_interrupted_partial_is_not_reused(self):
        self.target.with_name(self.target.name + '.partial').mkdir()
        with self.assertRaises(FileExistsError):
            self.run_freeze(Hub())

    def test_weights_with_the_wrong_output_size_are_refused(self):
        with self.assertRaisesRegex(ValueError, 'classifier.weight'):
            self.run_freeze(Hub(weights=safetensors((2, 4))))
        self.assert_nothing_published()


class TestLabels(unittest.TestCase):
    def test_bad_label_maps_are_refused(self):
        for config in ({}, {'id2label': {'0': 'LABEL_0', '1': 'LABEL_1', '2': 'LABEL_2'}},
                       {'id2label': {'0': 'positive', '1': 'negative'}},
                       {'id2label': {'0': 'positive', '1': 'negative', '3': 'neutral'}},
                       {'id2label': CONFIG['id2label'], 'label2id': {'neutral': 1, 'positive': 0, 'negative': 2}}):
            with self.assertRaises(ValueError):
                freeze.read_labels(config)


class TestVerify(FreezeCase):
    def frozen(self):
        self.run_freeze(Hub())
        return json.loads(self.manifest_path.read_text(encoding='utf-8'))

    def test_changed_missing_and_extra_files_are_reported(self):
        manifest = self.frozen()
        (self.target / 'vocab.txt').write_bytes(b'other')
        (self.target / 'README.md').unlink()
        (self.target / 'pytorch_model.bin').write_bytes(b'pickle')
        self.assertEqual(freeze.verify_frozen(self.target, manifest),
                         ['extra file: pytorch_model.bin', 'missing file: README.md', 'changed file: vocab.txt'])


if __name__ == '__main__':
    unittest.main()
