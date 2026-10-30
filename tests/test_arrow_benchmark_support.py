"""ABR-02 deterministic/ordered validation independent of batch boundaries."""
import importlib.util
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("arrow_benchmark_support", ROOT / "benchmarks/arrow_benchmark_support.py")
support = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(support)


def digest_pairs(pairs):
    digest = support.OrderedDigest()
    for key, value in pairs:
        digest.update_pair(key, value)
    return digest.result()


@pytest.mark.parametrize("large", [False, True])
@pytest.mark.parametrize("chunk", [1, 2, 10])
def test_ordered_buffer_digest_matches_records_and_slices(large, chunk):
    records = [(b"", b""), (b"\x00\xff", b"value"), (b"a", b""), (b"b", b"\x00")]
    arrow_type = pa.large_binary() if large else pa.binary()
    padded = [(b"padding", b"ignored")] + records + [(b"trailing", b"ignored")]
    arrays = [pa.array([row[i] for row in padded], type=arrow_type).slice(1, len(records)) for i in (0, 1)]
    table = pa.Table.from_arrays(arrays, names=["key", "value"])
    digest = support.OrderedDigest()
    for batch in table.to_batches(max_chunksize=chunk):
        digest.update_batch(batch)
    assert digest.result() == digest_pairs(records)
    assert digest.result() != digest_pairs(list(reversed(records)))


def test_digest_disambiguates_record_boundaries():
    assert digest_pairs([(b"a", b"bc"), (b"d", b"e")]) != digest_pairs([(b"ad", b"b"), (b"", b"ce")])
    # Identical concatenated column payloads, different key/value boundaries.
    assert digest_pairs([(b"a", b"v"), (b"bc", b"xy")]) != digest_pairs([(b"ab", b"vx"), (b"c", b"y")])


def test_null_columns_rejected():
    batch = pa.record_batch([pa.array([None], type=pa.binary()), pa.array([b"v"])], names=["key", "value"])
    with pytest.raises(ValueError, match="non-null"):
        support.OrderedDigest().update_batch(batch)


@pytest.mark.parametrize("variable", [False, True])
def test_dataset_sorted_seeded_and_exact_size(variable):
    first = list(support.pairs(101, 64, variable, 42))
    assert first == list(support.pairs(101, 64, variable, 42))
    assert first != list(support.pairs(101, 64, variable, 43))
    assert len(first) == len(dict(first)) == 101
    assert [k for k, _ in first] == sorted(k for k, _ in first)
    assert all(1 <= len(v) <= 64 for _, v in first)
    if not variable:
        assert all(len(v) == 64 for _, v in first)


def test_reference_scopes_and_existing_path_guard(tmp_path):
    scopes = support.scopes(100, ["full", "prefix", "range"], "sensor/")
    config = {"path": str(tmp_path / "dataset"), "rows": 100, "value_size": 8,
              "variable": True, "seed": 11, "scopes": scopes}
    result = support.prepare_dataset(config)
    assert result["reference"]["full"]["rows"] == 100
    assert result["reference"]["prefix"]["rows"] == 90
    assert result["reference"]["range"]["rows"] == 45
    with pytest.raises(ValueError, match="must not already exist"):
        support.prepare_dataset(config)
