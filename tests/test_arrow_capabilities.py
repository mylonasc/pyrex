"""ABR-06: discoverable raw-KV contract and captured iterator controls."""
import gc
import itertools

import pytest
import pyrex

pa = pytest.importorskip("pyarrow")


@pytest.mark.parametrize("kind", [pyrex.PyRocksDB, pyrex.PyRocksDBExtended, pyrex.TransactionDB])
def test_capabilities_match_available_apis(kind, tmp_path):
    with kind(str(tmp_path / "db")) as db:
        caps = db.arrow_capabilities()
        assert caps["contract_version"] == 1
        for name in ("scan_arrow", "scan_arrow_cf", "scan_arrow_ranges", "scan_arrow_ranges_cf",
                     "multi_get_arrow", "multi_get_arrow_cf"):
            assert caps[name] is hasattr(db, name)
        assert caps["offset_bits"] == 32 and caps["owned_output"] and caps["single_export"]
        assert caps["snapshot_scope"] == "same_db_committed_state"
        assert caps["range_order"] == "bytewise_union"
        assert caps["point_order"] == "input_with_duplicates"
        assert caps["point_value_nullable"] and not caps["scan_value_nullable"]
        assert caps["batch_size_range"] == (1, 16777216)
        caps["owned_output"] = False
        assert db.arrow_capabilities()["owned_output"]
        db.close()
        assert db.arrow_capabilities()["contract_version"] == 1


@pytest.mark.parametrize("fill,verify,readahead", list(itertools.product([False, True], [False, True], [0, 4096, 65536])))
@pytest.mark.parametrize("source", ["scan", "ranges", "points"])
def test_captured_controls_and_interoperability(tmp_path, fill, verify, readahead, source):
    with pyrex.PyRocksDB(str(tmp_path / "db")) as db:
        db.put(b"k", b"v")
        options = pyrex.ReadOptions()
        options.fill_cache, options.verify_checksums, options.readahead_size = fill, verify, readahead
        if source == "scan":
            scan = db.scan_arrow(read_options=options)
        elif source == "ranges":
            scan = db.scan_arrow_ranges(prefixes=[b"k"], read_options=options)
        else:
            scan = db.multi_get_arrow([b"k"], read_options=options)
        config = scan.configuration
        assert config["fill_cache"] is fill and config["verify_checksums"] is verify
        assert config["readahead_size"] == (0 if source == "points" else readahead)
        assert config["total_order_seek"]
        options.fill_cache, options.verify_checksums, options.readahead_size = not fill, not verify, 1
        del options
        gc.collect()
        assert scan.configuration == config
        config["batch_size"] = 0
        assert scan.configuration["batch_size"] == 65536
        with pa.RecordBatchReader.from_stream(scan) as reader:
            result = reader.read_all()
        assert result.to_pylist() == [dict(key=b"k", value=b"v")]
        assert scan.configuration["contract_version"] == 1


@pytest.mark.parametrize("name", ["tailing", "prefix_same_as_start", "auto_prefix_mode", "snapshot",
                                 "iterate_upper_bound", "iterate_lower_bound", "total_order_seek"])
def test_unsupported_controls_not_silently_accepted(name):
    options = pyrex.ReadOptions()
    with pytest.raises(AttributeError):
        setattr(options, name, True)


def test_readahead_validation_and_empty_scan(tmp_path):
    options = pyrex.ReadOptions()
    assert options.readahead_size == 0
    for invalid in (-1, 1073741825):
        with pytest.raises((TypeError, ValueError)):
            options.readahead_size = invalid
    options.readahead_size = 1073741824
    with pyrex.PyRocksDB(str(tmp_path / "db")) as db:
        db.default_read_options = options
        scan = db.scan_arrow()
        assert scan.configuration["readahead_size"] == 1073741824
        with pa.RecordBatchReader.from_stream(scan) as reader:
            assert reader.read_all().num_rows == 0
        assert scan.configuration["fill_cache"]


def test_polars_consumer_with_readahead(tmp_path):
    pl = pytest.importorskip("polars")
    with pyrex.PyRocksDB(str(tmp_path / "db")) as db:
        db.put(b"k", b"v")
        opts = pyrex.ReadOptions()
        opts.readahead_size = 4096
        with pa.RecordBatchReader.from_stream(db.scan_arrow(read_options=opts)) as reader:
            assert pl.from_arrow(reader).rows() == [(b"k", b"v")]
