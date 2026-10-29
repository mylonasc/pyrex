"""Sequence provenance through the real RocksDB transaction bindings (#34)."""

from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

import pyrex


@pytest.fixture
def db(tmp_path):
    with pyrex.TransactionDB(str(tmp_path / "db")) as db:
        yield db


def iterator_pairs(txn, read_options=None):
    it = txn.new_iterator(read_options)
    it.seek_to_first()
    pairs = []
    while it.valid():
        pairs.append((it.key(), it.value()))
        it.next()
    it.check_status()
    return pairs


def test_empty_database_and_explicit_snapshot(db):
    assert db.latest_sequence_number == 0
    with db.transaction() as txn:
        assert txn.snapshot_sequence_number is None
        txn.set_snapshot()
        assert txn.snapshot_sequence_number == 0
        assert txn.get(b"missing") is None
        assert iterator_pairs(txn) == []
        # Neither accessor creates writes or replaces the snapshot.
        for _ in range(10):
            assert txn.snapshot_sequence_number == 0
            assert db.latest_sequence_number == 0


@pytest.mark.parametrize("at_begin", [False, True])
@pytest.mark.parametrize("custom_read_options", [False, True])
def test_snapshot_sequence_and_reads_survive_concurrent_commit(db, at_begin, custom_read_options):
    cf = db.create_column_family("named")
    db.put(b"a", b"old")
    db.put_cf(cf, b"a", b"cf-old")
    options = pyrex.TransactionOptions()
    options.set_snapshot = at_begin
    read_options = pyrex.ReadOptions() if custom_read_options else None
    pinned = threading.Event()

    def writer():
        assert pinned.wait(timeout=10), "reader did not pin its snapshot"
        with db.transaction() as txn:
            txn.put(b"a", b"new")
            txn.put(b"b", b"added")
            txn.put_cf(cf, b"a", b"cf-new")
            txn.commit()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        try:
            with db.transaction(transaction_options=options) as txn:
                if not at_begin:
                    assert txn.snapshot_sequence_number is None
                    txn.set_snapshot()
                sequence = txn.snapshot_sequence_number
                assert isinstance(sequence, int)
                assert sequence == db.latest_sequence_number
                pinned.set()
                future.result(timeout=10)

                assert db.latest_sequence_number > sequence
                assert txn.snapshot_sequence_number == sequence
                assert txn.get(b"a", read_options) == b"old"
                assert txn.get(b"b", read_options) is None
                assert txn.get_cf(cf, b"a", read_options) == b"cf-old"
                # Created only AFTER the writer commits, but still pinned.
                assert iterator_pairs(txn, read_options) == [(b"a", b"old")]
                assert txn.snapshot_sequence_number == sequence

                with db.transaction(transaction_options=options) as fresh:
                    if not at_begin:
                        fresh.set_snapshot()
                    assert fresh.snapshot_sequence_number == db.latest_sequence_number
                    assert fresh.snapshot_sequence_number > sequence
                    assert fresh.get(b"a") == b"new"
                    assert fresh.get_cf(cf, b"a") == b"cf-new"
                    assert iterator_pairs(fresh) == [(b"a", b"new"), (b"b", b"added")]
        finally:
            pinned.set()
        future.result(timeout=10)


def test_unpinned_transaction_stays_unpinned(db):
    with db.transaction() as txn:
        assert txn.snapshot_sequence_number is None
        db.put(b"a", b"committed")
        assert txn.get(b"a") == b"committed"
        assert txn.snapshot_sequence_number is None


def test_sequence_changes_only_when_snapshot_is_explicitly_replaced(db):
    with db.transaction() as txn:
        txn.set_snapshot()
        assert txn.snapshot_sequence_number == 0
        db.put(b"a", b"new")
        assert txn.snapshot_sequence_number == 0
        assert txn.get(b"a") is None
        txn.set_snapshot()
        assert txn.snapshot_sequence_number == db.latest_sequence_number
        assert txn.snapshot_sequence_number > 0
        assert txn.get(b"a") == b"new"


def test_latest_sequence_tracks_only_committed_writes(db):
    with db.transaction() as txn:
        txn.set_snapshot()
        initial = db.latest_sequence_number
        txn.put(b"a", b"pending")
        assert db.latest_sequence_number == initial
        assert txn.snapshot_sequence_number == initial
        # Local writes overlay the pinned committed-state horizon.
        assert txn.get(b"a") == b"pending"
        assert iterator_pairs(txn) == [(b"a", b"pending")]
        txn.commit()
    committed = db.latest_sequence_number
    assert committed > initial
    with db.transaction() as txn:
        txn.put(b"b", b"rolled back")
        txn.rollback()
    assert db.latest_sequence_number == committed
    db.put(b"c", b"direct")
    assert db.latest_sequence_number > committed


@pytest.mark.parametrize("completion", ["commit", "rollback", "close"])
@pytest.mark.parametrize("with_snapshot", [False, True])
def test_snapshot_sequence_rejects_completed_transactions(db, completion, with_snapshot):
    txn = db.begin_transaction()
    if with_snapshot:
        txn.set_snapshot()
    if completion == "close":
        db.close()
    else:
        getattr(txn, completion)()
    with pytest.raises(pyrex.RocksDBException, match="no longer active"):
        _ = txn.snapshot_sequence_number


def test_latest_sequence_rejects_closed_database(db):
    db.close()
    with pytest.raises(pyrex.RocksDBException, match="closed"):
        _ = db.latest_sequence_number


def test_sequence_properties_are_read_only(db):
    with db.transaction() as txn:
        with pytest.raises(AttributeError):
            txn.snapshot_sequence_number = 0
    with pytest.raises(AttributeError):
        db.latest_sequence_number = 0


@pytest.mark.parametrize("completion", ["commit", "rollback", "close"])
def test_accessors_racing_completion_are_lifetime_safe(tmp_path, completion):
    # Start competing native operations together, without timing-based sleeps.
    with ThreadPoolExecutor(max_workers=1) as pool:
        for i in range(20):
            with pyrex.TransactionDB(str(tmp_path / str(i))) as db:
                txn = db.begin_transaction()
                txn.set_snapshot()
                start = threading.Barrier(2, timeout=10)

                def read_sequences():
                    start.wait()
                    try:
                        assert txn.snapshot_sequence_number == 0
                    except pyrex.RocksDBException as exc:
                        assert "no longer active" in str(exc)
                    try:
                        assert db.latest_sequence_number == 0
                    except pyrex.RocksDBException as exc:
                        assert completion == "close"
                        assert "closed" in str(exc)

                future = pool.submit(read_sequences)
                start.wait()
                if completion == "close":
                    db.close()
                else:
                    getattr(txn, completion)()
                future.result(timeout=10)
