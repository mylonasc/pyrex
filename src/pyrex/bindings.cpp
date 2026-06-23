#include "bindings.hpp"

#include <pybind11/stl.h>

#include <exception>
#include <memory>

#include "column_family.hpp"
#include "db.hpp"
#include "exceptions.hpp"
#include "iterator.hpp"
#include "options.hpp"
#include "write_batch.hpp"

#include "rocksdb/options.h"

void bind_pyrex(py::module_& m) {
    m.doc() = R"doc(
        A robust, high-performance Python wrapper for the RocksDB key-value store.

        This module provides two main classes for interacting with RocksDB:
        1. PyRocksDB: A simple interface for standard key-value operations on a
           database with a single (default) column family.
        2. PyRocksDBExtended: An advanced interface that inherits from PyRocksDB and
           adds full support for creating, managing, and using multiple Column Families.
    )doc";

    static py::exception<RocksDBException> rocksdb_exception(m, "RocksDBException", PyExc_RuntimeError);
    rocksdb_exception.doc() = R"doc(
        Custom exception raised for RocksDB-specific operational errors.

        This exception is raised when a RocksDB operation fails for reasons
        such as I/O errors, corruption, invalid arguments, or when an operation
        is attempted on a closed database.
    )doc";

    py::register_exception_translator([](std::exception_ptr p) {
        try {
            if (p) {
                std::rethrow_exception(p);
            }
        } catch (const RocksDBException &e) {
            PyErr_SetString(rocksdb_exception.ptr(), e.what());
        }
    });

    py::enum_<rocksdb::CompressionType>(m, "CompressionType", R"doc(
        Enum for different compression types supported by RocksDB.
    )doc")
        .value("kNoCompression", rocksdb::kNoCompression, "No compression.")
        .value("kSnappyCompression", rocksdb::kSnappyCompression, "Snappy compression (default).")
        .value("kBZip2Compression", rocksdb::kBZip2Compression, "BZip2 compression.")
        .value("kLZ4Compression", rocksdb::kLZ4Compression, "LZ4 compression.")
        .value("kLZ4HCCompression", rocksdb::kLZ4HCCompression, "LZ4HC (high compression) compression.")
        .value("kXpressCompression", rocksdb::kXpressCompression, "Xpress compression.")
        .value("kZSTD", rocksdb::kZSTD, "Zstandard compression.");

    py::class_<PyReadOptions, std::shared_ptr<PyReadOptions>>(m, "ReadOptions", R"doc(
        Configuration options for read operations (Get, Iterator).
    )doc")
        .def(py::init<>(), "Constructs a new ReadOptions object with default settings.")
        .def_property("fill_cache", &PyReadOptions::get_fill_cache, &PyReadOptions::set_fill_cache, "If True, reads will fill the block cache. Defaults to True.")
        .def_property("verify_checksums", &PyReadOptions::get_verify_checksums, &PyReadOptions::set_verify_checksums, "If True, all data read from underlying storage will be verified against its checksums. Defaults to True.");

    py::class_<PyWriteOptions, std::shared_ptr<PyWriteOptions>>(m, "WriteOptions", R"doc(
        Configuration options for write operations (Put, Delete, Write).
    )doc")
        .def(py::init<>(), "Constructs a new WriteOptions object with default settings.")
        .def_property("sync", &PyWriteOptions::get_sync, &PyWriteOptions::set_sync, "If True, the write will be flushed from the OS buffer cache before the write is considered complete. Defaults to False.")
        .def_property("disable_wal", &PyWriteOptions::get_disable_wal, &PyWriteOptions::set_disable_wal, "If True, writes will not be written to the Write Ahead Log. Defaults to False.");

    py::class_<PyOptions>(m, "PyOptions", R"doc(
        Configuration options for opening and managing a RocksDB database.

        This class wraps `rocksdb::Options` and `rocksdb::ColumnFamilyOptions`
        to provide a convenient way to configure database behavior from Python.
    )doc")
        .def(py::init<>(), "Constructs a new PyOptions object with default settings.")
        .def_property("create_if_missing", &PyOptions::get_create_if_missing, &PyOptions::set_create_if_missing, "If True, the database will be created if it is missing. Defaults to True.")
        .def_property("error_if_exists", &PyOptions::get_error_if_exists, &PyOptions::set_error_if_exists, "If True, an error is raised if the database already exists. Defaults to False.")
        .def_property("max_open_files", &PyOptions::get_max_open_files, &PyOptions::set_max_open_files, "Number of open files that can be used by the DB. Defaults to -1 (unlimited).")
        .def_property("write_buffer_size", &PyOptions::get_write_buffer_size, &PyOptions::set_write_buffer_size, "Amount of data to build up in a memory buffer (MemTable) before flushing. Defaults to 64MB.")
        .def_property("compression", &PyOptions::get_compression, &PyOptions::set_compression, "The compression type to use for sst files. Defaults to Snappy.")
        .def_property("max_background_jobs", &PyOptions::get_max_background_jobs, &PyOptions::set_max_background_jobs, "Maximum number of concurrent background jobs (compactions and flushes).")
        .def("increase_parallelism", &PyOptions::increase_parallelism, py::arg("total_threads"), R"doc(
            Increases RocksDB's parallelism by tuning background threads.

            Args:
                total_threads (int): The total number of background threads to use.
        )doc", py::call_guard<py::gil_scoped_release>())
        .def("optimize_for_small_db", &PyOptions::optimize_for_small_db, R"doc(
            Optimizes RocksDB for small databases by reducing memory and CPU consumption.
        )doc", py::call_guard<py::gil_scoped_release>())
        .def("use_block_based_bloom_filter", &PyOptions::use_block_based_bloom_filter, py::arg("bits_per_key") = 10.0, R"doc(
            Enables a Bloom filter for block-based tables to speed up 'Get' operations.

            Args:
                bits_per_key (float): The number of bits per key for the Bloom filter.
                    Higher values reduce false positives but increase memory usage.
        )doc", py::call_guard<py::gil_scoped_release>())
        .def_property("cf_write_buffer_size", &PyOptions::get_cf_write_buffer_size, &PyOptions::set_cf_write_buffer_size, "Default write_buffer_size for newly created Column Families.")
        .def_property("cf_compression", &PyOptions::get_cf_compression, &PyOptions::set_cf_compression, "Default compression type for newly created Column Families.");

    py::class_<PyColumnFamilyHandle, std::shared_ptr<PyColumnFamilyHandle>>(m, "ColumnFamilyHandle", R"doc(
        Represents a handle to a RocksDB Column Family.

        This object is used to perform operations on a specific data partition
        within a `PyRocksDBExtended` instance.
    )doc")
        .def_property_readonly("name", &PyColumnFamilyHandle::get_name, "The name of this column family.")
        .def("is_valid", &PyColumnFamilyHandle::is_valid, "Checks if the handle is still valid (i.e., has not been dropped).");

    py::class_<PyWriteBatch>(m, "PyWriteBatch", R"doc(
        A batch of write operations (Put, Delete) that can be applied atomically.
    )doc")
        .def(py::init<>(), "Constructs an empty write batch.")
        .def("put", &PyWriteBatch::put, py::arg("key"), py::arg("value"), "Adds a key-value pair to the batch for the default column family.")
        .def("put_cf", &PyWriteBatch::put_cf, py::arg("cf_handle"), py::arg("key"), py::arg("value"), "Adds a key-value pair to the batch for a specific column family.")
        .def("delete", &PyWriteBatch::del, py::arg("key"), "Adds a key deletion to the batch for the default column family.")
        .def("delete_cf", &PyWriteBatch::del_cf, py::arg("cf_handle"), py::arg("key"), "Adds a key deletion to the batch for a specific column family.")
        .def("merge", &PyWriteBatch::merge, py::arg("key"), py::arg("value"), "Adds a merge operation to the batch for the default column family.")
        .def("merge_cf", &PyWriteBatch::merge_cf, py::arg("cf_handle"), py::arg("key"), py::arg("value"), "Adds a merge operation to the batch for a specific column family.")
        .def("clear", &PyWriteBatch::clear, "Clears all operations from the batch.");

    py::class_<PyRocksDBIterator, std::shared_ptr<PyRocksDBIterator>>(m, "PyRocksDBIterator", R"doc(
        An iterator for traversing key-value pairs in a RocksDB database.
    )doc")
        .def("valid", &PyRocksDBIterator::valid, "Returns True if the iterator is currently positioned at a valid entry.", py::call_guard<py::gil_scoped_release>())
        .def("seek_to_first", &PyRocksDBIterator::seek_to_first, "Positions the iterator at the first key.", py::call_guard<py::gil_scoped_release>())
        .def("seek_to_last", &PyRocksDBIterator::seek_to_last, "Positions the iterator at the last key.", py::call_guard<py::gil_scoped_release>())
        .def("seek", &PyRocksDBIterator::seek, py::arg("key"), "Positions the iterator at the first key >= the given key.", py::call_guard<py::gil_scoped_release>())
        .def("next", &PyRocksDBIterator::next, "Moves the iterator to the next entry.", py::call_guard<py::gil_scoped_release>())
        .def("prev", &PyRocksDBIterator::prev, "Moves the iterator to the previous entry.", py::call_guard<py::gil_scoped_release>())
        .def("key", &PyRocksDBIterator::key, "Returns the current key as bytes, or None if invalid.")
        .def("value", &PyRocksDBIterator::value, "Returns the current value as bytes, or None if invalid.")
        .def("check_status", &PyRocksDBIterator::check_status, "Raises RocksDBException if an error occurred during iteration.", py::call_guard<py::gil_scoped_release>());

    py::class_<PyRocksDB, std::shared_ptr<PyRocksDB>>(m, "PyRocksDB", R"doc(
        A Python wrapper for RocksDB providing simple key-value storage.

        This class interacts exclusively with the 'default' column family.
        For multi-column-family support, use `PyRocksDBExtended`.
    )doc")
        .def(py::init<const std::string&, PyOptions*, bool>(),
            py::arg("path"),
            py::arg("options") = nullptr,
            py::arg("read_only") = false,
            R"doc(
            Opens a RocksDB database at the specified path.

            Args:
                path (str): The file system path to the database.
                options (PyOptions, optional): Custom options for configuration.
                read_only (bool, optional): If True, opens the database in read-only mode.
                                            Defaults to False.
        )doc", py::call_guard<py::gil_scoped_release>())
        .def("put", &PyRocksDB::put, py::arg("key"), py::arg("value"), py::arg("write_options") = nullptr, "Inserts a key-value pair.", py::call_guard<py::gil_scoped_release>())
        .def("get", &PyRocksDB::get, py::arg("key"), py::arg("read_options") = nullptr, "Retrieves the value for a key.")
        .def("delete", &PyRocksDB::del, py::arg("key"), py::arg("write_options") = nullptr, "Deletes a key.", py::call_guard<py::gil_scoped_release>())
        .def("write", &PyRocksDB::write, py::arg("write_batch"), py::arg("write_options") = nullptr, "Applies a batch of operations atomically.", py::call_guard<py::gil_scoped_release>())
        .def("write_columnar_batch", &PyRocksDB::write_columnar_batch, py::arg("keys"), py::arg("values"), py::kw_only(), py::arg("write_options") = nullptr, py::arg("on_null") = "error", "Writes columnar key/value arrays using a native RocksDB WriteBatch.")
        .def("new_iterator", &PyRocksDB::new_iterator, py::arg("read_options") = nullptr, "Creates a new iterator.", py::keep_alive<0, 1>())
        .def("get_options", &PyRocksDB::get_options, "Returns the options the database was opened with.")
        .def_property("default_read_options", &PyRocksDB::get_default_read_options, &PyRocksDB::set_default_read_options, "The default ReadOptions used for get and iterator operations.")
        .def_property("default_write_options", &PyRocksDB::get_default_write_options, &PyRocksDB::set_default_write_options, "The default WriteOptions used for put, delete, and write operations.")
        .def("close", &PyRocksDB::close, "Closes the database, releasing resources and the lock.", py::call_guard<py::gil_scoped_release>())
        .def("__enter__", [](PyRocksDB &db) -> PyRocksDB& { return db; })
        .def("__exit__", [](PyRocksDB &db, py::object /* type */, py::object /* value */, py::object /* traceback */) {
            db.close();
        });

    py::class_<PyRocksDBExtended, PyRocksDB, std::shared_ptr<PyRocksDBExtended>>(m, "PyRocksDBExtended", R"doc(
        An advanced Python wrapper for RocksDB with full Column Family support.
    )doc")
        .def(py::init<const std::string&, PyOptions*, bool>(),
            py::arg("path"),
            py::arg("options") = nullptr,
            py::arg("read_only") = false,
            R"doc(
            Opens or creates a RocksDB database with Column Family support.

            Args:
                path (str): The file system path to the database.
                options (PyOptions, optional): Custom options for configuration.
                read_only (bool, optional): If True, opens the database in read-only mode.
                                            Defaults to False.
        )doc", py::call_guard<py::gil_scoped_release>())
        .def("put_cf", &PyRocksDBExtended::put_cf, py::arg("cf_handle"), py::arg("key"), py::arg("value"), py::arg("write_options") = nullptr, "Inserts a key-value pair into a specific column family.", py::call_guard<py::gil_scoped_release>())
        .def("get_cf", &PyRocksDBExtended::get_cf, py::arg("cf_handle"), py::arg("key"), py::arg("read_options") = nullptr, "Retrieves the value for a key from a specific column family.")
        .def("delete_cf", &PyRocksDBExtended::del_cf, py::arg("cf_handle"), py::arg("key"), py::arg("write_options") = nullptr, "Deletes a key from a specific column family.", py::call_guard<py::gil_scoped_release>())
        .def("list_column_families", &PyRocksDBExtended::list_column_families, "Lists the names of all existing column families.")
        .def("create_column_family", &PyRocksDBExtended::create_column_family, py::arg("name"), py::arg("cf_options") = nullptr, "Creates a new column family.", py::call_guard<py::gil_scoped_release>())
        .def("drop_column_family", &PyRocksDBExtended::drop_column_family, py::arg("cf_handle"), "Drops a column family.", py::call_guard<py::gil_scoped_release>())
        .def("new_cf_iterator", &PyRocksDBExtended::new_cf_iterator, py::arg("cf_handle"), py::arg("read_options") = nullptr, "Creates a new iterator for a specific column family.", py::keep_alive<0, 1>())
        .def("get_column_family", &PyRocksDBExtended::get_column_family, py::arg("name"), "Retrieves a ColumnFamilyHandle by its name.")
        .def_property_readonly("default_cf", &PyRocksDBExtended::get_default_cf, "Returns the handle for the default column family.");
}
