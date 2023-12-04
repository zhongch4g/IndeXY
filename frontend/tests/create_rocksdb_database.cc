#include <gflags/gflags.h>
#include <tbb/tbb.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "XYStore.hpp"
#include "leanstore/LeanStore.hpp"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util.h"
#include "trace.h"

DEFINE_bool (is_write, true, "is write");

using namespace rocksdb;
using KeyTrace = RandomKeyTrace<util::TraceUniform>;
KeyTrace* key_trace_;

rocksdb::WriteBatch writeBatch;
rocksdb::DB* db;
rocksdb::Options options;
rocksdb::DBOptions dboptions;
rocksdb::WriteOptions woptions;
rocksdb::ReadOptions roptions;
rocksdb::BlockBasedTableOptions tableOptions;
uint64_t columnFamilyNums_;
std::vector<void*> handles;
std::string kDBPath = "./rocksdb_320M_rnd_db";
uint64_t kNum = 320000000;

static void TransformKey (uint64_t rid, IndexKey& index_key) {
    index_key.setKeyLen (sizeof (rid));
    reinterpret_cast<uint64_t*> (&index_key[0])[0] = __builtin_bswap64 (rid);
}

int main () {
    key_trace_ = new KeyTrace (kNum);
    std::cout << "Read key from file" << std::endl;
    key_trace_->FromFile ("database_320M_rnd_db.trc");
    if (FLAGS_is_write) {
        std::cout << "Write all keys" << std::endl;
        woptions.disableWAL = true;
        woptions.sync = false;

        // Enable the bloomfilter
        tableOptions.whole_key_filtering = true;
        tableOptions.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
        // tableOptions.block_cache = rocksdb::NewLRUCache (1 * 1024 * 1024 * 1024LL);
        // tableOptions.cache_index_and_filter_blocks = true;
        // tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism (4);
        options.OptimizeLevelStyleCompaction ();
        // ====================== FLATTEN the throughput ========================
        // Set up the rate limiter to allow a maximum of 1 MB/s write rate
        // The first parameter is the write rate limit in bytes per second
        // The second parameter is the refill period in microseconds
        // The third parameter is the fair queueing factor (optional)
        options.rate_limiter.reset (rocksdb::NewGenericRateLimiter (150000000));

        options.use_direct_io_for_flush_and_compaction = true;
        options.use_direct_reads = true;
        options.create_if_missing = true;
        options.max_open_files = -1;
        options.compression = rocksdb::kSnappyCompression;
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
        options.write_buffer_size = 256 * 1024 * 1024;
        // options.arena_block_size = 64 * 1024;
        // options.disable_auto_compactions = true;
        options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (tableOptions));

        // create the database

        rocksdb::Status s = rocksdb::DB::Open (options, kDBPath, &db);

        tbb::task_scheduler_init taskScheduler (1);
        std::chrono::high_resolution_clock::time_point begin, end;

        begin = std::chrono::high_resolution_clock::now ();
        {
            // at least 4 threads
            tbb::parallel_for (
                tbb::blocked_range<uint64_t> (0, key_trace_->count_),
                [&] (const tbb::blocked_range<uint64_t>& range) {
                    for (uint64_t t_i = range.begin (); t_i < range.end (); t_i++) {
                        auto& key = key_trace_->keys_[t_i];
                        Key k;
                        TransformKey (key, k);

                        std::string rValue;
                        rValue.resize (8);
                        std::memcpy (rValue.data (), &key, 8);
                        db->Put (woptions, rocksdb::Slice ((char*)k.getData (), k.getKeyLen ()),
                                 rValue);
                    }
                });
        }
        end = std::chrono::high_resolution_clock::now ();
        std::cout << "write time elapsed = "
                  << (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count () /
                      1000000.0)
                  << std::endl;
        delete db;

        // key_trace_->ToFile ("rocksdb_320M_rnd_db.trc");
    } else {
        std::cout << "Read all keys" << std::endl;
        key_trace_->FromFile ("rocksdb_320M_rnd_db.trc");

        woptions.disableWAL = true;
        woptions.sync = false;

        // Enable the bloomfilter
        tableOptions.whole_key_filtering = true;
        tableOptions.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
        // tableOptions.block_cache = rocksdb::NewLRUCache (1 * 1024 * 1024 * 1024LL);
        // tableOptions.cache_index_and_filter_blocks = true;
        // tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism (4);
        options.OptimizeLevelStyleCompaction ();
        // ====================== FLATTEN the throughput ========================
        // Set up the rate limiter to allow a maximum of 1 MB/s write rate
        // The first parameter is the write rate limit in bytes per second
        // The second parameter is the refill period in microseconds
        // The third parameter is the fair queueing factor (optional)
        options.rate_limiter.reset (rocksdb::NewGenericRateLimiter (150000000));

        options.use_direct_io_for_flush_and_compaction = true;
        options.use_direct_reads = true;
        options.create_if_missing = true;
        options.max_open_files = -1;
        options.compression = rocksdb::kSnappyCompression;
        options.compaction_style = rocksdb::kCompactionStyleUniversal;
        options.write_buffer_size = 256 * 1024 * 1024;
        // options.arena_block_size = 64 * 1024;
        // options.disable_auto_compactions = true;
        options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (tableOptions));

        // create the database

        rocksdb::Status s = rocksdb::DB::Open (options, kDBPath, &db);

        tbb::task_scheduler_init taskScheduler (1);
        std::chrono::high_resolution_clock::time_point begin, end;

        begin = std::chrono::high_resolution_clock::now ();
        {
            // at least 4 threads
            tbb::parallel_for (
                tbb::blocked_range<uint64_t> (0, key_trace_->count_),
                [&] (const tbb::blocked_range<uint64_t>& range) {
                    for (uint64_t t_i = range.begin (); t_i < range.end (); t_i++) {
                        auto& key = key_trace_->keys_[t_i];
                        Key k;
                        TransformKey (key, k);

                        std::string rValue;
                        auto isok =
                            db->Get (roptions, rocksdb::Slice ((char*)k.getData (), k.getKeyLen ()),
                                     &rValue);
                        if (!isok.ok ()) {
                            perror ("can not find key\n");
                            exit (0);
                        }
                    }
                });
        }
        end = std::chrono::high_resolution_clock::now ();
        std::cout << "read time elapsed = "
                  << (std::chrono::duration_cast<std::chrono::microseconds> (end - begin).count () /
                      1000000.0)
                  << std::endl;
        delete db;
    }

    return 0;
}