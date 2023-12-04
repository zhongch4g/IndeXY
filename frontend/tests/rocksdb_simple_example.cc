#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util.h"
#include "trace.h"

using namespace rocksdb;
using KeyTrace = RandomKeyTrace<util::TraceUniform>;
KeyTrace* key_trace_;

rocksdb::WriteBatchWithIndex writeBatchWithIndex;
rocksdb::WriteBatch writeBatch[2];
rocksdb::WriteBatch singleWriteBatch;
rocksdb::DB* db_;
rocksdb::Options options_;
rocksdb::DBOptions dboptions_;
rocksdb::WriteOptions write_options_;
rocksdb::ReadOptions read_options_;
rocksdb::BlockBasedTableOptions table_options;
std::string cf_name;
rocksdb::PerfContext* perf_stat;
rocksdb::IOStatsContext* io_stat;

const int kThreadNum = 1;
const long kNum = 10000000;
const long kbatch = 100000;  /// 1000 20s 100 19.7 1 21.6
const std::string key_suffix = "_A_LONG_SUFFIX";
const std::string value =
    "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234"
    "5678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"
    "9012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012"
    "3456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456"
    "7890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
    "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234"
    "5678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"
    "9012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012"
    "345678901234567890123456789012345678901234567890";
const std::string value2 = "12345678";

void Func (DB* db, long start, long count) {
    Status s;
    std::cout << "Thread " << std::this_thread::get_id () << " handle key range: " << start
              << " to " << start + count << std::endl;
    WriteBatch batch;
    batch.Clear ();
    auto key_iterator = key_trace_->iterate_between (start, count);

    for (long i = 0; i < count / kbatch; i++) {
        // auto res = db->KeyMayExist (read_options_, std::to_string (key), &value);
        // db->Get (read_options_, std::to_string (key), &val);

        for (long j = 0; j < kbatch; j++) {
            auto& key = key_iterator.Next ();
            std::string val;
            batch.Put (std::to_string (key), value2);
        }

        db->Write (write_options_, &batch);
        batch.Clear ();
        // s = db->Put (write_options_, std::to_string (key), value2);
        if (!s.ok ()) {
            std::cout << "Thread " << std::this_thread::get_id () << " put error: " << s.ToString ()
                      << std::endl;
            break;
        }

        // if (i % 10000 == 0) {
        //     // printf ("current %u, %lu, %lu, %lu\n", i, sizeof (key), value.length () * sizeof
        //     // (char),
        //     //         value2.length () * sizeof (char));
        //     printf ("%i\n", i);
        // }
    }
}

std::string kDBPath = "./testdb";

int main () {
    key_trace_ = new KeyTrace (kNum);

    write_options_.disableWAL = true;
    write_options_.sync = false;
    // The block cache stores uncompressed blocks.
    // Optionally user can set a second block cache storing compressed blocks.
    // Reads will fetch data blocks first from uncompressed block cache, then compressed block
    // cache. The compressed block cache can be a replacement of OS page cache, if Direct-IO is
    // used.
    table_options.block_cache = rocksdb::NewLRUCache (1 * 1024 * 1024 * 1024);
    table_options.whole_key_filtering = true;
    // table_options.optimize_filters_for_memory = true;
    table_options.block_size = 16 * 1024;
    table_options.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
    options_.IncreaseParallelism (8);
    options_.OptimizeLevelStyleCompaction ();
    options_.create_if_missing = true;
    options_.write_buffer_size = 256 * 1024 * 1024;
    options_.compression = rocksdb::CompressionType::kNoCompression;
    // options_.memtable_whole_key_filtering = true;
    options_.memtable_prefix_bloom_size_ratio = 0.1;

    options_.max_background_compactions = 4;
    options_.max_background_flushes = 4;

    options_.max_open_files = 500000;

    options_.table_factory.reset (rocksdb::NewBlockBasedTableFactory (table_options));

    // open DB
    Status s = DB::Open (options_, kDBPath, &db_);
    assert (s.ok ());

    std::thread* threads[kThreadNum];
    auto start = std::chrono::steady_clock::now ();
    // initialize writebatch object
    writeBatch[0].Clear ();
    writeBatch[1].Clear ();
    for (int i = 0; i < kThreadNum; i++) {
        threads[i] = new std::thread (Func, db_, i * kNum / kThreadNum, kNum / kThreadNum);
        std::this_thread::sleep_for (std::chrono::microseconds (100));
    }

    for (auto& t : threads) {
        t->join ();
    }
    auto end = std::chrono::steady_clock::now ();

    std::cout << "used " << (end - start).count () << std::endl;

    for (int i = 0; i < kThreadNum; i++) {
        delete threads[i];
    }

    std::string stats;
    db_->GetProperty ("rocksdb.stats", &stats);
    printf ("%s\n", stats.c_str ());

    // =============================================================
    // rocksdb::SetPerfLevel (rocksdb::PerfLevel::kEnableTime);
    // rocksdb::get_perf_context ()->Reset ();
    // rocksdb::get_perf_context ()->EnablePerLevelPerfContext ();
    // rocksdb::PerfContext* perf_stat = rocksdb::get_perf_context ();

    // rocksdb::get_iostats_context ()->Reset ();
    // rocksdb::IOStatsContext* io_stat = rocksdb::get_iostats_context ();

    // printf ("======= Negtive Single-Threaded Read =======\n");
    // start = std::chrono::steady_clock::now ();
    // for (int i = 0; i < kNum; i++) {
    //     if (i % 5000000 == 0) {
    //         printf ("Negtive Read %d\n", i);
    //     }
    //     std::string key = std::to_string (i + kNum + kNum) + key_suffix;
    //     std::string cvalue;
    //     auto s = db_->Get (rocksdb::ReadOptions (), key, &cvalue);
    //     if (s.ok ()) {
    //         printf ("false 1\n");
    //     }
    //     assert (!s.ok ());
    // }
    // end = std::chrono::steady_clock::now ();
    // std::cout << "used " << (end - start).count () << std::endl;
    // printf ("BLOOMFILTER\nNegative %s\n", perf_stat->ToString ().c_str ());
    // printf ("BLOOMFILTER\nNegative %s", io_stat->ToString ().c_str ());

    // rocksdb::SetPerfLevel (rocksdb::PerfLevel::kEnableTime);
    // rocksdb::get_perf_context ()->Reset ();
    // rocksdb::get_perf_context ()->EnablePerLevelPerfContext ();
    // rocksdb::PerfContext* perf_stat2 = rocksdb::get_perf_context ();

    // rocksdb::get_iostats_context ()->Reset ();
    // rocksdb::IOStatsContext* io_stat2 = rocksdb::get_iostats_context ();
    // printf ("======= Positive Single-Threaded Read =======\n");
    // start = std::chrono::steady_clock::now ();

    // for (int i = 0; i < kNum; i++) {
    //     if (i % 5000000 == 0) {
    //         printf ("Positive Read %d\n", i);
    //     }

    //     std::string key = std::to_string (i) + key_suffix;
    //     std::string cvalue;
    //     auto s = db_->Get (rocksdb::ReadOptions (), key, &cvalue);
    //     if (!s.ok ()) {
    //         printf ("false 2\n");
    //     }
    //     assert (s.ok ());
    // }
    // end = std::chrono::steady_clock::now ();
    // std::cout << "used " << (end - start).count () << std::endl;
    // printf ("BLOOMFILTER\nPositive %s", perf_stat2->ToString ().c_str ());
    // printf ("BLOOMFILTER\nPositive %s", io_stat2->ToString ().c_str ());

    delete db_;

    return 0;
}