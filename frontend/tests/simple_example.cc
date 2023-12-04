// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "./rocksdb_simple_example";
#endif

// void printStatistics () { printf ("Statistics\n%s \n", options_.statistics->ToString ().c_str
// ()); }

using namespace rocksdb;

const int kThreadNum = 1;
const long kNum = 10000000;
thread_local long kBatch = 1;
const std::string key_suffix = "_A_LONG_SUFFIX";
const std::string value = "123456789012345678901234567890";
// const std::string value = std::string(1024, 'a');
std::vector<rocksdb::ColumnFamilyHandle*> handles;
rocksdb::WriteOptions writeOptions;

void Func (DB* db, long start, long count) {
    WriteBatch batch;
    batch.Clear ();
    rocksdb::Status s;
    std::cout << "Thread " << std::this_thread::get_id () << " handle key range: " << start
              << " to " << start + count << std::endl;
    for (long i = 0; i < count / kBatch; i++) {
        if ((i % 1000000) == 0) std::cout << i << std::endl;
        for (long j = 0; j < kBatch; j++, i++) {
            s = batch.Put (std::to_string (i + start) + key_suffix, value);
        }
        db->Write (WriteOptions (), &batch);
        // s = db->Put (writeOptions, std::to_string (i + start) + key_suffix, value);
        if (!s.ok ()) {
            std::cout << "Thread " << std::this_thread::get_id () << " put error: " << s.ToString ()
                      << std::endl;
            break;
        }
    }
}

void Func2 (DB* db, long start, long count, int tid) {
    WriteBatch batch;
    batch.Clear ();
    Status s;
    std::cout << "Thread " << std::this_thread::get_id () << " handle key range: " << start
              << " to " << start + count << std::endl;
    for (long i = 0; i < count; i++) {
        if ((i % 1000000) == 0) std::cout << i << std::endl;
        // for (long j = 0; j < kBatch; j++, i++) {
        //     s = batch.Put (std::to_string (i + start) + key_suffix, value);
        // }
        // db->Write (WriteOptions (), &batch);
        s = db->Put (writeOptions, handles[tid], std::to_string (i + start) + key_suffix, value);
        if (!s.ok ()) {
            std::cout << "Thread " << std::this_thread::get_id () << " put error: " << s.ToString ()
                      << std::endl;
            break;
        }
    }
}

int main () {
    // DB* db;
    // Options options;
    // options.use_direct_io_for_flush_and_compaction = true;
    // // options.PrepareForBulkLoad ();
    // options.memtable_whole_key_filtering = true;
    // options.memtable_prefix_bloom_size_ratio = 0.1;
    // // options.optimize_filters_for_hits = true;

    // // options.use_direct_reads = true;
    // rocksdb::WriteOptions write_option_disable;
    // write_option_disable.disableWAL = true;
    // write_option_disable.memtable_insert_hint_per_batch = true;

    // // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    // options.IncreaseParallelism ();
    // options.OptimizeLevelStyleCompaction ();
    // // options.allow_concurrent_memtable_write = false;
    // // options.enable_write_thread_adaptive_yield = false;
    // // create the DB if it's not already present
    // options.create_if_missing = true;
    // rocksdb::SetPerfLevel (rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
    // rocksdb::PerfContext ().Reset ();
    // rocksdb::IOStatsContext ().Reset ();

    // BlockBasedTableOptions table_options;

    // table_options.whole_key_filtering = true;
    // table_options.block_cache = rocksdb::NewLRUCache (8 * 1024 * 1024);
    // // table_options.cache_index_and_filter_blocks = true;
    // table_options.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
    // options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (table_options));

    // // open DB
    // Status s = DB::Open (options, kDBPath, &db);
    // assert (s.ok ());

    // printf ("=================== TEST ====================\n");
    // WriteBatch batch;
    // uint64_t nthread = 4;
    // uint64_t nkeys = 16000000;
    // std::thread threads[nthread];
    // uint64_t idx[4];
    // std::atomic<bool> isValid = false;
    // idx[0] = 0;
    // idx[1] = 16000000;
    // idx[2] = 32000000;
    // idx[3] = 64000000;

    // for (int t = 0; t < 4; t++) {
    //     threads[t] = std::thread ([db, &idx, nthread, nkeys, &write_option_disable, &t, &batch] {
    //         WriteBatch batchk;
    //         thread_local int tid = t;
    //         for (int i = 0; i < nkeys / nthread; i++) {
    //             std::string* key = new std::string ("WVERIFY" + std::to_string (idx[t]++));
    //             std::string* value = new std::string (
    //                 "MOCKMOCKMOMOCKMOCKMOMOCKMOCKMOMOCKMOCKMOMOCKMOCKMOMOCKMOCKMOMOCKMO"
    //                 "CKMOMOCKMOCKMO");

    //             auto ikey = rocksdb::Slice (key->data (), key->size ());
    //             auto ivalue = rocksdb::Slice (value->data (), value->size ());
    //             if (i % 1000000 == 0) {
    //                 printf ("%lu, %d\n",
    //                         std::hash<std::thread::id> () (std::this_thread::get_id ()), tid);
    //                 // printf ("%lu, %lu\n", ikey.size (), ivalue.size ());
    //             }

    //             batchk.Put (ikey, ivalue);

    //             if (batchk.Count () > 0 && batchk.Count () % 1000 == 0) {
    //                 db->Write (write_option_disable, &batchk);
    //                 batchk.Clear ();
    //             }
    //             // db->Put(write_option_disable, ikey, ivalue);
    //             delete key;
    //             delete value;
    //         }
    //         return 0;
    //     });
    // }
    // for (auto& t : threads) {
    //     t.join ();
    // }
    // printStatistics ();
    // std::string stats;
    // db->GetProperty ("rocksdb.stats", &stats);
    // printf ("%s\n", stats.c_str ());

    // printf ("======= Positive Single-Threaded Read =======\n");
    // auto start = std::chrono::high_resolution_clock::now ();
    // idx[0] = 0;
    // idx[1] = 16000000;
    // idx[2] = 32000000;
    // idx[3] = 64000000;
    // for (int i = 0; i < nkeys / nthread; i++) {
    //     if (i % 1000000 == 0) printf ("current %d\n", i * 4);
    //     std::string key1 = "WVERIFY" + std::to_string (idx[0]++);
    //     std::string key2 = "WVERIFY" + std::to_string (idx[1]++);
    //     std::string key3 = "WVERIFY" + std::to_string (idx[2]++);
    //     std::string key4 = "WVERIFY" + std::to_string (idx[3]++);
    //     std::string cvalue1;
    //     std::string cvalue2;
    //     std::string cvalue3;
    //     std::string cvalue4;
    //     auto s1 = db->Get (rocksdb::ReadOptions (), key1, &cvalue1);
    //     assert (s1.ok ());
    //     auto s2 = db->Get (rocksdb::ReadOptions (), key2, &cvalue2);
    //     assert (s2.ok ());
    //     auto s3 = db->Get (rocksdb::ReadOptions (), key3, &cvalue3);
    //     assert (s3.ok ());
    //     auto s4 = db->Get (rocksdb::ReadOptions (), key4, &cvalue4);
    //     assert (s4.ok ());
    // }
    // auto end = std::chrono::high_resolution_clock::now ();
    // std::chrono::duration<double, std::milli> tm = end - start;
    // std::cout << tm.count () << std::endl;
    // exit (1);
    // // start = std::chrono::high_resolution_clock::now();
    // // printf("================\n");
    // // for (int i = 0; i < 16000000; i++) {
    // //   if (i % 1000000 == 0) printf("current %d\n", i);
    // //   std::string key = "WVERIFY" + std::to_string(idx[0]++);
    // //   std::string cvalue;
    // //   auto s = db->Get(rocksdb::ReadOptions(), key, &cvalue);
    // //   assert(!s.ok());
    // // }
    // // end = std::chrono::high_resolution_clock::now();
    // // tm = end - start;
    // // std::cout << tm.count() << std::endl;

    // // std::string stats1;
    // // db->GetProperty("rocksdb.stats", &stats1);
    // // printf("%s\n", stats.c_str());

    // // printStatistics();

    // printf ("=================== TEST ====================\n");

    // // Put key-value
    // s = db->Put (WriteOptions (), "key1", "value");
    // assert (s.ok ());
    // std::string value;
    // // get value
    // s = db->Get (ReadOptions (), "key1", &value);
    // assert (s.ok ());
    // assert (value == "value");

    // // atomically apply a set of updates
    // {
    //     WriteBatch batch;
    //     batch.Delete ("key1");
    //     batch.Put ("key2", value);
    //     s = db->Write (WriteOptions (), &batch);
    // }

    // s = db->Get (ReadOptions (), "key1", &value);
    // assert (s.IsNotFound ());

    // db->Get (ReadOptions (), "key2", &value);
    // assert (value == "value");

    // {
    //     PinnableSlice pinnable_val;
    //     db->Get (ReadOptions (), db->DefaultColumnFamily (), "key2", &pinnable_val);
    //     assert (pinnable_val == "value");
    // }

    // {
    //     std::string string_val;
    //     // If it cannot pin the value, it copies the value to its internal buffer.
    //     // The intenral buffer could be set during construction.
    //     PinnableSlice pinnable_val (&string_val);
    //     db->Get (ReadOptions (), db->DefaultColumnFamily (), "key2", &pinnable_val);
    //     assert (pinnable_val == "value");
    //     // If the value is not pinned, the internal buffer must have the value.
    //     assert (pinnable_val.IsPinned () || string_val == "value");
    // }

    // PinnableSlice pinnable_val;
    // s = db->Get (ReadOptions (), db->DefaultColumnFamily (), "key1", &pinnable_val);
    // assert (s.IsNotFound ());
    // // Reset PinnableSlice after each use and before each reuse
    // pinnable_val.Reset ();
    // db->Get (ReadOptions (), db->DefaultColumnFamily (), "key2", &pinnable_val);
    // assert (pinnable_val == "value");
    // pinnable_val.Reset ();
    // // The Slice pointed by pinnable_val is not valid after this point

    // delete db;

    DB* db;
    writeOptions.disableWAL = true;

    Options options;
    options.IncreaseParallelism (8);
    options.OptimizeLevelStyleCompaction ();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 1024;
    options.db_write_buffer_size = 1 * 1024 * 1024 * 1024;

    std::cout << "use parallel" << std::endl;
    options.allow_concurrent_memtable_write = true;
    options.enable_write_thread_adaptive_yield = true;

    // options.memtable_whole_key_filtering = true;
    // options.memtable_prefix_bloom_size_ratio = 0.1;

    BlockBasedTableOptions table_options;

    table_options.whole_key_filtering = true;
    table_options.block_cache = rocksdb::NewLRUCache (0);
    // table_options.cache_index_and_filter_blocks = true;
    table_options.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
    options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (table_options));
    options.statistics = CreateDBStatistics ();

    // open DB
    Status s = DB::Open (options, kDBPath, &db);
    assert (s.ok ());

    rocksdb::ColumnFamilyOptions cf_options;
    handles.resize (kThreadNum);
    for (auto i = 0; i < handles.size (); i++) {
        // open the new one, too
        cf_options.write_buffer_size = 1024 * 1024 * 1024;
        auto c = db->CreateColumnFamily (cf_options, std::to_string (i), &(handles[i]));
        if (!c.ok ()) {
            std::cout << "Creating column family failed -> " << i << std::endl;
        } else {
            std::cout << "Creating column family successed -> " << i << std::endl;
        }
        assert (c.ok ());
    }

    std::thread* threads[kThreadNum];
    auto start = std::chrono::steady_clock::now ();
    for (int i = 0; i < kThreadNum; i++) {
        threads[i] = new std::thread (Func, db, i * kNum / kThreadNum, kNum / kThreadNum);
        std::this_thread::sleep_for (std::chrono::microseconds (100));
    }
    // for (int i = 0; i < kThreadNum; i++) {
    //     threads[i] = new std::thread (Func2, db, i * kNum / kThreadNum, kNum / kThreadNum, i);
    //     std::this_thread::sleep_for (std::chrono::microseconds (100));
    // }

    for (auto& t : threads) {
        t->join ();
    }
    auto end = std::chrono::steady_clock::now ();

    std::cout << "used " << (end - start).count () << std::endl;

    for (int i = 0; i < kThreadNum; i++) {
        delete threads[i];
    }

    Statistics* statistics = options.statistics.get ();
    printf ("Statistics\n%s \n", statistics->ToString ().c_str ());
    // Statistics::getHistogramString();
    exit (1);

    rocksdb::SetPerfLevel (rocksdb::PerfLevel::kEnableTime);
    rocksdb::get_perf_context ()->Reset ();
    // rocksdb::get_perf_context ()->EnablePerLevelPerfContext ();
    // rocksdb::PerfContext* perf_stat = rocksdb::get_perf_context ();

    // rocksdb::get_iostats_context ()->Reset ();
    // rocksdb::IOStatsContext* io_stat = rocksdb::get_iostats_context ();

    printf ("======= Negtive Single-Threaded Read =======\n");
    start = std::chrono::steady_clock::now ();
    for (int i = 0; i < kNum; i++) {
        if (i % 5000000 == 0) {
            printf ("Negtive Read %d\n", i);
        }
        std::string key = std::to_string (i + kNum + kNum) + key_suffix;
        std::string cvalue;
        auto s = db->Get (rocksdb::ReadOptions (), key, &cvalue);
        if (s.ok ()) {
            printf ("false 1\n");
        }
        assert (!s.ok ());
    }
    end = std::chrono::steady_clock::now ();
    std::cout << "used " << (end - start).count () << std::endl;
    // printf ("BLOOMFILTER\nNegative %s\n", perf_stat->ToString ().c_str ());
    // printf ("BLOOMFILTER\nNegative %s", io_stat->ToString ().c_str ());

    // rocksdb::SetPerfLevel (rocksdb::PerfLevel::kEnableTime);
    // rocksdb::get_perf_context ()->Reset ();
    // rocksdb::get_perf_context ()->EnablePerLevelPerfContext ();
    // rocksdb::PerfContext* perf_stat2 = rocksdb::get_perf_context ();

    // rocksdb::get_iostats_context ()->Reset ();
    // rocksdb::IOStatsContext* io_stat2 = rocksdb::get_iostats_context ();
    printf ("======= Positive Single-Threaded Read =======\n");
    start = std::chrono::steady_clock::now ();

    for (int i = 0; i < kNum; i++) {
        if (i % 5000000 == 0) {
            printf ("Positive Read %d\n", i);
        }

        std::string key = std::to_string (i) + key_suffix;
        std::string cvalue;
        auto s = db->Get (rocksdb::ReadOptions (), key, &cvalue);
        if (!s.ok ()) {
            printf ("false 2\n");
        }
        assert (s.ok ());
    }
    end = std::chrono::steady_clock::now ();
    std::cout << "used " << (end - start).count () << std::endl;
    // printf ("BLOOMFILTER\nPositive %s", perf_stat2->ToString ().c_str ());
    // printf ("BLOOMFILTER\nPositive %s", io_stat2->ToString ().c_str ());

    delete db;

    return 0;
}
