#pragma once
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
// -------------------------------------------------------------------------------------
#include <jemalloc/jemalloc.h>
#include <tbb/parallel_for.h>

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <queue>
#include <random>
#include <string>
#include <vector>

#include "artree-ori/Tree.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"

using namespace ART_OLC_ORI;
// -------------------------------------------------------------------------------------

class LSM {
public:
    LSM () { initializeLSM (); }
    ~LSM () { delete db; }

public:
    rocksdb::WriteBatch writeBatch;
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::DBOptions dboptions;
    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;
    rocksdb::BlockBasedTableOptions table_options;
    std::string cf_name;

    void initializeLSM () {
        write_options.disableWAL = true;
        write_options.sync = false;

        // Enable the bloomfilter
        // table_options.block_cache = rocksdb::NewLRUCache (1 * 1024 * 1024 * 1024);
        table_options.whole_key_filtering = true;
        table_options.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));

        // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
        options.IncreaseParallelism (5);
        options.OptimizeLevelStyleCompaction ();
        options.create_if_missing = true;

        // options.memtable_whole_key_filtering = true;
        // options.memtable_prefix_bloom_size_ratio = 0.1;

        options.max_background_compactions = 2;
        options.max_background_flushes = 2;
        options.level0_stop_writes_trigger = 1410065407;
        options.level0_slowdown_writes_trigger = 1410065407;
        options.level0_file_num_compaction_trigger = 1410065407;
        options.disable_auto_compactions = true;

        options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (table_options));

        // create the database
        rocksdb::Status s =
            rocksdb::DB::Open (options, "artree_" + std::to_string ((uint64_t)this), &db);
        assert (s.ok ());
    }
};
// -------------------------------------------------------------------------------------
template <class Record>
class ARTLSM {
public:
    ART_OLC_ORI::Tree* tree_;
    std::string idx_name;
    LSM* lsm_;
    // Get real value based on pointer and release the memory
    using getObjectFn = std::function<void (uint64_t, std::string&)>;
    static inline getObjectFn getObject{};
    // Allocate a new space for value and get its pointer (Row id)
    using getRidFn = std::function<void (uint64_t&, char*)>;
    static inline getRidFn getRid{};
    using releaseValueFn = std::function<void (uint64_t, std::atomic<uint64_t>&)>;
    static inline releaseValueFn releaseValue{};

    static bool OffLoadFn (void* store, uint8_t* key, size_t keyLen, TID tid) {
        rocksdb::WriteOptions writeDisableWAL;
        writeDisableWAL.disableWAL = true;

        std::string ivalue;
        getObject (tid, ivalue);

        rocksdb::DB* db = reinterpret_cast<rocksdb::DB*> (store);
        rocksdb::Status s;

        s = db->Put (writeDisableWAL, rocksdb::Slice ((char*)key, keyLen), ivalue);
        return s.ok ();
    }

    static bool BatchOffLoadFn (void* store, uint8_t* key, size_t keyLen, TID tid,
                                void* writeBatch) {
        rocksdb::WriteOptions writeDisableWAL;
        writeDisableWAL.disableWAL = true;
        std::string ivalue;
        getObject (tid, ivalue);

        rocksdb::DB* db = reinterpret_cast<rocksdb::DB*> (store);
        if (!db) return false;
        assert (db);
        rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (writeBatch);
        rocksdb::Status s = wb->Put (rocksdb::Slice ((char*)key, keyLen), ivalue);
        return s.ok ();
    }

    static bool DBLookupFn (void* store, uint8_t* key, size_t keyLen) {
        rocksdb::DB* db = reinterpret_cast<rocksdb::DB*> (store);
        std::string val;
        auto s = db->Get (rocksdb::ReadOptions (), rocksdb::Slice ((char*)key, keyLen), &val);

        return s.ok ();
    }

public:
    void CreateIndex (const std::function<void (uint64_t, std::string&)>& fn1,
                      const std::function<void (uint64_t&, char*)>& fn2,
                      const std::function<void (uint64_t, std::atomic<uint64_t>&)>& fn3) {
        // the callback function to allocate/deallocate the real value
        getObject = fn1;
        getRid = fn2;
        releaseValue = fn3;
    }

    bool UpsertImpl (const IndexKey& key, TID rid) {
        auto thread = tree_->getThreadInfo ();
        bool seeHybrid = false;
        bool ret;
        std::string tidlist_str;

        ret = tree_->insert (key, rid, thread, nullptr, nullptr, &seeHybrid);
        return ret;
    }

    TID DBLookup (const IndexKey& key) {
        // encounter a hybrid node in artree during lookup, we should search in rocksdb
        std::string tidlist_str;
        auto s =
            lsm_->db->Get (lsm_->read_options,
                           rocksdb::Slice ((char*)key.getData (), key.getKeyLen ()), &tidlist_str);
        assert (s.ok ());
        return std::stoul (tidlist_str);
    }

    TID LookupImpl (IndexKey& key) {
        auto thread = tree_->getThreadInfo ();
        bool seeHybrid = false;
        auto tid = tree_->lookup (key, thread, nullptr, nullptr, &seeHybrid);
        // TODO: When to search key from LSM
        if (tid == 0) {
            // search from the lsm
            std::string tidlist_str;
            auto s = lsm_->db->Get (lsm_->read_options,
                                    rocksdb::Slice ((char*)key.getData (), key.getKeyLen ()),
                                    &tidlist_str);
            if (!s.ok ()) {
                printf ("whatwhatwhat %lu\n", tid);
            }
            assert (s.ok ());
            return std::stoul (tidlist_str);
        }
        return tid;
    }

    TID LookupRangeImpl (const IndexKey& keyleft, const IndexKey& keyright,
                         ART::ThreadInfo& thread) {
        bool seeHybrid = false;

        TID mtagets[1000];
        Key ckey;
        size_t found = 0;

        auto find = tree_->lookupRange (false, keyleft, keyright, ckey, mtagets, 1000, found,
                                        thread, &seeHybrid);
        if (found > 0) {
            return 1;
        }

        if (seeHybrid) {
            return DBLookup (ckey);
        }
        return 0;
    }

    bool DeleteImpl (const IndexKey& key, TID rid) {
        auto thread = tree_->getThreadInfo ();
        bool seeHybrid = false;
        bool res = tree_->remove (key, rid, thread, nullptr, &seeHybrid);
        if (!seeHybrid) {
            return res;
        }

        auto s = lsm_->db->Delete (lsm_->write_options,
                                   rocksdb::Slice ((char*)key.getData (), key.getKeyLen ()));

        return s.ok ();
    }

public:
    ART::ThreadInfo getThreadInfo () { return tree_->getThreadInfo (); }

    std::string ToStats () { return tree_->ToStats (); }

public:
    ARTLSM () {}
    ~ARTLSM () {
        // TODO: releas ridlist
        // tree_->ReleaseTree ([] (TID ridlist) {});
        delete tree_;
        tree_ = nullptr;
    }
};
