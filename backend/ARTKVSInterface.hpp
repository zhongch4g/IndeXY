#pragma once
#ifndef ARTKVS_INTERFACE_H
#define ARTKVS_INTERFACE_H

#include "leanstore/LeanStore.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
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
#include <iomanip>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "XYConfig.hpp"
#include "artree-shared-headers/Key.h"
#include "artreeX/Tree.h"
#include "logger.h"
// =====================================================================
// Rocksdb's header
// =====================================================================
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"


// =====================================================================
using namespace ART_OLC_X;
// =====================================================================
// Get LoadKey callback function for ARTree
using loadKeyCallbackFn = bool (*) (TID tid, Key& key);
// Get real value based on pointer and release the memory
using getObjectFn = std::function<void (uint64_t, std::string&)>;
// Allocate a new space for value and get its pointer (Row id)
using getRidFn = std::function<void (uint64_t&, char*)>;
// Get the value via RID and release the value
using releaseValueFn = std::function<void (uint64_t, std::atomic<uint64_t>&)>;
// =====================================================================

namespace ARTKVS {
class ProducerConsumerQueue {
    std::vector<candidate_t*> arr;  // array to store queue elements
    int capacity;                   // maximum capacity of the queue
    int front;                      // front points to the front element in the queue (if any)
    int rear;                       // rear points to the last element in the queue
    int count;                      // current size of the queue

public:
    // Constructor to initialize a queue
    ProducerConsumerQueue (int size) {
        arr.resize (size);

        capacity = size;
        front = 0;
        rear = -1;
        count = 0;
    }
    // Utility function to return the size of the queue
    int size ();
    // Utility function to check if the queue is empty or not
    bool isEmpty ();
    // Utility function to check if the queue is full or not
    bool isFull ();
    // Utility function to dequeue the front element
    void dequeue ();
    // Utility function to add an item to the queue
    void enqueue (candidate_t* item);
    // Utility function to return the front element of the queue
    candidate_t* peek ();
    void destroy ();
};

class ThreadLocalExtra {
public:
    ThreadLocalExtra () { batch_ = new rocksdb::WriteBatch (); }
    rocksdb::WriteBatch* batch_;
};

class InstanceExtra {
public:
    InstanceExtra (const getObjectFn& fn1, const getRidFn& fn2, const releaseValueFn& fn3) {
        getObject = fn1;
        getRid = fn2;
        releaseValue = fn3;
    }

public:
    getObjectFn getObject{};
    getRidFn getRid{};
    releaseValueFn releaseValue{};
};

class RocksdbY {
public:
    static bool KVStoreLookupFn (void* store, uint8_t* key, size_t keyLen) {
        rocksdb::DB* db = reinterpret_cast<rocksdb::DB*> (store);
        std::string val;
        auto s = db->Get (rocksdb::ReadOptions (), rocksdb::Slice ((char*)key, keyLen), &val);

        return s.ok ();
    }

    static bool UnloadFn (void* store, uint8_t* key, size_t keyLen, TID tid,
                          void* threadLocalExtra) {
        RocksdbY* db = reinterpret_cast<RocksdbY*> (store);
        rocksdb::WriteOptions writeDisableWAL;
        writeDisableWAL.disableWAL = true;

        std::string ivalue;
        db->instanceExtra->getObject (tid, ivalue);

        rocksdb::DB* db_ = reinterpret_cast<rocksdb::DB*> (db->db);
        ThreadLocalExtra* ey = reinterpret_cast<ThreadLocalExtra*> (threadLocalExtra);
        rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (ey->batch_);

        rocksdb::Status s = wb->Put (rocksdb::Slice ((char*)key, keyLen), ivalue);
        return s.ok ();
    }

public:
    void initializeDB (std::string name, const getObjectFn& fn1, const getRidFn& fn2,
                       const releaseValueFn& fn3);
    bool lookup (uint8_t* key, size_t keyLen, uint64_t& tid_kvs);
    bool scan (uint8_t* key, size_t keyLen, uint64_t scan_length);

public:
    rocksdb::WriteBatch writeBatch;
    rocksdb::DB* db;
    rocksdb::Options options;
    rocksdb::DBOptions dboptions;
    rocksdb::WriteOptions woptions;
    rocksdb::ReadOptions roptions;
    rocksdb::BlockBasedTableOptions tableOptions;
    uint64_t columnFamilyNums_;
    std::vector<void*> handles;
    std::string dbname_ = "";
    InstanceExtra* instanceExtra;
};

class LeanstoreY {
public:
    static bool KVStoreLookupFn (void* store, uint8_t* key, size_t keyLen) {
        LeanstoreY* db = reinterpret_cast<LeanstoreY*> (store);
        std::string val;
        uint64_t tid_kvs;
        db->lookup (key, keyLen, tid_kvs);
        return 1;
    }

    static bool UnloadFn (void* store, uint8_t* key, size_t keyLen, TID tid,
                          void* threadLocalExtra) {
        LeanstoreY* leanWrapper = reinterpret_cast<LeanstoreY*> (store);
        std::string ivalue;
        leanWrapper->instanceExtra->getObject (tid, ivalue);
        leanWrapper->insert (key, keyLen, reinterpret_cast<uint8_t*> (ivalue.data ()),
                             ivalue.size ());
        return 1;
    }

public:
    void initializeDB (leanstore::LeanStore& db, std::string name, const getObjectFn& fn1,
                       const getRidFn& fn2, const releaseValueFn& fn3);
    bool insert (uint8_t* key, size_t keyLen, uint8_t* value, size_t valueLen);
    bool lookup (uint8_t* key, size_t keyLen, uint64_t& tid_kvs);
    bool scan (uint64_t rid, uint64_t scan_length);
    unsigned fold (uint8_t* writer, const s32& x);
    unsigned fold (uint8_t* writer, const s64& x);
    unsigned fold (uint8_t* writer, const u64& x);
    unsigned fold (uint8_t* writer, const u32& x);

public:
    leanstore::storage::btree::BTreeInterface* btree;
    std::string dbname_ = "";
    InstanceExtra* instanceExtra;
};

class ARTreeKVS {
public:
    ARTreeKVS (leanstore::LeanStore& leanstore_db, getObjectFn fn1, getRidFn fn2,
               releaseValueFn fn3, std::string name);
    ARTreeKVS (rocksdb::DB* rocksdb_db, getObjectFn fn1, getRidFn fn2, releaseValueFn fn3,
               std::string name);
    ARTreeKVS (leanstore::LeanStore& leanstore_db, rocksdb::DB* rocksdb_db, getObjectFn fn1,
               getRidFn fn2, releaseValueFn fn3, std::string name);
    void CreateARTreeKVS (loadKeyCallbackFn loadkey, getObjectFn fn1, getRidFn fn2,
                          releaseValueFn fn3, std::string idx_name);
    bool Insert (const IndexKey& key, TID rid);
    TID Lookup (IndexKey& key, uint64_t& disk_access);
    TID Lookup2 (IndexKey& key, TID rid, uint64_t& disk_access);
    TID LookupRange (const IndexKey& keyleft, const IndexKey& keyright);
    bool LookupIterator (const IndexKey& target, TID rid);
    bool Delete (const IndexKey& key, TID rid);
    void SelectVictimPolicy (UnloadPolicy* policy, size_t dumpSize,
                             std::vector<candidate_t*>& publicList, uint64_t& publicListLen_);
    double unloadDirtyPrefix (const IndexKey key, ThreadLocalExtra* threadLocalExtra);
    double getPrefixSubtreeSize (IndexKey key);
    bool releaseCleanPrefix (const IndexKey& key, std::atomic<uint64_t>& vDeallocated,
                             std::function<void (uint64_t, std::atomic<uint64_t>&)>& releaseMemory,
                             ThreadLocalExtra* threadLocalExtra);
    void startBackgroundThreads ();
    void stopBackgroundThreads ();
    void traverse ();
    void producer ();
    void release ();
    void load ();
    void consumer ();
    uint64_t idxMemoryUsageInfo ();
    uint64_t vMemoryInfo ();
    std::string ToStats ();
    void getPrefixMapCounter ();
    void ReleaseX ();
    // =====================================================================

public:
    getObjectFn getObject{};
    getRidFn getRid{};
    releaseValueFn releaseValue{};

public:
    bool ARTcache = false;

public:
    ART_OLC_X::Tree* tree_;
    LeanstoreY* leanstore_y_;
    RocksdbY* rocksdb_y_;

    std::string idx_name_;
    std::atomic<bool> bg_threads_keep_running;
    bool release_keep_running;
    bool temp_stop_insert = false;
    uint64_t ctn_idx_producer = 0;
    uint64_t ctn_idx_release = 0;
    std::atomic<uint64_t> vMemoryAllocated = 0;
    std::atomic<uint64_t> vMemoryDeallocated = 0;
    double prevMaxSubtreeSize = 0;
    uint64_t initLength;
    uint64_t diskread_ = 0;

    // the queue for traverse thread collecting the kth level nodes from artree
    std::vector<candidate_t*> publicList;
    uint64_t publicListLen_;
    std::mutex queueLock;
    std::condition_variable queuelock_cv;
    int STOP_COUNTER;
    ProducerConsumerQueue generalPCQuene{2};
    std::vector<std::thread> background_thread;
    std::atomic<bool> ready_to_refresh_publiclist{true};

    // the queue and lock for multh-thread unloading and releasing
    std::queue<candidate_t*> producerConsumerQueue;
    std::condition_variable consumerwait_cv;
    std::condition_variable not_full_cv_;
    std::condition_variable not_empty_cv_;
    std::mutex pc_mu;  // synchronize producer and consumer
    // producer thread bit status
    uint64_t release_counter = 0, unload_counter = 0;
    std::atomic<bool> force_stop_producer_thread{false};
    std::atomic<bool> force_stop_release_thread{false};
    // consumer thread
    std::atomic<uint64_t> nUnloadingThreadsWorking{0};
    std::atomic<uint64_t> nReleasingThreadsWorking{0};

    // Profiling
    std::unordered_map<std::string, uint64_t> unload_prefix_map;
    std::unordered_map<std::string, uint64_t> release_prefix_map;
};

};  // namespace ARTKVS

#endif
