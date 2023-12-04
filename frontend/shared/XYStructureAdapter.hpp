#ifndef CONNECTOR_OLD_H
#define CONNECTOR_OLD_H

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
#include <iomanip>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <vector>

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

#define LEANSTORE
// #define ROCKSDB
// =====================================================================
using namespace ART_OLC_X;
// =====================================================================
using KVStoreLookupFunc = std::function<bool (void*, uint8_t*, size_t)>;
using KVStoreWriteFunc = std::function<bool (void*, uint8_t*, size_t, TID)>;
// =====================================================================
// Get real value based on pointer and release the memory
using getObjectFn = std::function<void (uint64_t, std::string&)>;
static inline getObjectFn getObject{};
// Allocate a new space for value and get its pointer (Row id)
using getRidFn = std::function<void (uint64_t&, char*)>;
static inline getRidFn getRid{};
// Get the value via RID and release the value
using releaseValueFn = std::function<void (uint64_t, uint64_t&)>;
static inline releaseValueFn releaseValue{};
// =====================================================================

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
    int size () { return count; }
    // Utility function to check if the queue is empty or not
    bool isEmpty () { return (size () == 0); }
    // Utility function to check if the queue is full or not
    bool isFull () { return (size () == capacity); }
    // Utility function to dequeue the front element
    void dequeue () {
        // check for queue underflow
        if (isEmpty ()) {
            exit (EXIT_FAILURE);
        }
        front = (front + 1) % capacity;
        count--;
    }
    // Utility function to add an item to the queue
    void enqueue (candidate_t* item) {
        // check for queue overflow
        if (isFull ()) {
            exit (EXIT_FAILURE);
        }
        rear = (rear + 1) % capacity;
        arr[rear] = item;
        count++;
    }
    // Utility function to return the front element of the queue
    candidate_t* peek () {
        if (isEmpty ()) {
            exit (EXIT_FAILURE);
        }
        return arr[front];
    }
    void destroy () {}
};

class ExtraY {
public:
    ExtraY () { batch_ = new rocksdb::WriteBatch (); }
    rocksdb::WriteBatch* batch_;
};

template <class Record>
class RocksdbY {
public:
    static bool KVStoreLookupFn (void* store, uint8_t* key, size_t keyLen) {
        rocksdb::DB* db = reinterpret_cast<rocksdb::DB*> (store);
        std::string val;
        auto s = db->Get (rocksdb::ReadOptions (), rocksdb::Slice ((char*)key, keyLen), &val);

        return s.ok ();
    }

    static bool UnloadFn (void* store, uint8_t* key, size_t keyLen, TID tid, void* extra) {
        RocksdbY<Record>* db = reinterpret_cast<RocksdbY<Record>*> (store);
        rocksdb::WriteOptions writeDisableWAL;
        writeDisableWAL.disableWAL = true;

        std::string ivalue;
        getObject (tid, ivalue);

        rocksdb::DB* db_ = reinterpret_cast<rocksdb::DB*> (db->db);
        ExtraY* ey = reinterpret_cast<ExtraY*> (extra);
        rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (ey->batch_);

        rocksdb::Status s = wb->Put (rocksdb::Slice ((char*)key, keyLen), ivalue);
        return s.ok ();
    }

public:
    void initializeDB (const getObjectFn& fn1, const getRidFn& fn2, const releaseValueFn& fn3);
    void registerKVStoreLookupFunc ();
    bool insert ();
    bool lookup ();

public:
    KVStoreLookupFunc kvStoreLookupFunc;
    KVStoreWriteFunc kvStoreWriteFunc;
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

public:
    static inline getObjectFn getObject{};
    static inline getRidFn getRid{};
    static inline releaseValueFn releaseValue{};
};

template <class Record>
class LeanstoreY {
public:
    static bool KVStoreLookupFn (void* store, uint8_t* key, size_t keyLen) {
        LeanstoreY<Record>* db = reinterpret_cast<LeanstoreY<Record>*> (store);
        std::string val;
        uint8_t v[sizeof (uint64_t)];
        db->lookup (key, keyLen, v);
        return 1;
    }

    static bool UnloadFn (void* store, uint8_t* key, size_t keyLen, TID tid, void* extra) {
        LeanstoreY<Record>* leanWrapper = reinterpret_cast<LeanstoreY<Record>*> (store);
        std::string ivalue;
        getObject (tid, ivalue);
        leanWrapper->insert (key, keyLen, reinterpret_cast<uint8_t*> (ivalue.data ()),
                             ivalue.size ());
        return 1;
    }

public:
    void initializeDB (leanstore::LeanStore& db, std::string name, const getObjectFn& fn1,
                       const getRidFn& fn2, const releaseValueFn& fn3);
    bool insert (uint8_t* key, size_t keyLen, uint8_t* value, size_t valueLen);
    bool lookup (uint8_t* key, size_t keyLen, uint8_t* v);
    void registerKVStoreLookupFunc ();
    unsigned fold (uint8_t* writer, const s32& x);
    unsigned fold (uint8_t* writer, const s64& x);
    unsigned fold (uint8_t* writer, const u64& x);
    unsigned fold (uint8_t* writer, const u32& x);

public:
    leanstore::storage::btree::BTreeInterface* btree;
    KVStoreLookupFunc kvStoreLookupFunc;
    KVStoreWriteFunc kvStoreWriteFunc;
    std::string dbname_ = "";

public:
    static inline getObjectFn getObject{};
    static inline getRidFn getRid{};
    static inline releaseValueFn releaseValue{};
};

template <class Record, typename Y>
class ARTreeX {
public:
    void constructYStructure (Y* kvStoreY);
    void CreateIndex (const getObjectFn& fn1, const getRidFn& fn2, const releaseValueFn& fn3,
                      Tree* art, std::string idx_name, uint64_t initLen);
    bool Insert (const IndexKey& key, TID rid);
    TID Lookup (IndexKey& key);
    TID RecoveryLookup (IndexKey& key, uint64_t& disk_access);
    TID LookupRange (const IndexKey& keyleft, const IndexKey& keyright);
    bool Delete (const IndexKey& key, TID rid);
    void SelectVictimPolicy (UnloadPolicy* policy, size_t dumpSize,
                             std::vector<candidate_t*>& publicList, uint64_t& publicListLen_);
    double unloadDirtyPrefix (const IndexKey key, ExtraY* extra);
    double getPrefixSubtreeSize (IndexKey key);
    bool releaseCleanPrefix (const IndexKey& key, uint64_t& vDeallocated,
                             std::function<void (uint64_t, uint64_t&)>& releaseMemory,
                             ExtraY* extra);
    void startBackgroundThreads ();
    void stopBackgroundThreads ();
    void traverse ();
    void producer ();
    void release ();
    void load ();
    void consumer ();
    uint64_t structXMemoryInfo ();
    uint64_t structXVMemoryInfo ();
    std::string ToStats ();
    void getPrefixMapCounter ();
    // =====================================================================

public:
    static inline getObjectFn getObject{};
    static inline getRidFn getRid{};
    static inline releaseValueFn releaseValue{};

public:
    Tree* tree_;
    Y* kvStoreY;
    std::string idx_name_;
    std::atomic<bool> bg_threads_keep_running;
    bool release_keep_running;
    bool temp_stop_insert = false;
    uint64_t ctn_idx_producer = 0;
    uint64_t ctn_idx_release = 0;
    uint64_t vMemoryAllocated = 0;
    uint64_t vMemoryDeallocated = 0;
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

template <class Record, typename Y>
void ARTreeX<Record, Y>::constructYStructure (Y* kvStoreY) {
    this->kvStoreY = kvStoreY;
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::CreateIndex (const getObjectFn& fn1, const getRidFn& fn2,
                                      const releaseValueFn& fn3, Tree* art, std::string idx_name,
                                      uint64_t initLen) {
    // the callback function to allocate/deallocate the real value
    getObject = fn1;
    getRid = fn2;
    releaseValue = fn3;
    tree_ = art;
    idx_name_ = idx_name;
    tree_->urlevel_ = 2;  // for tpcc 3 for ycsb 2
    initLength = initLen;

    // Initilize the Public List
    auto thread_id = std::hash<std::thread::id> () (std::this_thread::get_id ());
    if (idx_name_ == "orderline" || idx_name == "ycsb") {
        publicList.resize (initLen);
        for (uint64_t i = 0; i < initLen; i++) {
            publicList[i] = new candidate_t{1, nullptr, ""};
            tree_->memoryInfo->increaseByBytes (thread_id, malloc_usable_size (publicList[i]));
        }
    }

    publicListLen_ = 0;
    printf ("finished Public List allocating %lu\n", tree_->memoryInfo->totalARTMemoryUsage ());
}

template <class Record, typename Y>
bool ARTreeX<Record, Y>::Insert (const IndexKey& key, TID rid) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    bool ret = tree_->insert (key, rid, thread, nullptr, nullptr, kvStoreY->kvStoreLookupFunc,
                              kvStoreY, &seeHybrid, true);
    return ret;
}

template <class Record, typename Y>
TID ARTreeX<Record, Y>::Lookup (IndexKey& key) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    auto tid = tree_->lookup (key, thread, nullptr, nullptr, &seeHybrid);
    // TODO: When to search key from LSM
    if (seeHybrid || !tid) {
        // search from the lsm
        uint8_t v[sizeof (uint64_t)];
        auto res = kvStoreY->lookup (key.getData (), key.getKeyLen (), v);
        std::string result (v, v + sizeof (v) / sizeof (v[0]));

        // std::cout << std::stoul (result) << std::endl;
        return res;
    }
    return tid;
}

template <class Record, typename Y>
TID ARTreeX<Record, Y>::RecoveryLookup (IndexKey& key, uint64_t& disk_access) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    auto tid = tree_->lookup (key, thread, nullptr, nullptr, &seeHybrid);
    if (seeHybrid || !tid) {
        // search from the lsm
        uint8_t v[sizeof (uint64_t)];
        auto res = kvStoreY->lookup (key.getData (), key.getKeyLen (), v);
        diskread_++;
        disk_access++;
        if (res) {
            // insert the key just looking up from Y to ARTree
            uint64_t rid = *reinterpret_cast<uint64_t*> (v);
            bool ret = tree_->insert (key, rid, thread, nullptr, nullptr,
                                      kvStoreY->kvStoreLookupFunc, kvStoreY, &seeHybrid, true);
        }
        return res;
    }
    return tid;
}

template <class Record, typename Y>
TID ARTreeX<Record, Y>::LookupRange (const IndexKey& keyleft, const IndexKey& keyright) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    TID mtagets[10000];
    Key ckey;
    size_t found = 0;

    auto find = tree_->lookupRange (false, keyleft, keyright, ckey, mtagets, 1000, found, thread,
                                    &seeHybrid);
    if (found > 0) {
        return 1;
    }

    if (seeHybrid) {
        // return kvStoreY->lookup();
    }
    return 0;
}

template <class Record, typename Y>
bool ARTreeX<Record, Y>::Delete (const IndexKey& key, TID rid) {
    // TODO: not complete (delete from the store)
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    bool res =
        tree_->remove (key, rid, thread, nullptr, Y::kvStoreLookupFunc, kvStoreY, &seeHybrid);
    if (!seeHybrid) {
        return res;
    }
    return 1;
}
template <class Record, typename Y>
void ARTreeX<Record, Y>::SelectVictimPolicy (UnloadPolicy* policy, size_t dumpSize,
                                             std::vector<candidate_t*>& publicList,
                                             uint64_t& publicListLen_) {
    auto thread = tree_->getThreadInfo ();
    tree_->calculateVictims (policy, dumpSize, thread, publicList, publicListLen_);
}
template <class Record, typename Y>
double ARTreeX<Record, Y>::unloadDirtyPrefix (const IndexKey key, ExtraY* extra) {
    // The number of keys in a subtree is used as a unit (batch); this can vary. 
    auto threadInfo = tree_->getThreadInfo ();
    std::pair<bool, double> res =
        tree_->unloadDirtyPrefix (key, threadInfo, kvStoreY, Y::UnloadFn, extra);
#ifdef ROCKSDB
    rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (extra->batch_);
    rocksdb::WriteOptions writeDisableWAL;
    writeDisableWAL.disableWAL = true;
    uint64_t keysInBatch = wb->Count ();
    kvStoreY->db->Write (writeDisableWAL, wb);
    wb->Clear ();
#endif

    return res.second;
}

template <class Record, typename Y>
double ARTreeX<Record, Y>::getPrefixSubtreeSize (IndexKey key) {
    auto size = tree_->getPrefixSubtreeSize (key);
    return size;
}

template <class Record, typename Y>
bool ARTreeX<Record, Y>::releaseCleanPrefix (
    const IndexKey& key, uint64_t& vDeallocated,
    std::function<void (uint64_t, uint64_t&)>& releaseMemory, ExtraY* extra) {
    auto threadInfo = tree_->getThreadInfo ();
    auto res = tree_->releaseCleanPrefix (key, threadInfo, vDeallocated, releaseMemory, kvStoreY,
                                          Y::UnloadFn, extra);
#ifdef ROCKSDB
    rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (extra->batch_);
    rocksdb::WriteOptions writeDisableWAL;
    writeDisableWAL.disableWAL = true;
    uint64_t keysInBatch = wb->Count ();
    kvStoreY->db->Write (writeDisableWAL, wb);
    wb->Clear ();
#endif
    return res;
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::stopBackgroundThreads () {
    bg_threads_keep_running.store (false);
    while (!generalPCQuene.isEmpty ()) {
        generalPCQuene.dequeue ();
    }
    while (STOP_COUNTER != 0) {
        not_full_cv_.notify_all ();
        not_empty_cv_.notify_all ();
    }
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::startBackgroundThreads () {
    // ClockStat
    // There are two bits for each inner node that represent write bit and unload bit,
    // respectively.
    // 1. write bit (identify if this node is recently unused or not)
    // 2. unload bit (identify if this subtree is clean)

    //  node1 -> node2 -> node3 -> node4 -> ... -> node N ->
    //   ^                                                 |
    //   |_________________________________________________|

    // Policy
    // We use an array to store the reference of each subtree in the 4th or 5th level of the
    // ART and recursively access each entry of the array to check the ClockStat of each
    // node (Clock replacement algorithm). In this way, we can decide which subtree we are
    // going to unload.

    // Two background thread
    // 1. Release thread -> release the memory of the subtree
    // 2. Unload thread -> unload the subtrees to the LSM (Rocksdb) but keep the memory
    // copies start the background threads
    printf ("[Start the Background Threads]\n");
    bg_threads_keep_running.store (true);
    release_keep_running = false;

    STOP_COUNTER = 4;

    background_thread.emplace_back (&ARTreeX<Record, Y>::traverse, this);
    background_thread.emplace_back (&ARTreeX<Record, Y>::producer, this);
    background_thread.emplace_back (&ARTreeX<Record, Y>::release, this);
    background_thread.emplace_back (&ARTreeX<Record, Y>::consumer, this);

    background_thread[0].detach ();
    background_thread[1].detach ();
    background_thread[2].detach ();
    background_thread[3].detach ();
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::traverse () {
    // traverse the artree every 2-3s and store the 4th level of the nodes to a vector
    auto policy_type = static_cast<UnloadPolicyType> (2);
    UnloadPolicy* policy = UnloadPolicyFactory (policy_type);
    while (bg_threads_keep_running.load ()) {
        // should set after sleeping 4 secs (inform producer thread to stop while running)
        std::unique_lock<std::mutex> queueMU (queueLock);
        if (!ready_to_refresh_publiclist.load ()) {
            queuelock_cv.wait (queueMU, [&] () {
                printf ("CHECK TRAVERSE LOCK bg %d ready fresh %d\n",
                        bg_threads_keep_running.load (), ready_to_refresh_publiclist.load ());
                return (!bg_threads_keep_running) || ready_to_refresh_publiclist.load ();
            });
            if (!bg_threads_keep_running) {
                printf ("stop traverse 1\n");
                STOP_COUNTER--;
                force_stop_producer_thread.store (true);
                force_stop_release_thread.store (true);
                delete policy;
                return;
            }
        }

        printf ("==========================================\n");
        printf ("      Start to traverse the subtrees \n");
        printf ("==========================================\n");
        for (uint64_t i = 0; i < publicListLen_; i++) {
            candidate_t* item = publicList[i];
            if (item->node == nullptr) {
                continue;
            }
            item->node->candidate = nullptr;
            item->node = nullptr;
        }

        auto starttime = std::chrono::system_clock::now ();
        // Get two signal before starting
        SelectVictimPolicy (policy, 250, publicList, publicListLen_);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds> (
            std::chrono::system_clock::now () - starttime);
        printf ("[Public list refresh time] %f s.\n", duration.count () / 1000000.0);

        // Randomly pick a subtree to check the size
        std::random_device rand_dev;
        std::mt19937 generator (rand_dev ());
        std::uniform_int_distribution<uint64_t> distr (0, publicListLen_ - 1);
        auto nPrefix = distr (generator);
        double subtreeSize = getPrefixSubtreeSize (publicList[nPrefix]->prefix);
        printf ("random picked subtree %lu size %2.10f, prev %2.10f\n", nPrefix, subtreeSize,
                prevMaxSubtreeSize);
        if ((prevMaxSubtreeSize > 0) && (subtreeSize > (prevMaxSubtreeSize * 4))) {
            tree_->urlevel_++;
        }
        if (subtreeSize > prevMaxSubtreeSize) {
            prevMaxSubtreeSize = subtreeSize;
        }

        force_stop_producer_thread.store (false);
        force_stop_release_thread.store (false);
        queueMU.unlock ();
        int sleeptime = 4;
        while (sleeptime--) {
            ready_to_refresh_publiclist.store (false);
            queuelock_cv.notify_one ();
            // ----------------------------------------------------------------------------------------
            uint64_t valid_item = 0, nbusys = 0;
            uint64_t counter00 = 0, counter10 = 0, counter01 = 0, counter11 = 0;
            // ----------------------------------------------------------------------------------------

            for (uint64_t i = 0; i < publicListLen_; i++) {
                auto item = publicList[i];

                bool needRestart = false;
                item->readLockOrRestart (needRestart);
                if (needRestart) nbusys++;
                if (!item->node) {
                    continue;
                }

                valid_item++;
                if ((!item->node->isClockWriteBit ()) && (!item->node->isClockUnloadBit ())) {
                    counter00++;
                } else if ((item->node->isClockWriteBit ()) && (!item->node->isClockUnloadBit ())) {
                    item->node->clearClockWriteBit ();
                    item->node->setClockUnloadBit ();
                    counter10++;
                } else if ((!item->node->isClockWriteBit ()) && (item->node->isClockUnloadBit ())) {
                    counter01++;
                } else if ((item->node->isClockWriteBit ()) && (item->node->isClockUnloadBit ())) {
                    item->node->clearClockWriteBit ();
                    item->node->setClockUnloadBit ();
                    counter11++;
                }
            }
            printf ("valid_items %lu publicListLen %lu invalid_items %lu nbusys %lu\n", valid_item,
                    publicListLen_, publicListLen_ - valid_item, nbusys);
            printf ("(W = 0, U = 0) %lu (W = 1, U = 0) %lu (W = 0, U = 1) %lu (W =1, U =1) %lu\n",
                    counter00, counter10, counter01, counter11);
            std::this_thread::sleep_for (1s);
        }
        force_stop_producer_thread.store (true);
        force_stop_release_thread.store (true);
    }
    force_stop_producer_thread.store (true);
    force_stop_release_thread.store (true);
    delete policy;
    STOP_COUNTER--;
    printf ("stop traverse\n");
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::producer () {
    // background unload thread "clean the dirty subtree"
    // node status (The write bit only set by write thread, the unload bit only set by
    // unload thread)
    // write-bit unload-bit
    //     0         0
    //     1         0
    //     0         1
    //     1         1
    // to verify the subtree is not too dirty or being intensively inserted.
    while (publicListLen_ <= 0) continue;
    while (bg_threads_keep_running.load ()) {
        std::unique_lock<std::mutex> queueMU (queueLock);

        if (ready_to_refresh_publiclist.load ()) {
            queuelock_cv.wait (queueMU, [&] () {
                printf ("CHECK PRODUCER LOCK bg %d ready fresh %d\n",
                        bg_threads_keep_running.load (), ready_to_refresh_publiclist.load ());
                return (!bg_threads_keep_running) || (!ready_to_refresh_publiclist.load ());
            });
            if (!bg_threads_keep_running) {
                printf ("stop producer 1\n");
                STOP_COUNTER--;
                return;
            }
        }
    RESTART_UNLOAD:
        uint64_t i = (ctn_idx_producer < publicListLen_) ? ctn_idx_producer : 0;
        uint64_t local_unloaded = 1;
        for (; i < publicListLen_; i++) {
            if (force_stop_producer_thread.load ()) {
                ctn_idx_producer = i;
                break;
            }
            candidate_t* item = publicList[i];
            bool needRestart = false;
            if (!item->node) continue;

            auto v = item->readLockOrRestart (needRestart);
            if (needRestart) {
                continue;
            }

            if ((!item->node->isClockWriteBit ()) && (item->node->isClockUnloadBit ())) {
                // #ifdef LEANSTORE
                //                 if (item->node->count < 255) {
                //                     continue;
                //                 }
                // #endif

                // If a node lying in this stat, it means there is no write request to this
                // subtree and we can clean this dirty tree
                // Start to unload the subtree (Need to lock the subtree)
                std::unique_lock<std::mutex> ul (pc_mu);

                // If more than 4 unprocessed items are in the data queue,
                // wait for sometime before adding more
                if (generalPCQuene.size () >= 1) {
                    not_full_cv_.wait (ul, [&] () {
                        return (!bg_threads_keep_running) || !(generalPCQuene.size () >= 1);
                    });
                    if (!bg_threads_keep_running) {
                        printf ("stop producer 1\n");
                        STOP_COUNTER--;
                        return;
                    }
                }
                item->writeLock (v, needRestart);
                if (needRestart) {
                    item->writeUnlock ();
                    continue;
                }
                generalPCQuene.enqueue (item);
                // Unlock the lock and notify the consumer that data is available
                ul.unlock ();
                not_empty_cv_.notify_one ();
                local_unloaded++;
            }
            if (local_unloaded > 0 && local_unloaded % 5 == 0) break;
        }
        if (i >= publicListLen_ - 1) {
            ctn_idx_producer = 0;
        } else {
            std::random_device rand_dev;
            std::mt19937 generator (rand_dev ());
            std::uniform_int_distribution<uint64_t> distr (0, publicListLen_ - 1);
            ctn_idx_producer = distr (generator);
        }

        // if (i >= publicListLen_ - 1) {
        //     ctn_idx_producer = 0;
        // } else {
        //     ctn_idx_producer = i;
        // }

        if (!force_stop_producer_thread.load ()) goto RESTART_UNLOAD;

        /* make sure
            1. the unload queue is empty
            2. the release thread is not traversing the public list
            3. the unload thread is not unloading the subtree
        */
        while (!generalPCQuene.isEmpty () && !nUnloadingThreadsWorking.load () &&
               !nReleasingThreadsWorking.load ()) {
            not_empty_cv_.notify_one ();
        }
        ready_to_refresh_publiclist.store (true);
        queueMU.unlock ();
        queuelock_cv.notify_one ();
    }
    STOP_COUNTER--;
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::release () {
    // background release thread (take over the hybrid node)
    // Condition
    // 1. choose the subtree which is not too much diry
    // 2. if dirty, clean it first (SKIP)
    // 3. lock because unload (SKIP)
    ExtraY* extraY = new ExtraY ();
    while (publicListLen_ <= 0) continue;

    while (bg_threads_keep_running.load ()) {
        if (publicListLen_ == 0) continue;

        if (!release_keep_running) continue;

        if (force_stop_release_thread.load ()) continue;

        uint64_t i = (ctn_idx_release < publicListLen_) ? ctn_idx_release : 0;
        uint64_t local_released = 1;
        for (; i < publicListLen_; i++) {
            if (force_stop_release_thread.load ()) {
                ctn_idx_release = i;
                break;
            }
            if (!release_keep_running) {
                ctn_idx_release = i;
                break;
            }
            candidate_t* item = publicList[i];
            auto needRestart = false;
            if (!item->node) continue;

            auto v = item->readLockOrRestart (needRestart);
            if (needRestart) {
                continue;
            }

            if ((!item->node->isClockWriteBit ()) && (!item->node->isClockUnloadBit ())) {
                // #ifdef LEANSTORE
                //                 if (item->node->count < 255) {
                //                     continue;
                //                 }
                // #endif
                // the subtree is clean W = 0 U = 0 (release the memory)
                item->writeLock (v, needRestart);
                if (needRestart) {
                    item->writeUnlock ();
                    continue;
                }
                nReleasingThreadsWorking.fetch_add (1);
                auto res =
                    releaseCleanPrefix (item->prefix, vMemoryDeallocated, releaseValue, extraY);
                item->writeUnlock ();
                nReleasingThreadsWorking.fetch_sub (1);
                // release_prefix_map[item->prefix]++;
                if (res) {
                    local_released++;
                    release_counter++;
                }
            }
            if (local_released > 0 && local_released % 5 == 0) break;
        }
        // if (i >= publicListLen_ - 1) {
        //     ctn_idx_release = 0;
        // } else {
        //     std::random_device rand_dev;
        //     std::mt19937 generator (rand_dev ());
        //     std::uniform_int_distribution<uint64_t> distr (0, publicListLen_ - 1);
        //     ctn_idx_release = distr (generator);
        // }

        if (i >= publicListLen_ - 1) {
            ctn_idx_release = 0;
        } else {
            ctn_idx_release = i + 1;
        }
    }
    STOP_COUNTER--;
    printf ("stop release\n");
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::load () {
    // background load thread
    while (bg_threads_keep_running) {
        // printf ("bg release threads keep running\n");

        std::this_thread::sleep_for (1s);
    }
    STOP_COUNTER--;
    printf ("stop load\n");
    return;
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::consumer () {
    // function of consumer (unloading the subtree with multi-thread manner)
    // TODO: The queue may empty, but the unload is processing
    std::this_thread::sleep_for (1s);
    ExtraY* extraY = new ExtraY ();
    while (bg_threads_keep_running.load ()) {
        std::unique_lock<std::mutex> ul (pc_mu);
        // If the generalPCQuene is empty,
        // wait for the producer to add something to it
        if (generalPCQuene.isEmpty ()) {
            // Predicate should return false to continue waiting.
            // Thus, if the queue is empty, predicate should return false
            // (!producerConsumerQueue.empty())
            not_empty_cv_.wait (
                ul, [&] () { return (!bg_threads_keep_running) || !generalPCQuene.isEmpty (); });
            if (!bg_threads_keep_running) {
                printf ("stop unload\n");
                STOP_COUNTER--;
                delete extraY;
                return;
            }
        }

        if (generalPCQuene.isEmpty ()) continue;
        candidate_t* item = generalPCQuene.peek ();  // Pick the element from the queue
        // If this statement is commented, the producer is blocked till this
        // loop ends
        if (!generalPCQuene.isEmpty ()) {
            generalPCQuene.dequeue ();
        } else {
            ul.unlock ();
            continue;
        }

        // Unlock the lock to unblock the producer.
        ul.unlock ();
        not_full_cv_.notify_one ();  // Tell the producer that they can go ahead
        // since 1 element is now popped off for processing
        nUnloadingThreadsWorking.fetch_add (1);
        if (!item->node) {
            item->writeUnlock ();
            nUnloadingThreadsWorking.fetch_sub (1);
            continue;
        }
        if (item->node->isClockWriteBit ()) {
            item->writeUnlock ();
            nUnloadingThreadsWorking.fetch_sub (1);
            continue;
        }

        // TODO: consider insert and unload go to the same subtree
        // or add a condition that only unload N256
        double unloadedMB = unloadDirtyPrefix (item->prefix, extraY);

        if (unloadedMB == 0) {
            item->writeUnlock ();
            nUnloadingThreadsWorking.fetch_sub (1);
            continue;
        }
        if (!item->node) {
            item->writeUnlock ();
            nUnloadingThreadsWorking.fetch_sub (1);
            continue;
        }
        // record the unloaded prefix
        // unload_prefix_map[item->prefix]++;
        item->node->clearClockUnloadBit ();  // TODO: node could be empty
        unload_counter++;
        item->writeUnlock ();
        nUnloadingThreadsWorking.fetch_sub (1);
    }
    delete extraY;
    STOP_COUNTER--;
    printf ("stop unload\n");
}

template <class Record, typename Y>
uint64_t ARTreeX<Record, Y>::structXMemoryInfo () {
    return tree_->memoryInfo->totalARTMemoryUsage ();
}

template <class Record, typename Y>
uint64_t ARTreeX<Record, Y>::structXVMemoryInfo () {
    return vMemoryAllocated - vMemoryDeallocated;
}

template <class Record, typename Y>
std::string ARTreeX<Record, Y>::ToStats () {
    return tree_->ToStats ();
}

template <class Record, typename Y>
void ARTreeX<Record, Y>::getPrefixMapCounter () {
    for (std::pair<std::string, uint64_t> pr : unload_prefix_map) {
        cout << "p " << pr.second << endl;
    }
    for (std::pair<std::string, uint64_t> pr : release_prefix_map) {
        cout << "r " << pr.second << endl;
    }
}

template <class Record>
unsigned LeanstoreY<Record>::fold (uint8_t* writer, const s32& x) {
    *reinterpret_cast<u32*> (writer) = __builtin_bswap32 (x ^ (1ul << 31));
    return sizeof (x);
}

template <class Record>
unsigned LeanstoreY<Record>::fold (uint8_t* writer, const s64& x) {
    *reinterpret_cast<u64*> (writer) = __builtin_bswap64 (x ^ (1ull << 63));
    return sizeof (x);
}

template <class Record>
unsigned LeanstoreY<Record>::fold (uint8_t* writer, const u64& x) {
    *reinterpret_cast<u64*> (writer) = __builtin_bswap64 (x);
    return sizeof (x);
}

template <class Record>
unsigned LeanstoreY<Record>::fold (uint8_t* writer, const u32& x) {
    *reinterpret_cast<u32*> (writer) = __builtin_bswap32 (x);
    return sizeof (x);
}

template <class Record>
void LeanstoreY<Record>::initializeDB (leanstore::LeanStore& db, std::string name,
                                       const getObjectFn& fn1, const getRidFn& fn2,
                                       const releaseValueFn& fn3) {
    // the callback function to allocate/deallocate the real value
    getObject = fn1;
    getRid = fn2;
    releaseValue = fn3;
    if (FLAGS_recover) {
        btree = &db.retrieveBTreeLL ("ycsb");
    } else {
        btree = &db.registerBTreeLL ("ycsb");
    }
    registerKVStoreLookupFunc ();
}

template <class Record>
bool LeanstoreY<Record>::insert (uint8_t* key, size_t keyLen, uint8_t* value, size_t valueLen) {
    btree->insert (key, keyLen, value, valueLen);
    return 1;
}

template <class Record>
bool LeanstoreY<Record>::lookup (uint8_t* key, size_t keyLen, uint8_t* v) {
    return btree->lookup (key, keyLen, [&] (const u8* payload, u16 payload_length) {
        memcpy (v, payload, payload_length);
    }) == leanstore::storage::btree::OP_RESULT::OK;
}

template <class Record>
void LeanstoreY<Record>::registerKVStoreLookupFunc () {
    kvStoreLookupFunc = LeanstoreY::KVStoreLookupFn;
}

template <class Record>
void RocksdbY<Record>::initializeDB (const getObjectFn& fn1, const getRidFn& fn2,
                                     const releaseValueFn& fn3) {
    // the callback function to allocate/deallocate the real value
    getObject = fn1;
    getRid = fn2;
    releaseValue = fn3;

    woptions.disableWAL = true;
    woptions.sync = false;

    // Enable the bloomfilter
    tableOptions.whole_key_filtering = true;
    tableOptions.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
    tableOptions.block_cache = rocksdb::NewLRUCache (1 * 1024 * 1024 * 1024LL);
    tableOptions.cache_index_and_filter_blocks = true;
    tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism (8);
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
    dbname_ = "artree_" + std::to_string ((uint64_t)this);
    printf ("rocksdb name %s\n", dbname_.c_str ());
    rocksdb::Status s = rocksdb::DB::Open (options, dbname_, &db);
    assert (s.ok ());
    if (columnFamilyNums_ != 0) {
        rocksdb::ColumnFamilyOptions cf_options;
        cf_options = options;
        handles.resize (columnFamilyNums_);
        for (auto i = 0; i < handles.size (); i++) {
            auto cfhandler = reinterpret_cast<rocksdb::ColumnFamilyHandle*> (handles[i]);
            auto c = db->CreateColumnFamily (cf_options, std::to_string (i), &(cfhandler));
            if (!c.ok ()) {
                std::cout << "Creating column family failed -> " << i << std::endl;
            } else {
                std::cout << "Creating column family successed -> " << i << std::endl;
            }
            assert (c.ok ());
        }
    }
}

template <class Record>
void RocksdbY<Record>::registerKVStoreLookupFunc () {
    kvStoreLookupFunc = RocksdbY<Record>::KVStoreLookupFn;
}

#endif