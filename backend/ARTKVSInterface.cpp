#include "ARTKVSInterface.hpp"

namespace ARTKVS {

// Utility function to return the size of the queue
int ProducerConsumerQueue::size () { return count; }
// Utility function to check if the queue is empty or not
bool ProducerConsumerQueue::isEmpty () { return (size () == 0); }
// Utility function to check if the queue is full or not
bool ProducerConsumerQueue::isFull () { return (size () == capacity); }
// Utility function to dequeue the front element
void ProducerConsumerQueue::dequeue () {
    // check for queue underflow
    if (isEmpty ()) {
        exit (EXIT_FAILURE);
    }
    front = (front + 1) % capacity;
    count--;
}
// Utility function to add an item to the queue
void ProducerConsumerQueue::enqueue (candidate_t* item) {
    // check for queue overflow
    if (isFull ()) {
        exit (EXIT_FAILURE);
    }
    rear = (rear + 1) % capacity;
    arr[rear] = item;
    count++;
}
// Utility function to return the front element of the queue
candidate_t* ProducerConsumerQueue::peek () {
    if (isEmpty ()) {
        exit (EXIT_FAILURE);
    }
    return arr[front];
}
void destroy () {}

// =====================================================================================

void RocksdbY::initializeDB (std::string name, const getObjectFn& fn1, const getRidFn& fn2,
                             const releaseValueFn& fn3) {
    // the callback function to allocate/deallocate the real value
    instanceExtra = new InstanceExtra (fn1, fn2, fn3);

    woptions.disableWAL = true;
    woptions.sync = false;

    // Enable the bloomfilter
    tableOptions.whole_key_filtering = true;
    tableOptions.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));
    tableOptions.block_cache = rocksdb::NewLRUCache (FLAGS_dram_gib * 1024 * 1024 * 1024ULL);
    // tableOptions.cache_index_and_filter_blocks = true;
    // tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;

    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism (4);
    options.OptimizeUniversalStyleCompaction();
    // options.OptimizeLevelStyleCompaction ();
    // ====================== FLATTEN the throughput ========================
    // Set up the rate limiter to allow a maximum of 1 MB/s write rate
    // The first parameter is the write rate limit in bytes per second
    // The second parameter is the refill period in microseconds
    // The third parameter is the fair queueing factor (optional)
    // options.rate_limiter.reset (rocksdb::NewGenericRateLimiter (200000000));

    options.use_direct_io_for_flush_and_compaction = true;
    options.use_direct_reads = true;
    options.create_if_missing = true;
    options.max_open_files = -1;
    // options.compression = rocksdb::kSnappyCompression;
    options.compression = rocksdb::kNoCompression;
    options.write_buffer_size = 256 * 1024 * 1024;
    // options.PrepareForBulkLoad ();
    // options.arena_block_size = 64 * 1024;
    // options.disable_auto_compactions = true;
    options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (tableOptions));

    // create the database
    dbname_ = "artree_" + std::to_string ((uint64_t)this);
    INFO ("Rocksdb name %s", FLAGS_dbname.c_str ());
    
    if (FLAGS_dbname == "tpcc") {
        rocksdb::Status s = rocksdb::DB::Open (options, dbname_, &db);
        assert (s.ok ());
    } else {
        rocksdb::Status s = rocksdb::DB::Open (options, FLAGS_dbname, &db);
        assert (s.ok ());
    }
    
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
bool RocksdbY::lookup (uint8_t* key, size_t keyLen, uint64_t& tid_kvs) {
    std::string val;
    auto s = db->Get (rocksdb::ReadOptions (), rocksdb::Slice ((char*)key, keyLen), &val);
    instanceExtra->getRid (tid_kvs, val.data ());
    return s.ok ();
}
bool RocksdbY::scan (uint8_t* key, size_t keyLen, uint64_t scan_length) {
    if (db == nullptr) {
        // Handle error
        INFO ("db does not exists\n");
    }

    // Iterate over each item in the database and print them
    std::unique_ptr<rocksdb::Iterator> it (db->NewIterator (rocksdb::ReadOptions ()));
    if (!it) {
        // Handle error
        INFO ("iterator does not exists\n");
    }
    uint64_t i = 0;
    it->Seek (rocksdb::Slice ((char*)key, keyLen));

    for (; i < scan_length && it->Valid (); i++) {
        it->Next ();
    }
    // INFO ("%lu\n", i);
    return 1;
}
// =====================================================================================

unsigned LeanstoreY::fold (uint8_t* writer, const s32& x) {
    *reinterpret_cast<u32*> (writer) = __builtin_bswap32 (x ^ (1ul << 31));
    return sizeof (x);
}

unsigned LeanstoreY::fold (uint8_t* writer, const s64& x) {
    *reinterpret_cast<u64*> (writer) = __builtin_bswap64 (x ^ (1ull << 63));
    return sizeof (x);
}

unsigned LeanstoreY::fold (uint8_t* writer, const u64& x) {
    *reinterpret_cast<u64*> (writer) = __builtin_bswap64 (x);
    return sizeof (x);
}

unsigned LeanstoreY::fold (uint8_t* writer, const u32& x) {
    *reinterpret_cast<u32*> (writer) = __builtin_bswap32 (x);
    return sizeof (x);
}

void LeanstoreY::initializeDB (leanstore::LeanStore& db, std::string name, const getObjectFn& fn1,
                               const getRidFn& fn2, const releaseValueFn& fn3) {
    // the callback function to allocate/deallocate the real value
    instanceExtra = new InstanceExtra (fn1, fn2, fn3);

    if (FLAGS_recover) {
        btree = &db.retrieveBTreeLL ("ycsb");
    } else {
        btree = &db.registerBTreeLL (name);
    }
}

bool LeanstoreY::insert (uint8_t* key, size_t keyLen, uint8_t* value, size_t valueLen) {
    btree->insert (key, keyLen, value, valueLen);
    return 1;
}

bool LeanstoreY::lookup (uint8_t* key, size_t keyLen, uint64_t& tid_kvs) {
    return btree->lookup (key, keyLen, [&] (const u8* payload, u16 payload_length) {
        instanceExtra->getRid (tid_kvs, (char*)payload);
    }) == leanstore::storage::btree::OP_RESULT::OK;
}

bool LeanstoreY::scan (uint64_t rid, uint64_t scan_length) {
    u8 key_bytes[sizeof (Key)];
    uint64_t i = 0;
    btree->scanAsc (
        key_bytes, fold (key_bytes, rid),
        [&] (const u8* key, u16 key_length, const u8* payload, u16 payload_length) {
            if (i < scan_length) {
                i++;
                return true;
            }

            return false;
        },
        [&] {});
    return true;
}

// =====================================================================================

ARTreeKVS::ARTreeKVS (leanstore::LeanStore& leanstore_db, getObjectFn fn1, getRidFn fn2,
                      releaseValueFn fn3, std::string name) {
    leanstore_y_ = new LeanstoreY ();
    leanstore_y_->initializeDB (leanstore_db, name, fn1, fn2, fn3);
}
ARTreeKVS::ARTreeKVS (rocksdb::DB* rocksdb_db, getObjectFn fn1, getRidFn fn2, releaseValueFn fn3,
                      std::string name) {
    rocksdb_y_ = new RocksdbY ();
    rocksdb_y_->initializeDB (name, fn1, fn2, fn3);
}
ARTreeKVS::ARTreeKVS (leanstore::LeanStore& leanstore_db, rocksdb::DB* rocksdb_db, getObjectFn fn1,
                      getRidFn fn2, releaseValueFn fn3, std::string name) {
    leanstore_y_ = new LeanstoreY ();
    leanstore_y_->initializeDB (leanstore_db, name, fn1, fn2, fn3);
    rocksdb_y_ = new RocksdbY ();
    rocksdb_y_->initializeDB (name, fn1, fn2, fn3);
}

void ARTreeKVS::CreateARTreeKVS (loadKeyCallbackFn loadkey, getObjectFn fn1, getRidFn fn2,
                                 releaseValueFn fn3, std::string idx_name) {
    tree_ = new Tree (loadkey);

    getObject = fn1;
    getRid = fn2;
    releaseValue = fn3;
    idx_name_ = idx_name;
    tree_->urlevel_ = FLAGS_start_level;  // for tpcc 3 for ycsb 2
    initLength = FLAGS_public_list_len;

    // Initilize the Public List
    auto thread_id = std::hash<std::thread::id> () (std::this_thread::get_id ());
    if (idx_name_ == "orderline" || idx_name == "ycsb") {
        publicList.resize (initLength);
        for (uint64_t i = 0; i < initLength; i++) {
            publicList[i] = new candidate_t{1, nullptr, ""};
            // tree_->memoryInfo->increaseByBytes (thread_id, malloc_usable_size (publicList[i]));
        }
    }

    publicListLen_ = 0;
}

bool ARTreeKVS::Insert (const IndexKey& key, TID rid) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    bool ret;
#ifdef WITH_LEANSTORE
    ret = tree_->insert (key, rid, thread, nullptr, nullptr, LeanstoreY::KVStoreLookupFn,
                              leanstore_y_, &seeHybrid, false);
#endif
#ifdef WITH_ROCKSDB
    ret = tree_->insert (key, rid, thread, nullptr, nullptr, RocksdbY::KVStoreLookupFn,
                              rocksdb_y_, &seeHybrid, false);
#endif
    return ret;
}

TID ARTreeKVS::Lookup (IndexKey& key, uint64_t& disk_access) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    auto tid = tree_->lookup (key, thread, nullptr, nullptr, &seeHybrid);
    // seeHybrid = TRUE if the lookup meets hybrid bit
    if (tid) return tid;  // it finds target in memory
    diskread_++;
    disk_access++;
    // search from the key value store
    // getRid(); // transfer from the string
    uint64_t tid_kvs;
    bool res;
#ifdef WITH_LEANSTORE
    res = leanstore_y_->lookup (key.getData (), key.getKeyLen (), tid_kvs);
    if (!res) return 0;
    while (1) {
        if (ARTcache) {
            bool ret = tree_->insert (key, tid_kvs, thread, nullptr, nullptr,
                                      LeanstoreY::KVStoreLookupFn, leanstore_y_, &seeHybrid, true);
            break;
        }
    }
#endif
#ifdef WITH_ROCKSDB
    res = rocksdb_y_->lookup (key.getData (), key.getKeyLen (), tid_kvs);
    if (!res) return 0;
    while (1) {
        if (ARTcache) {
            bool ret = tree_->insert (key, tid_kvs, thread, nullptr, nullptr,
                                      RocksdbY::KVStoreLookupFn, rocksdb_y_, &seeHybrid, true);
            break;
        }
    }
#endif
    return tid_kvs;
}

TID ARTreeKVS::Lookup2 (IndexKey& key, TID rid, uint64_t& disk_access) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    auto tid = tree_->lookup (key, thread, nullptr, nullptr, &seeHybrid);
    // seeHybrid = TRUE if the lookup meets hybrid bit
    if (tid) return tid;  // it finds target in memory
    diskread_++;
    disk_access++;
#ifdef WITH_LEANSTORE
    while (1) {
        if (ARTcache) {
            bool ret = tree_->insert (key, rid, thread, nullptr, nullptr,
                                      LeanstoreY::KVStoreLookupFn, leanstore_y_, &seeHybrid, true);
            break;
        }
    }
#endif
#ifdef WITH_ROCKSDB
    while (1) {
        if (ARTcache) {
            bool ret = tree_->insert (key, rid, thread, nullptr, nullptr, RocksdbY::KVStoreLookupFn,
                                      rocksdb_y_, &seeHybrid, true);
            break;
        }
    }
#endif
    return rid;
}

TID ARTreeKVS::LookupRange (const IndexKey& keyleft, const IndexKey& keyright) {
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

bool ARTreeKVS::LookupIterator (const IndexKey& target, TID rid) {
    auto thread = tree_->getThreadInfo ();
    uint64_t scan_length = 1 + leanstore::utils::RandomGenerator::getRand (0, 100);
    auto iter = tree_->NewIterator (nullptr);
    iter->Seek (target);
    for (uint64_t i = 0; i < scan_length && iter->Valid (); i++) {
        iter->Next ();
    }
    bool res;
#ifdef WITH_LEANSTORE
    res = leanstore_y_->scan (rid, scan_length);

#endif
#ifdef WITH_ROCKSDB
    res = rocksdb_y_->scan (target.getData (), target.getKeyLen (), scan_length);
#endif
    return 1;
}

bool ARTreeKVS::Delete (const IndexKey& key, TID rid) {
    // TODO: not complete (delete from the store)
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    bool res;
#ifdef WITH_LEANSTORE
    res = tree_->remove (key, rid, thread, nullptr, LeanstoreY::KVStoreLookupFn, leanstore_y_,
                              &seeHybrid);
#endif
#ifdef WITH_ROCKSDB
    res = tree_->remove (key, rid, thread, nullptr, RocksdbY::KVStoreLookupFn, rocksdb_y_,
                              &seeHybrid);
#endif
    if (!seeHybrid) {
        return res;
    }
    return 1;
}

void ARTreeKVS::SelectVictimPolicy (UnloadPolicy* policy, size_t dumpSize,
                                    std::vector<candidate_t*>& publicList,
                                    uint64_t& publicListLen_) {
    auto thread = tree_->getThreadInfo ();
    tree_->calculateVictims (policy, dumpSize, thread, publicList, publicListLen_);
}

double ARTreeKVS::unloadDirtyPrefix (const IndexKey key, ThreadLocalExtra* threadLocalExtra) {
    // The number of keys in a subtree is used as a unit (batch); this can vary. 
    auto threadInfo = tree_->getThreadInfo ();
    std::pair<bool, double> res;
#ifdef WITH_LEANSTORE
    res = tree_->unloadDirtyPrefix (key, threadInfo, leanstore_y_,
                                                            LeanstoreY::UnloadFn, threadLocalExtra);
#endif
#ifdef WITH_ROCKSDB
    res = tree_->unloadDirtyPrefix (key, threadInfo, rocksdb_y_,
                                                            RocksdbY::UnloadFn, threadLocalExtra);
    rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (threadLocalExtra->batch_);
    rocksdb::WriteOptions writeDisableWAL;
    writeDisableWAL.disableWAL = true;
    uint64_t keysInBatch = wb->Count ();
    rocksdb_y_->db->Write (writeDisableWAL, wb);
    wb->Clear ();
#endif
    return res.second;
}

double ARTreeKVS::getPrefixSubtreeSize (IndexKey key) {
    auto size = tree_->getPrefixSubtreeSize (key);
    return size;
}

bool ARTreeKVS::releaseCleanPrefix (
    const IndexKey& key, std::atomic<uint64_t>& vDeallocated,
    std::function<void (uint64_t, std::atomic<uint64_t>&)>& releaseMemory,
    ThreadLocalExtra* threadLocalExtra) {
    auto threadInfo = tree_->getThreadInfo ();
    bool res;
#ifdef WITH_LEANSTORE
    res = tree_->releaseCleanPrefix (key, threadInfo, vDeallocated, releaseMemory,
                                          leanstore_y_, LeanstoreY::UnloadFn, threadLocalExtra);
#endif
#ifdef WITH_ROCKSDB
    res = tree_->releaseCleanPrefix (key, threadInfo, vDeallocated, releaseMemory, rocksdb_y_,
                                          RocksdbY::UnloadFn, threadLocalExtra);
    rocksdb::WriteBatch* wb = reinterpret_cast<rocksdb::WriteBatch*> (threadLocalExtra->batch_);
    rocksdb::WriteOptions writeDisableWAL;
    writeDisableWAL.disableWAL = true;
    uint64_t keysInBatch = wb->Count ();
    rocksdb_y_->db->Write (writeDisableWAL, wb);
    wb->Clear ();
#endif
    return res;
}

void ARTreeKVS::stopBackgroundThreads () {
    bg_threads_keep_running.store (false);
    while (!generalPCQuene.isEmpty ()) {
        generalPCQuene.dequeue ();
    }
    while (STOP_COUNTER != 0) {
        not_full_cv_.notify_all ();
        not_empty_cv_.notify_all ();
    }
}

void ARTreeKVS::startBackgroundThreads () {
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
    INFO ("[Start the Background Threads]");
    bg_threads_keep_running.store (true);
    release_keep_running = false;

    STOP_COUNTER = 4;

    background_thread.emplace_back (&ARTreeKVS::traverse, this);
    background_thread.emplace_back (&ARTreeKVS::producer, this);
    background_thread.emplace_back (&ARTreeKVS::release, this);
    background_thread.emplace_back (&ARTreeKVS::consumer, this);

    background_thread[0].detach ();
    background_thread[1].detach ();
    background_thread[2].detach ();
    background_thread[3].detach ();
}

void ARTreeKVS::traverse () {
    // traverse the artree every 2-3s and store the 4th level of the nodes to a vector
    auto policy_type = static_cast<UnloadPolicyType> (2);
    UnloadPolicy* policy = UnloadPolicyFactory (policy_type);
    while (bg_threads_keep_running.load ()) {
        // should set after sleeping 4 secs (inform producer thread to stop while running)
        std::unique_lock<std::mutex> queueMU (queueLock);
        if (!ready_to_refresh_publiclist.load ()) {
            queuelock_cv.wait (queueMU, [&] () {
                INFO ("CHECK TRAVERSE LOCK bg %d ready fresh %d", bg_threads_keep_running.load (),
                      ready_to_refresh_publiclist.load ());
                return (!bg_threads_keep_running) || ready_to_refresh_publiclist.load ();
            });
            if (!bg_threads_keep_running) {
                INFO ("STOP TRAVERSE 1");
                STOP_COUNTER--;
                force_stop_producer_thread.store (true);
                force_stop_release_thread.store (true);
                delete policy;
                return;
            }
        }

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
        // printf ("[Public list refresh time] %f s.\n", duration.count () / 1000000.0);

        // Randomly pick a subtree to check the size
        std::random_device rand_dev;
        std::mt19937 generator (rand_dev ());
        std::uniform_int_distribution<uint64_t> distr (0, publicListLen_ - 1);
        auto nPrefix = distr (generator);
        double subtreeSize = getPrefixSubtreeSize (publicList[nPrefix]->prefix);
        INFO ("random picked subtree %lu size %2.10f, prev %2.10f", nPrefix, subtreeSize,
              prevMaxSubtreeSize);
        // if ((prevMaxSubtreeSize > 0) && (subtreeSize > (prevMaxSubtreeSize * 4))) {
        //     tree_->urlevel_++;
        // }
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
                if ((!item->node->isClockAccessBit ()) && (!item->node->isClockUnloadBit ())) {
                    counter00++;
                } else if ((item->node->isClockAccessBit ()) &&
                           (!item->node->isClockUnloadBit ())) {
                    item->node->clearClockAccessBit ();
                    item->node->setClockUnloadBit ();
                    counter10++;
                } else if ((!item->node->isClockAccessBit ()) &&
                           (item->node->isClockUnloadBit ())) {
                    counter01++;
                } else if ((item->node->isClockAccessBit ()) && (item->node->isClockUnloadBit ())) {
                    item->node->clearClockAccessBit ();
                    item->node->setClockUnloadBit ();
                    counter11++;
                }
            }
            INFO ("valid_items %lu publicListLen %lu invalid_items %lu nbusys %lu ARTcache %d",
                  valid_item, publicListLen_, publicListLen_ - valid_item, nbusys, ARTcache);
            INFO ("(W = 0, U = 0) %lu (W = 1, U = 0) %lu (W = 0, U = 1) %lu (W =1, U =1) %lu",
                  counter00, counter10, counter01, counter11);
            std::this_thread::sleep_for (1s);
        }
        INFO ("============================== EPOCH ENDS ==============================");
        force_stop_producer_thread.store (true);
        force_stop_release_thread.store (true);
    }
    force_stop_producer_thread.store (true);
    force_stop_release_thread.store (true);
    delete policy;
    STOP_COUNTER--;
    INFO ("STOP TRAVERSE");
}

void ARTreeKVS::producer () {
    // background unload thread "clean the dirty subtree"
    // node status (The write bit only set by write thread, the unload bit only set by
    // unload thread)
    // write-bit unload-bit
    //     0         0
    //     1         0
    //     0         1
    //     1         1
    // to verify the subtree is not too dirty or being intensively inserted.
    while (bg_threads_keep_running.load ()) {
        std::unique_lock<std::mutex> queueMU (queueLock);
        if (ready_to_refresh_publiclist.load ()) {
            queuelock_cv.wait (queueMU, [&] () {
                INFO ("CHECK PRODUCER LOCK bg %d ready fresh %d", bg_threads_keep_running.load (),
                      ready_to_refresh_publiclist.load ());
                return (!bg_threads_keep_running) || (!ready_to_refresh_publiclist.load ());
            });
            if (!bg_threads_keep_running) {
                INFO ("STOP PRODUCER");
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

            if ((!item->node->isClockAccessBit ()) && (item->node->isClockUnloadBit ())) {
                // #ifdef WITH_LEANSTORE
                // INFO ("item->node->count %lu\n", item->node->count);
                if (item->node->count < 16) {
                    continue;
                }
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
                        INFO ("STOP PRODUCER 1");
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

void ARTreeKVS::release () {
    // background release thread (take over the hybrid node)
    // Condition
    // 1. choose the subtree which is not too much diry
    // 2. if dirty, clean it first (SKIP)
    // 3. lock because unload (SKIP)
    ThreadLocalExtra* extraY = new ThreadLocalExtra ();

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

            if ((!item->node->isClockAccessBit ()) && (!item->node->isClockUnloadBit ())) {
                // #ifdef WITH_LEANSTORE
                if (item->node->count < 16) {
                    continue;
                }
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
            if (local_released > 0 && local_released % 10 == 0) break;
        }
        if (i >= publicListLen_ - 1) {
            ctn_idx_release = 0;
        } else {
            std::random_device rand_dev;
            std::mt19937 generator (rand_dev ());
            std::uniform_int_distribution<uint64_t> distr (0, publicListLen_ - 1);
            ctn_idx_release = distr (generator);
        }

        // if (i >= publicListLen_ - 1) {
        //     ctn_idx_release = 0;
        // } else {
        //     ctn_idx_release = i + 1;
        // }
    }
    STOP_COUNTER--;
    INFO ("STOP RELEASE");
}

void ARTreeKVS::load () {
    // background load thread
    while (bg_threads_keep_running) {
        // printf ("bg release threads keep running\n");

        std::this_thread::sleep_for (1s);
    }
    STOP_COUNTER--;
    INFO ("STOP LOAD");
    return;
}

void ARTreeKVS::consumer () {
    // function of consumer (unloading the subtree with multi-thread manner)
    std::this_thread::sleep_for (1s);
    ThreadLocalExtra* extraY = new ThreadLocalExtra ();
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
                INFO ("STOP UNLOAD");
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
        if (item->node->isClockAccessBit ()) {
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
    INFO ("STOP UNLOAD");
}

uint64_t ARTreeKVS::idxMemoryUsageInfo () { return tree_->memoryInfo->totalARTMemoryUsage (); }

uint64_t ARTreeKVS::vMemoryInfo () { return vMemoryAllocated.load () - vMemoryDeallocated.load (); }

std::string ARTreeKVS::ToStats () { return tree_->ToStats (); }

void ARTreeKVS::getPrefixMapCounter () {
    for (std::pair<std::string, uint64_t> pr : unload_prefix_map) {
        cout << "p " << pr.second << endl;
    }
    for (std::pair<std::string, uint64_t> pr : release_prefix_map) {
        cout << "r " << pr.second << endl;
    }
}

void ARTreeKVS::ReleaseX () {
    tree_->ReleaseTree ([this] (TID ridlist) { releaseValue (ridlist, vMemoryDeallocated); });
}
};  // namespace ARTKVS
