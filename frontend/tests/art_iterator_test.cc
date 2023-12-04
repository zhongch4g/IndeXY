#include <chrono>
#include <iostream>

#include "artreeX/Tree.h"
#include "gtest/gtest.h"
#include "logger.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"

using namespace std;

rocksdb::DB* db_;
rocksdb::Options options_;
rocksdb::WriteOptions write_options_;
rocksdb::ReadOptions read_options_;

static bool OffLoadFn (void* store, uint8_t* key, size_t keyLen, TID tid, void* extra) {
    std::string tidlist_str = std::to_string (tid);
    rocksdb::DB* db = reinterpret_cast<rocksdb::DB*> (store);
    auto s = db->Put (rocksdb::WriteOptions (), rocksdb::Slice ((char*)key, keyLen), tidlist_str);
    return s.ok ();
}

std::vector<std::string> kStrKeys = {"abas",  "abat",  "al", "apa", "appa",   "appl",
                                     "bushe", "bushy", "bw", "cab", "calcit", "calciu"};

std::vector<std::pair<std::string, TID>> sstore;

bool loadKey (TID tid, Key& key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    key.setKeyLen (sizeof (tid));
    reinterpret_cast<uint64_t*> (&key[0])[0] = __builtin_bswap64 (tid);
    return true;
}

bool loadStrKey (TID tid, Key& key) {
    tid--;
    key.set (kStrKeys[tid].data (), kStrKeys[tid].size ());
    // printf ("load str key: %s\n", key.getData ());
    return true;
}

bool dbOffloadInteger (void* store, uint8_t* key, size_t keyLen, TID tid, void* extra) {
    static int i = 1;
    // printf ("offload %4d tid: %lu\n", i++, tid);
    return true;
}

void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {}

bool dbOffloadStr (void* store, uint8_t* key, size_t keyLen, TID tid, void* extra) {
    static int i = 1;
    // printf ("offload str key %s, tid: %lu keylen: %lu\n", (char*)key, tid, keyLen);
    sstore.push_back ({std::string ((char*)key, keyLen), tid});
    return true;
}

bool dbBatchOffloadStr (void* store, uint8_t* key, size_t keyLen, TID tid, void* extra) {
    static int i = 1;
    // printf ("offload str key %s, tid: %lu keylen: %lu\n", (char*)key, tid, keyLen);
    sstore.push_back ({std::string ((char*)key, keyLen), tid});
    return true;
}

class ARTTEST : public testing::Test {
public:
    ARTTEST () { tree_ = new ART_OLC_X::Tree (loadKey); }
    ~ARTTEST () {
        tree_->ReleaseTree ([] (TID ridlist) {});
        delete tree_;
    }

    // load key [256, n + 256)
    // tid      [256, n + 256)
    void LoadDense (size_t n) {
        keys_.resize (n);
        for (size_t i = 0; i < n; i++) keys_[i] = i + 256;

        auto t = tree_->getThreadInfo ();
        for (uint64_t i = 0; i < n; i++) {
            Key key;
            loadKey (keys_[i], key);
            tree_->insert (key, keys_[i], t);
        }
    }

    ART_OLC_X::Tree* tree_;
    vector<size_t> keys_;
};

class ARTTEST_STR : public testing::Test {
public:
    ARTTEST_STR () { tree_ = new ART_OLC_X::Tree (loadStrKey); }
    ~ARTTEST_STR () {
        tree_->ReleaseTree ([] (TID ridlist) {});
        delete tree_;
    }

    void Load () {
        auto t = tree_->getThreadInfo ();
        for (uint64_t i = 0; i < kStrKeys.size (); ++i) {
            Key key;
            key.set (kStrKeys[i].data (), kStrKeys[i].size ());
            tree_->insert (key, i + 1, t);
        }
        // printf ("%s\n", tree_->ToStats ().c_str ());
    }

    ART_OLC_X::Tree* tree_;
};

TEST_F (ARTTEST, SeekToFirstEmpty) {
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Next ());
    ASSERT_FALSE (iter->Valid ());
    iter->SeekToFirst ();
    ASSERT_FALSE (iter->Next ());
    ASSERT_FALSE (iter->Valid ());

    delete iter;
}

TEST_F (ARTTEST, LoadAndSeekFirst) {
    LoadDense (1024);
    auto* iter = tree_->NewIterator (nullptr);

    iter->SeekToFirst ();
    for (size_t i = 256; i < 1024 + 256; i++) {
        ASSERT_EQ (iter->GetTID (), i);
        ART::Key key;
        bool res = iter->GetKey (key);
        size_t ikey;
        memcpy (&ikey, key.getData (), 8);
        ikey = __builtin_bswap64 (ikey);
        ASSERT_EQ (ikey, i);
        DEBUG ("key (%u): %lu, tid: %lu", key.getKeyLen (), ikey, iter->GetTID ());
        iter->Next ();
    }

    ASSERT_FALSE (iter->Valid ());

    iter->SeekToFirst ();
    for (size_t i = 256; i < 1024 + 256; i++) {
        ASSERT_TRUE (iter->GetTID () == i);
        ART::Key key;
        bool res = iter->GetKey (key);
        size_t ikey;
        memcpy (&ikey, key.getData (), 8);
        ikey = __builtin_bswap64 (ikey);
        ASSERT_TRUE (ikey == i);
        // DEBUG ("key (%u): %lu, tid: %lu", key.getKeyLen (), ikey, iter->GetTID ());
        iter->Next ();
    }
    ASSERT_FALSE (iter->Valid ());
    ASSERT_FALSE (iter->Next ());
    ASSERT_FALSE (iter->Next ());

    Key key;
    loadKey (256, key);
    auto t = tree_->getThreadInfo ();
    auto res = tree_->lookup (key, t);
    ASSERT_EQ (256, res);

    delete iter;
}

TEST_F (ARTTEST, SeekEmpty) {
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Valid ());

    // Seek for empty string in empty tree
    Key key;
    Key gkey;
    key.set ("", 0);
    iter->Seek (key);
    ASSERT_FALSE (iter->Valid ());

    delete iter;
}

TEST_F (ARTTEST, LoadAndSeek) {
    LoadDense (1024);
    auto* iter = tree_->NewIterator (nullptr);

    // Seek for empty string, should return the first key in tree_
    Key key;
    Key gkey;
    key.set ("", 0);
    iter->Seek (key);
    ASSERT_TRUE (iter->Valid ());
    ASSERT_TRUE (iter->GetKey (gkey));
    loadKey (256, key);
    ASSERT_EQ (key, gkey);

    auto readFunc = [&] (size_t seek_num) {
        loadKey (seek_num, key);
        iter->Seek (key);
        ASSERT_TRUE (iter->Valid ());
        for (size_t i = seek_num; i < 1024 + 256; i++) {
            ASSERT_TRUE (iter->Valid ());
            bool res = iter->GetKey (gkey);
            loadKey (i, key);
            ASSERT_EQ (key, gkey);
            ASSERT_EQ (iter->GetTID (), i);
            iter->Next ();
        }
        ASSERT_FALSE (iter->Valid ());
    };

    readFunc (266);
    readFunc (333);
    readFunc (555);
    readFunc (777);

    delete iter;
}

TEST_F (ARTTEST, LoadAndSeeEnd) {
    LoadDense (1024);
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Valid ());

    Key key;
    loadKey (666, key);
    iter->Seek (key);
    ASSERT_TRUE (iter->Valid ());
    ASSERT_EQ (666, iter->GetTID ());

    loadKey (6969, key);
    iter->Seek (key);
    ASSERT_FALSE (iter->Valid ());

    delete iter;
}

TEST_F (ARTTEST, OffloadInteger) {
    LoadDense (1024);
    printf ("%s\n", tree_->ToStats ().c_str ());
    auto* iter = tree_->NewIterator (nullptr);

    auto epoch = tree_->getThreadInfo ();
    tree_->offLoadRandom (epoch, nullptr, dbOffloadInteger);

    iter->SeekToFirst ();
    int remain = 0;
    while (iter->Valid ()) {
        remain++;
        iter->Next ();
    }
    printf ("%s\n", tree_->ToStats ().c_str ());
    ASSERT_EQ (1024 - 255, remain);

    delete iter;
}

TEST_F (ARTTEST_STR, SeekToFirstEmpty) {
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Next ());
    ASSERT_FALSE (iter->Valid ());
    iter->SeekToFirst ();
    ASSERT_FALSE (iter->Next ());
    ASSERT_FALSE (iter->Valid ());

    delete iter;
}

TEST_F (ARTTEST_STR, LoadAndSeekFirst) {
    Load ();
    auto* iter = tree_->NewIterator (nullptr);

    iter->SeekToFirst ();
    for (size_t i = 0; i < kStrKeys.size (); i++) {
        ASSERT_EQ (iter->GetTID (), i + 1);
        ART::Key key;
        bool res = iter->GetKey (key);
        ASSERT_TRUE (res);
        int ret = memcmp (key.getData (), kStrKeys[i].data (), kStrKeys[i].size ());
        ASSERT_TRUE (ret == 0);
        INFO ("key (%u): %s, tid: %lu", key.getKeyLen (),
              std::string ((char*)key.getData (), key.getKeyLen ()).c_str (), iter->GetTID ());
        iter->Next ();
    }
    delete iter;
}

TEST_F (ARTTEST_STR, SeekEmpty) {
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Valid ());

    // Seek for empty string in empty tree
    Key key;
    Key gkey;
    key.set ("", 0);
    iter->Seek (key);
    ASSERT_FALSE (iter->Valid ());
    delete iter;
}

TEST_F (ARTTEST_STR, LoadAndSeek) {
    Load ();
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Valid ());

    // Seek for empty string, should return the first key in tree_
    Key key;
    Key gkey;
    key.set ("", 0);
    iter->Seek (key);
    ASSERT_TRUE (iter->Valid ());
    ASSERT_TRUE (iter->GetKey (gkey));
    key.set (kStrKeys[0].data (), kStrKeys[0].size ());
    ASSERT_EQ (key, gkey);

    key.set ("abatxyz", 4);
    iter->Seek (key);
    iter->GetKey (gkey);
    key.set ("abat", 4);
    ASSERT_EQ (gkey, key);

    key.set ("abatxyz", 7);
    iter->Seek (key);
    iter->GetKey (gkey);
    key.set ("al", 2);
    printf ("Seek key: %s, get key: %s\n", "abatxyz",
            std::string ((char*)gkey.getData (), gkey.getKeyLen ()).c_str ());
    ASSERT_EQ (gkey, key);

    auto readFunc = [&] (size_t starti) {
        key.set (kStrKeys[starti].data (), kStrKeys[starti].size ());
        iter->Seek (key);
        ASSERT_TRUE (iter->Valid ());
        for (size_t i = starti; i < kStrKeys.size (); ++i) {
            ASSERT_TRUE (iter->Valid ());
            bool res = iter->GetKey (gkey);
            key.set (kStrKeys[i].data (), kStrKeys[i].size ());
            ASSERT_EQ (key, gkey);
            ASSERT_EQ (iter->GetTID (), i + 1);
            iter->Next ();
        }
        ASSERT_FALSE (iter->Valid ());
    };

    readFunc (1);
    readFunc (3);
    readFunc (5);

    delete iter;
}

TEST_F (ARTTEST_STR, LoadAndSeekEnd) {
    Load ();
    auto* iter = tree_->NewIterator (nullptr);
    ASSERT_FALSE (iter->Valid ());

    Key key;
    key.set ("zzz", 3);
    iter->Seek (key);
    ASSERT_FALSE (iter->Valid ());

    delete iter;
}

TEST_F (ARTTEST_STR, OffLoadPrefix1) {
    Load ();
    Key prefix_k;
    prefix_k.set ("ap", 2);
    auto thread_info = tree_->getThreadInfo ();
    tree_->offLoadPrefix (prefix_k, thread_info, nullptr, dbOffloadStr);

    Key kstart, kend, ckey;
    kstart.set ("a", 1);
    kend.set ("z", 1);

    size_t found = 0;
    bool seehybrid = false;
    uint64_t mtagets[100];
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);

    ASSERT_EQ (found, 9);

    ASSERT_TRUE (tree_->setMigrationState (prefix_k, thread_info));
    bool seeHybrid = false;
    for (auto& [k, tid] : sstore) {
        Key akey;
        akey.set (k.data (), k.size ());
        ASSERT_TRUE (tree_->insert (akey, tid, thread_info, nullptr, nullptr, nullptr, nullptr,
                                    &seeHybrid, true));
    }
    ASSERT_TRUE (tree_->clearMigrationState (prefix_k, thread_info));
    ASSERT_FALSE (tree_->clearMigrationState (prefix_k, thread_info));

    found = 0;
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);
    ASSERT_EQ (12, found);
}

TEST_F (ARTTEST_STR, OffLoadPrefix2) {
    printf ("=========================\n");
    sstore.clear ();
    Load ();
    Key prefix_k;
    prefix_k.set ("bw", 2);
    auto thread_info = tree_->getThreadInfo ();
    tree_->offLoadPrefix (prefix_k, thread_info, nullptr, dbOffloadStr);

    Key kstart, kend, ckey;
    kstart.set ("a", 1);
    kend.set ("z", 1);

    size_t found = 0;
    bool seehybrid = false;
    uint64_t mtagets[100] = {0};
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);

    ASSERT_EQ (found, 11);

    ASSERT_TRUE (tree_->setMigrationState (prefix_k, thread_info));
    bool seeHybrid = false;
    for (auto& [k, tid] : sstore) {
        Key akey;
        akey.set (k.data (), k.size ());
        ASSERT_TRUE (tree_->insert (akey, tid, thread_info, nullptr, nullptr, nullptr, nullptr,
                                    &seeHybrid, true));
    }
    ASSERT_TRUE (tree_->clearMigrationState (prefix_k, thread_info));
    ASSERT_FALSE (tree_->clearMigrationState (prefix_k, thread_info));

    found = 0;
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);
    ASSERT_EQ (12, found);

    printf ("%s\n", tree_->ToStats ().c_str ());
}

TEST_F (ARTTEST_STR, OffLoadPrefix3) {
    options_.create_if_missing = true;
    rocksdb::Status s =
        rocksdb::DB::Open (options_, "artree_" + std::to_string ((uint64_t)this), &db_);
    assert (s.ok ());

    sstore.clear ();
    Load ();
    Key prefix_k;
    prefix_k.set ("ap", 2);
    auto thread_info = tree_->getThreadInfo ();
    tree_->offLoadPrefix (prefix_k, thread_info, db_, OffLoadFn);

    Key kstart, kend, ckey;
    kstart.set ("a", 1);
    kend.set ("z", 1);

    size_t found = 0;
    bool seehybrid = false;
    uint64_t mtagets[100];
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);
    ASSERT_EQ (found, 9);

    rocksdb::Iterator* iterator = db_->NewIterator (rocksdb::ReadOptions ());
    iterator->SeekToFirst ();
    for (; iterator->Valid (); iterator->Next ()) {
        auto slice = rocksdb::Slice (iterator->key ().data (), iterator->key ().size ());
        printf ("key = %s, value = %s \n", slice.ToString ().c_str (),
                iterator->value ().ToString ().c_str ());
    }
}

TEST_F (ARTTEST_STR, hybridBits) {
    auto thread_info = tree_->getThreadInfo ();
    // publiclist
    uint64_t lengthOfpublicList = 0;
    std::vector<ART_OLC_X::candidate_t*> publicList;
    for (uint64_t i = 0; i < 100; i++) {
        publicList[i] = new ART_OLC_X::candidate_t{1, nullptr, ""};
    }
    // Policy
    auto policy_type = static_cast<ART_OLC_X::UnloadPolicyType> (1);
    ART_OLC_X::UnloadPolicy* policy = UnloadPolicyFactory (policy_type);
    // Load the data
    Load ();
    // Get the node for each level
    tree_->urlevel_ = 3;
    tree_->calculateVictims (policy, 5, thread_info, publicList, lengthOfpublicList);
    printf ("[hybridBits1] %s\n", tree_->ToStats ().c_str ());
    std::atomic<uint64_t> vDeallocated = 0;
    Key prefix_k;
    prefix_k.set ("ap", 2);

    // tree_->offLoadPrefix (prefix_k, thread_info, nullptr, dbOffloadStr);
    // printf ("[hybridBits1-1] %s\n", tree_->ToStats ().c_str ());
    std::vector<void*> a;
    tree_->unloadDirtyPrefix (prefix_k, thread_info, nullptr, dbBatchOffloadStr, nullptr);
    printf ("[hybridBits2] %s\n", tree_->ToStats ().c_str ());
    tree_->releaseCleanPrefix (prefix_k, thread_info, vDeallocated, releaseValue, nullptr,
                               dbBatchOffloadStr, nullptr);
    printf ("[hybridBits3] %s\n", tree_->ToStats ().c_str ());

    tree_->calculateVictims (policy, 5, thread_info, publicList, lengthOfpublicList);

    Key kstart, kend, ckey;
    kstart.set ("a", 1);
    kend.set ("z", 1);

    size_t found = 0;
    bool seehybrid = false;
    uint64_t mtagets[100];
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);

    ASSERT_EQ (found, 9);
    printf ("Inserting the key to hybrid\n");
    tree_->urlevel_ = 1;
    std::string apa = "apa";
    std::string apb = "apb";
    Key key1;
    key1.set (apa.data (), apa.size ());
    tree_->insert (key1, 13, thread_info);

    found = 0;
    tree_->lookupRange (false, kstart, kend, ckey, mtagets, 100, found, thread_info, &seehybrid);
    ASSERT_EQ (found, 10);
}

int main (int argc, char** argv) {
    testing::InitGoogleTest (&argc, argv);
    int ret = RUN_ALL_TESTS ();
    PosixLogger::Destroy ();
    return ret;
}