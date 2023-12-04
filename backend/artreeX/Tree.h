//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_X_H
#define ART_OPTIMISTICLOCK_COUPLING_N_X_H

#include <functional>
#include <vector>

#include "ARTCounter.h"
#include "N.h"
#include "Policy.h"

using namespace ART;

// forward declaration of database classes
class TransactionRead;
struct IndexInfo;
constexpr size_t SizeofModifyTargetX = 24;

namespace ART_OLC_X {
class Tree {
    // the public interfaces requires ridlist_t but internally we stored as TID, so they have to be
    // consistent in size and assumption of reserved bits etc
    //
    static_assert (sizeof (TID) == sizeof (RLIST));

    // these functions are provided by database indexing layer to help indexing integration.
    using LookupRidListHandler = bool (*) (const RLIST& ridlist, TransactionRead* txn,
                                           IndexInfo* index, TID* target);
    using UpsertRidListHandler = bool (*) (RLIST& ridlist, TID tid, TransactionRead* txn,
                                           IndexInfo* index);
    using DeleteRidListHandler = bool (*) (RLIST& ridlist, TID tid, TransactionRead* txn);
    // using ReleaseHandler = void (*) (TID ridlist);
    using ReleaseHandler = std::function<void (TID)>;

    using DBOffloadHandler = bool (*) (void* store, uint8_t* key, size_t keyLen, TID tid,
                                       void* threadLocalExtra);

    using DBLookupHandler = bool (*) (void* store, uint8_t* key, size_t keyLen);

    using ScanCallback = std::function<void (TID* target)>;

    using ReleaseMemory = std::function<void (uint64_t, std::atomic<uint64_t>&)>;

    using KVStoreLookupHandler = std::function<bool (void* store, uint8_t* key, size_t keyLen)>;

public:
    using LoadKeyFunction = bool (*) (TID tid, Key& key);

    class Iterator;

private:
    N* const root;

    TID checkKey (const TID tid, const Key& k, TransactionRead* txn, TID* target = nullptr,
                  bool verifyKeyOnly = false) const;

    LoadKeyFunction loadKey;

public:
    Epoche epoche{256};

    GlobalMemoryInfo* memoryInfo;
    uint8_t urlevel_;

public:
    enum class CheckPrefixResult : uint8_t { Match, NoMatch, OptimisticMatch };

    enum class CheckPrefixPessimisticResult : uint8_t {
        Match,
        NoMatch,
    };

    enum class PCCompareResults : uint8_t {
        Smaller,
        Equal,
        Bigger,
    };
    enum class PCEqualsResults : uint8_t { BothMatch, Contained, NoMatch };
    static CheckPrefixResult checkPrefix (N* n, const Key& k, uint32_t& level);

    static CheckPrefixPessimisticResult checkPrefixPessimistic (
        N* n, const Key& k, uint32_t& level, uint8_t& nonMatchingKey, Prefix& nonMatchingPrefix,
        LoadKeyFunction loadKey, bool& needRestart, TransactionRead* txn, Tree* tree);

    static PCCompareResults checkPrefixCompare (const N* n, const Key& k, uint8_t fillKey,
                                                uint32_t& level, LoadKeyFunction loadKey,
                                                bool& needRestart, TransactionRead* txn,
                                                Tree* tree);

    static PCEqualsResults checkPrefixEquals (const N* n, uint32_t& level, const Key& start,
                                              const Key& end, LoadKeyFunction loadKey,
                                              bool& needRestart, TransactionRead* txn, Tree* tree);

public:
    Tree (LoadKeyFunction loadKey);

    Tree (const Tree&) = delete;

    Tree (Tree&& t) : root (t.root), loadKey (t.loadKey) {}

    ~Tree ();
    void ReleaseTree (ReleaseHandler handler);

    ThreadInfo getThreadInfo (TransactionRead* txn = nullptr);

    TID lookup (const Key& k, ThreadInfo& threadEpocheInfo, TID* target = nullptr,
                LookupRidListHandler handler = nullptr, bool* seeHybrid = nullptr) const;

    bool lookupRange (bool forMinMax, const Key& start, const Key& end, Key& continueKey,
                      TID result[], std::size_t resultLen, std::size_t& resultCount,
                      ThreadInfo& threadEpocheInfo, bool* seeHybrid = nullptr) const;

    size_t scanStatic (TransactionRead* txn, LookupRidListHandler handler, TID* target,
                       ScanCallback callback);

    bool insert (const Key& k, TID tid, ThreadInfo& epocheInfo,
                 ReleaseHandler release_handler = nullptr, UpsertRidListHandler handler = nullptr,
                 KVStoreLookupHandler dbLookupHandler = nullptr, void* store = nullptr,
                 bool* seeHybrid = nullptr, bool isMigrationLoad = false);

    bool remove (const Key& k, TID tid, ThreadInfo& epocheInfo,
                 DeleteRidListHandler handler = nullptr,
                 KVStoreLookupHandler dbLookupHandler = nullptr, void* store = nullptr,
                 bool* seeHybrid = nullptr);

    // offload node's children to store. The children should only be leaf or hybrid nodes.
    bool offloadNode (N* curN, ThreadInfo& epocheInfo, void* store, DBOffloadHandler handler);

    // offload node's children to store. The children should only be leaf or hybrid nodes.
    bool batchOffloadNode (N* curN, ThreadInfo& epocheInfo, void* store, DBOffloadHandler handler,
                           void* writeBatch);

    // randomly offload a node, which contains only leaf nodes
    void offLoadRandom (ThreadInfo& epocheInfo, void* store, DBOffloadHandler handler);

    // offload a left most children from node: pn->children[pkey], which contains only leaf nodes.
    bool offLoad (N* pn, uint8_t pkey, ThreadInfo& epocheInfo, void* store,
                  DBOffloadHandler handler);

    // the same logic as offLoad, will not delete the node in memory, only clean the subtree
    double offLoadDirty (N* pn, uint8_t pkey, ThreadInfo& epocheInfo, void* store,
                         DBOffloadHandler handler, void* threadLocalExtra);
    double checkSubtreeSize (N* pn, uint8_t pkey);

    bool releaseClean (N* pn, uint8_t pkey, ThreadInfo& epocheInfo,
                       std::atomic<uint64_t>& vDeallocated, ReleaseMemory releaseMemory,
                       void* store, DBOffloadHandler handler, void* threadLocalExtra);

    // offload all the keys contains prefix
    bool offLoadPrefix (const Key& prefix, ThreadInfo& epocheInfo, void* store,
                        DBOffloadHandler handler);

    /*
        unload all the keys contains prefix to lsm
        1. unload dirty data does not means release the memory copy at the same time
        2. write keys to rocksdb and set dirty or clean flag
        3. need to LOCK before unload
    */
    std::pair<bool, double> unloadDirtyPrefix (const Key& prefix, ThreadInfo& epocheInfo,
                                               void* store, DBOffloadHandler handler,
                                               void* threadLocalExtra);
    double getPrefixSubtreeSize (const Key& prefix);

    /*
        release all the nodes under the subtree
        check the correctness of the clean/dirty bit, to ensure there is only clean bit there
    */
    bool releaseCleanPrefix (const Key& prefix, ThreadInfo& epocheInfo,
                             std::atomic<uint64_t>& vDeallocated, ReleaseMemory releaseMemory,
                             void* store, DBOffloadHandler handler, void* threadLocalExtra);

    // return true when this prefix is previously offloaded.
    bool setMigrationState (const Key& prefix, ThreadInfo& epocheInfo);

    bool clearMigrationState (const Key& prefix, ThreadInfo& epocheInfo);

    void calculateVictims (UnloadPolicy* policy, size_t dumpSize, ThreadInfo& epocheInfo,
                           std::vector<candidate_t*>& publicList, uint64_t& lengthOfpublicList);

    std::string ToStats ();

    Iterator* NewIterator (TransactionRead* txn) const;

public:
    // database index added members
    IndexInfo* dbindex_;
};

class Tree::Iterator {
public:
    Iterator (TransactionRead* txn, Tree* tree, N* root);

    Iterator (const Iterator&) = delete;
    Iterator& operator= (const Iterator&) = delete;

    ~Iterator () {}

    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    bool Valid ();

    // Position at the first key in the source.  The iterator is Valid()
    // after this call iff the source is not empty.
    void SeekToFirst ();

    // Position at the first key in the source that is at or past target.
    // The iterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    void Seek (const Key& target);

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    bool Next ();

    // Return the key for the current entry.  The underlying storage for
    // the returned Key is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    bool GetKey (Key& key);

    // Return the value for the current entry.  The underlying storage for
    // the returned tid is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    TID GetTID ();

private:
    void reset () {
        cur_leaf_ = nullptr;

        // parent's ci child is me
        parent_ = nullptr;
        ci_ = 0;
        me_ = nullptr;
        me_key_ = 0;
        node_stack_.clear ();
    }

private:
    const Tree* tree_;
    ThreadInfo thread_info_;

    N* root_;

    N* cur_leaf_ = nullptr; /* When Valid(), cur_leaf_ points to a leaf node */

    // parent's ci child is me
    N* parent_ = nullptr;
    int ci_ = 0;
    N* me_ = nullptr;
    uint8_t me_key_ = 0; /* The value that me_ node stands for */

    std::vector<std::tuple<N* /* parent */, int /* ci */, N* /* me*/>> node_stack_;
};

}  // namespace ART_OLC_X
#endif  // ART_OPTIMISTICLOCK_COUPLING_N_H
