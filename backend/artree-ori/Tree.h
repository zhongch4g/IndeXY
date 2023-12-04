//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_H
#define ART_OPTIMISTICLOCK_COUPLING_N_H

#include <functional>
#include <vector>

#include "N.h"

using namespace ART;

// forward declaration of database classes
class TransactionRead;
struct IndexInfo;
constexpr size_t SizeofModifyTarget = 24;

namespace ART_OLC_ORI {
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
    using ReleaseHandler = void (*) (TID ridlist);

    using DBOffloadHandler = bool (*) (void* store, uint8_t* key, size_t keyLen, TID tid);

    using ScanCallback = std::function<void (TID* target)>;

public:
    using LoadKeyFunctionA = bool (*) (TID tid, Key& key, TransactionRead* txn, IndexInfo* dbindex,
                                       TID* target, bool verifyKeyOnly);
    using LoadKeyFunctionB = bool (*) (TID tid, Key& key);

    class Iterator;

private:
    N* const root;

    TID checkKey (const TID tid, const Key& k, TransactionRead* txn, TID* target = nullptr,
                  bool verifyKeyOnly = false) const;

    LoadKeyFunctionB loadKey;

    Epoche epoche{256};

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
        LoadKeyFunctionB loadKeyB, bool& needRestart, TransactionRead* txn, Tree* tree);

    static PCCompareResults checkPrefixCompare (const N* n, const Key& k, uint8_t fillKey,
                                                uint32_t& level, LoadKeyFunctionB loadKeyB,
                                                bool& needRestart, TransactionRead* txn,
                                                Tree* tree);

    static PCEqualsResults checkPrefixEquals (const N* n, uint32_t& level, const Key& start,
                                              const Key& end, LoadKeyFunctionB loadKeyB,
                                              bool& needRestart, TransactionRead* txn, Tree* tree);

public:
    Tree (LoadKeyFunctionB loadKeyB);

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
                 bool* seeHybrid = nullptr);

    bool remove (const Key& k, TID tid, ThreadInfo& epocheInfo,
                 DeleteRidListHandler handler = nullptr, bool* seeHybrid = nullptr);

    bool offloadNode (N* curN, ThreadInfo& epocheInfo, void* store, DBOffloadHandler handler);

    void offLoadRandom (ThreadInfo& epocheInfo, void* store, DBOffloadHandler handler);

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

}  // namespace ART_OLC_ORI
#endif  // ART_OPTIMISTICLOCK_COUPLING_N_H
