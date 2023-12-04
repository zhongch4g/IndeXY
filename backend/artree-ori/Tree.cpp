#include "Tree.h"

#include <assert.h>

#include <algorithm>
#include <functional>

#include "N.cpp"
#include "artree-shared-headers/Epoche.cpp"
#include "artree-shared-headers/Key.h"

namespace ART_OLC_ORI {

// ensure compile generate the type symbol
static TVLO_t tvlo_dummy;

Tree::Tree (LoadKeyFunctionB loadKey) : root (new N256 (nullptr, 0)), loadKey (loadKey) {}

Tree::~Tree () {}

void Tree::ReleaseTree (ReleaseHandler callback) {
    N::deleteChildren (root, callback);
    N::deleteNode (root, callback);
}

ThreadInfo Tree::getThreadInfo (TransactionRead* txn) { return ThreadInfo (this->epoche, txn); }

TID Tree::lookup (const Key& k, ThreadInfo& threadEpocheInfo, TID* target,
                  LookupRidListHandler callback, bool* seeHybrid) const {
    EpocheGuardReadonly epocheGuard (threadEpocheInfo);
restart:
    bool needRestart = false;

    N* node;
    N* parentNode = nullptr;
    uint64_t v;
    uint32_t level = 0;
    bool optimisticPrefixMatch = false;

    node = root;
    v = node->readLockOrRestart (needRestart);
    if (needRestart) goto restart;
    while (true) {
        switch (checkPrefix (node, k, level)) {  // increases level
            case CheckPrefixResult::NoMatch:
                node->readUnlockOrRestart (v, needRestart);
                if (needRestart) goto restart;
                return 0;
            case CheckPrefixResult::OptimisticMatch:
                optimisticPrefixMatch = true;
                [[fallthrough]];
            case CheckPrefixResult::Match:
                if (k.getKeyLen () <= level) {
                    return 0;
                }
                parentNode = node;
                node = N::getChild (k[level], parentNode);
                parentNode->checkOrRestart (v, needRestart);
                if (needRestart) goto restart;

                if (node == nullptr) {
                    return 0;
                }
                if (N::isLeaf (node)) {
                    parentNode->readUnlockOrRestart (v, needRestart);
                    if (needRestart) goto restart;

                    // check if node is a hybrid node
                    if (N::isHybrid (node) && seeHybrid != nullptr) {
                        *seeHybrid = true;
                        return 0;
                    }

                    auto tidlist = N::getLeaf (node);
                    if (level < k.getKeyLen () - 1 || optimisticPrefixMatch) {
                        return checkKey (tidlist, k, threadEpocheInfo.getTransaction (), target);
                    }

                    // a leaf match means need to verify key
                    auto ret = callback
                                   ? callback ((RLIST)tidlist, threadEpocheInfo.getTransaction (),
                                               dbindex_, target)
                                   : true;
                    return ret ? tidlist : 0;
                }
                level++;
        }
        uint64_t nv = node->readLockOrRestart (needRestart);
        if (needRestart) goto restart;

        parentNode->readUnlockOrRestart (v, needRestart);
        if (needRestart) goto restart;
        v = nv;
    }
}

bool Tree::lookupRange (bool forMinMax, const Key& start, const Key& end, Key& continueKey,
                        TID result[], std::size_t resultSize, std::size_t& resultsFound,
                        ThreadInfo& threadEpocheInfo, bool* seeHybrid) const {
    if (forMinMax) assert (resultSize == 1);
    for (uint32_t i = 0; i < std::min (start.getKeyLen (), end.getKeyLen ()); ++i) {
        if (start[i] > end[i]) {
            resultsFound = 0;
            return false;
        } else if (start[i] < end[i]) {
            break;
        }
    }
    EpocheGuard epocheGuard (threadEpocheInfo);
    TID toContinue = 0;

    std::function<void (const N*)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy,
                                           &continueKey, &threadEpocheInfo, &forMinMax, this,
                                           &seeHybrid] (const N* node) {
        bool needRestart = false;
        if (N::isLeaf (node)) {
            assert (resultsFound <= resultSize);

            // check if node is a hybrid node
            if (N::isHybrid (node) && seeHybrid != nullptr) {
                *seeHybrid = true;
                return;
            }

            // process the ridlist and if it works put into result set
            //
            auto ridlist = N::getLeaf (node);
            auto target = resultsFound < resultSize
                              ? (TID*)((char*)result + (resultsFound * SizeofModifyTarget))
                              : nullptr;

            // if the result buffer is full or for min/max search, we load the key. The fomer one is
            // because we need to a meaningful continue key for next search. The second one is to
            // avid repeated in and out of this procedure due to batch size 1.
            //
            // set toContinue to non-zero to break the search loop.
            //
            if (resultsFound == resultSize || forMinMax) {
                assert (ridlist != 0);

                bool found = loadKey (ridlist, continueKey);
                if (forMinMax) {
                    // if find a good one, we call it done; otherwise, continue the search loop
                    assert (!resultsFound);
                    if (found) {
                        resultsFound = 1;
                        // there is no point to continue, so fake a toContinue to break the loop
                        toContinue = TID (-1);
                        return;
                    }
                    assert (toContinue == 0);
                } else {
                    // can't find a meaningful continue key, have to continue the search loop
                    if (!found) {
                        assert (toContinue == 0);
                        return;
                    }
                    toContinue = ridlist;
                    assert (toContinue != 0);
                }
            } else {
                assert (!forMinMax);
                *(TID*)target = ridlist;
                resultsFound++;
            }
        } else {
            std::tuple<uint8_t, N*> children[256];
            uint32_t childrenCount = 0;
            auto v = N::getChildren (node, 0u, 255u, children, childrenCount, needRestart);
            if (needRestart) {
                assert (N::isObsolete (v));
                return;
            }

            for (uint32_t i = 0; i < childrenCount; ++i) {
                const N* n = std::get<1> (children[i]);
                copy (n);
                if (toContinue != 0) {
                    break;
                }
            }
        }
    };

    std::function<void (N*, uint8_t, uint32_t, const N*, uint64_t)> findStart =
        [&copy, &start, &findStart, &toContinue, &threadEpocheInfo, this] (
            N* node, uint8_t nodeK, uint32_t level, const N* parentNode, uint64_t vp) {
            if (N::isLeaf (node)) {
                copy (node);
                return;
            }
            bool needRestart = false;
            uint64_t v;
            PCCompareResults prefixResult;

            {
            readAgain:
                needRestart = false;
                v = node->readLockOrRestart (needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare (node, start, 0, level, loadKey, needRestart,
                                                   threadEpocheInfo.getTransaction (),
                                                   const_cast<Tree*> (this));
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart (vp, needRestart);
                if (needRestart) {
                readParentAgain:
                    needRestart = false;
                    vp = parentNode->readLockOrRestart (needRestart);
                    if (needRestart) {
                        if (N::isLocked (vp))
                            goto readParentAgain;
                        else
                            return;
                    }

                    node = N::getChild (nodeK, parentNode);

                    parentNode->readUnlockOrRestart (vp, needRestart);
                    if (needRestart) {
                        if (N::isLocked (vp))
                            goto readParentAgain;
                        else
                            return;
                    }

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf (node)) {
                        copy (node);
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart (v, needRestart);
                if (needRestart) goto readAgain;
            }

            switch (prefixResult) {
                case PCCompareResults::Bigger:
                    copy (node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen () > level) ? start[level] : 0;
                    std::tuple<uint8_t, N*> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren (node, startLevel, 255, children, childrenCount,
                                        needRestart);
                    if (needRestart) {
                        assert (N::isObsolete (v));
                        return;
                    }

                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0> (children[i]);
                        N* n = std::get<1> (children[i]);
                        if (k == startLevel) {
                            findStart (n, k, level + 1, node, v);
                        } else if (k > startLevel) {
                            copy (n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Smaller:
                    break;
            }
        };

    std::function<void (N*, uint8_t, uint32_t, const N*, uint64_t)> findEnd =
        [&copy, &end, &toContinue, &findEnd, &threadEpocheInfo, this] (
            N* node, uint8_t nodeK, uint32_t level, const N* parentNode, uint64_t vp) {
            if (N::isLeaf (node)) {
                copy (node);
                return;
            }
            bool needRestart = false;
            uint64_t v;
            PCCompareResults prefixResult;
            {
            readAgain:
                needRestart = false;
                v = node->readLockOrRestart (needRestart);
                if (needRestart) {
                    if (N::isLocked (vp))
                        goto readAgain;
                    else
                        return;
                }

                prefixResult = checkPrefixCompare (node, end, 255, level, loadKey, needRestart,
                                                   threadEpocheInfo.getTransaction (),
                                                   const_cast<Tree*> (this));
                if (needRestart) {
                    if (N::isLocked (vp))
                        goto readAgain;
                    else
                        return;
                }

                parentNode->readUnlockOrRestart (vp, needRestart);
                if (needRestart) {
                readParentAgain:
                    vp = parentNode->readLockOrRestart (needRestart);
                    if (needRestart) {
                        if (N::isLocked (vp))
                            goto readParentAgain;
                        else
                            return;
                    }
                    node = N::getChild (nodeK, parentNode);

                    parentNode->readUnlockOrRestart (vp, needRestart);
                    if (needRestart) {
                        if (N::isLocked (vp))
                            goto readParentAgain;
                        else
                            return;
                    }

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf (node)) {
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart (v, needRestart);
                if (needRestart) goto readAgain;
            }
            switch (prefixResult) {
                case PCCompareResults::Smaller:
                    copy (node);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel = (end.getKeyLen () > level) ? end[level] : 255;
                    std::tuple<uint8_t, N*> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren (node, 0, endLevel, children, childrenCount, needRestart);
                    if (needRestart) {
                        assert (N::isObsolete (v));
                        return;
                    }

                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0> (children[i]);
                        N* n = std::get<1> (children[i]);
                        if (k == endLevel) {
                            findEnd (n, k, level + 1, node, v);
                        } else if (k < endLevel) {
                            copy (n);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
                    break;
            }
        };

restart:
    bool needRestart = false;

    resultsFound = 0;

    uint32_t level = 0;
    N* node = nullptr;
    N* nextNode = root;
    N* parentNode;
    uint64_t v = 0;
    uint64_t vp;

    while (nextNode != nullptr) {
        parentNode = node;
        vp = v;
        node = nextNode;
        PCEqualsResults prefixResult;
        v = node->readLockOrRestart (needRestart);
        if (needRestart) goto restart;
        prefixResult =
            checkPrefixEquals (node, level, start, end, loadKey, needRestart,
                               threadEpocheInfo.getTransaction (), const_cast<Tree*> (this));
        if (needRestart) goto restart;
        if (parentNode != nullptr) {
            parentNode->readUnlockOrRestart (vp, needRestart);
            if (needRestart) goto restart;
        }
        node->readUnlockOrRestart (v, needRestart);
        if (needRestart) goto restart;

        switch (prefixResult) {
            case PCEqualsResults::NoMatch: {
                return false;
            }
            case PCEqualsResults::Contained: {
                copy (node);
                break;
            }
            case PCEqualsResults::BothMatch: {
                uint8_t startLevel = (start.getKeyLen () > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen () > level) ? end[level] : 255;
                if (startLevel != endLevel) {
                    std::tuple<uint8_t, N*> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren (node, startLevel, endLevel, children, childrenCount,
                                        needRestart);
                    if (needRestart) {
                        assert (N::isObsolete (v));
                        goto restart;
                    }

                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0> (children[i]);
                        N* n = std::get<1> (children[i]);
                        if (k == startLevel) {
                            findStart (n, k, level + 1, node, v);
                        } else if (k > startLevel && k < endLevel) {
                            copy (n);
                        } else if (k == endLevel) {
                            findEnd (n, k, level + 1, node, v);
                        }
                        if (toContinue) {
                            break;
                        }
                    }
                } else {
                    nextNode = N::getChild (startLevel, node);
                    node->readUnlockOrRestart (v, needRestart);
                    if (needRestart) goto restart;

                    if (N::isLeaf (nextNode)) {
                        copy (nextNode);
                        break;
                    }
                    level++;
                    continue;
                }
                break;
            }
        }
        break;
    }
    if (toContinue != 0 && !forMinMax) {
        // we already verified this is a good key at copy() for continue search - it is important we
        // don't invoke loadKey() to reload it when we are running under RC isolation level (higher
        // level is ok).
        //
        assert (continueKey.getKeyLen () != 0);
        return true;
    } else {
        return false;
    }
}

TID Tree::checkKey (const TID tid, const Key& k, TransactionRead* txn, TID* target,
                    bool verifyKeyOnly) const {
    Key kt;
    this->loadKey (tid, kt);
    if (k == kt) {
        return tid;
    }
    return 0;
}

bool Tree::offloadNode (N* curN, ThreadInfo& epocheInfo, void* store, DBOffloadHandler handler) {
    // curN should be the node that contains only leafnode (may contains hybrid node)
    // curN hold write lock

    std::vector<N*> children;

    switch (curN->getType ()) {
        case NTypes::N4: {
            N4* n = static_cast<N4*> (curN);

            for (uint32_t i = 0; i < n->getCount (); i++) {
                children.push_back (n->children[i]);
            }

            break;
        }
        case NTypes::N16: {
            N16* n = static_cast<N16*> (curN);
            for (uint32_t i = 0; i < n->getCount (); i++) {
                children.push_back (n->children[i]);
            }
            break;
        }
        case NTypes::N48: {
            N48* n = static_cast<N48*> (curN);
            for (uint32_t i = 0; i < n->getCount (); i++) {
                children.push_back (n->children[i]);
            }
            break;
        }
        case NTypes::N256: {
            N256* n = static_cast<N256*> (curN);
            for (uint32_t i = 0; i < 256; i++) {
                if (n->children[i] != nullptr) {
                    children.push_back (n->children[i]);
                }
            }
            break;
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }

    for (size_t i = 0; i < children.size (); i++) {
        // hybird node is also a leafnode, we ignore it
        if (N::isHybrid (children[i])) continue;

        auto tidlist = N::getLeaf (children[i]);
        Key key;
        loadKey (tidlist, key);
        bool ret = handler (store, key.getData (), key.getKeyLen (), tidlist);
        if (!ret) {
            return false;
        }
    }
    return true;
}

void Tree::offLoadRandom (ThreadInfo& threadInfo, void* store, DBOffloadHandler handler) {
    // pick a random node that contains only leaf node or hybrid node
restart:
    bool needRestart = false;
    N* node = nullptr;
    N* nextNode = root;
    N* parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint64_t parentVersion = 0;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->readLockOrRestart (needRestart);
        if (needRestart) goto restart;

        // obtain a random non-leaf child
        assert (!N::isLeaf (node));
        std::tie (nextNode, nodeKey) = N::getRandomChildNonLeaf (node);

        node->checkOrRestart (v, needRestart);
        if (needRestart) goto restart;

        if (nextNode == nullptr) {
            // means the node contains only leafnode or hybrid node
            // node is the offloading target
            if (parentNode) {
                parentNode->upgradeToWriteLockOrRestart (parentVersion, needRestart);
                if (needRestart) goto restart;

                node->upgradeToWriteLockOrRestart (v, needRestart);
                if (needRestart) {
                    parentNode->writeUnlock ();
                    goto restart;
                }

                // offloading the node to db first
                offloadNode (node, threadInfo, store, handler);

                // then mark node as hybrid and recycle
                this->epoche.markNodeForDeletion (node, threadInfo);
                N::change (parentNode, parentKey, N::setHybrid (node));
                parentNode->writeUnlock ();

                node->writeUnlock ();
            }
            return;
        }
        parentKey = nodeKey;
        parentVersion = v;
    }
}

bool Tree::insert (const Key& k, TID tid, ThreadInfo& epocheInfo, ReleaseHandler release_handler,
                   UpsertRidListHandler ridlistHandler, bool* seeHybrid) {
    EpocheGuard epocheGuard (epocheInfo);
restart:
    bool needRestart = false;

    N* node = nullptr;
    N* nextNode = root;
    N* parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint64_t parentVersion = 0;
    uint32_t level = 0;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->readLockOrRestart (needRestart);
        if (needRestart) goto restart;

        uint32_t nextLevel = level;

        uint8_t nonMatchingKey = std::numeric_limits<uint8_t>::max ();
        Prefix remainingPrefix;
        auto res = checkPrefixPessimistic (node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                           this->loadKey, needRestart, epocheInfo.getTransaction (),
                                           this);  // increases level
        if (needRestart) goto restart;
        switch (res) {
            case CheckPrefixPessimisticResult::NoMatch: {
                parentNode->upgradeToWriteLockOrRestart (parentVersion, needRestart);
                if (needRestart) goto restart;

                node->upgradeToWriteLockOrRestart (v, needRestart);
                if (needRestart) {
                    parentNode->writeUnlock ();
                    goto restart;
                }
                // 1) Create new node which will be parent of node, Set common prefix, level to this
                // node
                auto newNode = new N4 (node->getPrefix (), nextLevel - level);

                // 2)  add node and (tid, *k) as children
                newNode->insert (k[nextLevel], N::setLeaf (tid));
                newNode->insert (nonMatchingKey, node);

                // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node,
                // unlock
                N::change (parentNode, parentKey, newNode);
                parentNode->writeUnlock ();

                // 4) update prefix of node, unlock
                node->setPrefix (remainingPrefix,
                                 node->getPrefixLength () - ((nextLevel - level) + 1));

                node->writeUnlock ();
                return true;
            }
            case CheckPrefixPessimisticResult::Match:
                break;
        }
        level = nextLevel;
        nodeKey = k[level];
        nextNode = N::getChild (nodeKey, node);
        node->checkOrRestart (v, needRestart);
        if (needRestart) goto restart;

        // no conflicting keys there, so just insert the tid
        //
        if (nextNode == nullptr) {
            N::insertAndUnlock (node, v, parentNode, parentVersion, parentKey, nodeKey,
                                N::setLeaf (tid), needRestart, epocheInfo);
            if (needRestart) goto restart;
            return true;
        }

        if (parentNode != nullptr) {
            parentNode->readUnlockOrRestart (parentVersion, needRestart);
            if (needRestart) goto restart;
        }

        // one tid in the leaf, let's create a N4 to containing both existing and new one
        //
        if (N::isLeaf (nextNode)) {
            Key key;
            node->upgradeToWriteLockOrRestart (v, needRestart);
            if (needRestart) goto restart;

            // check if nextNode is a hybrid node, then we stop insertion
            if (N::isHybrid (nextNode) && seeHybrid != nullptr) {
                node->writeUnlock ();
                *seeHybrid = true;
                return false;
            }

            // the purpose of this load key is to see if key matches
            auto tidlist = N::getLeaf (nextNode);
            loadKey (tidlist, key);
            if (key.getKeyLen () == 0) {
                // found a key but version removed (could be left from an committed deletion with
                // version GC'ed first). We can replace it.

                // if the tidlist is a list, we should release it, since the
                // loadKey() function is called with verifykeyonly=true, this
                // means it just try to find any available version, and the
                // empty key means there's none.
                release_handler (tidlist);

                N::change (node, k[level], N::setLeaf (tid));
                node->writeUnlock ();
                return true;
            }

            level++;

            int sharedlen = level;
            int maxlen = key.getKeyLen ();
            while (sharedlen < maxlen && key[sharedlen] == k[sharedlen]) {
                sharedlen++;
            }

            if (sharedlen == maxlen) {
                // found a duplicate key, need to ask database to verify uniqueness and merge
                // the list. Replace the old one with the merged list.
                //
                RLIST mergelist = (RLIST)tidlist;
                auto ret = ridlistHandler ? ridlistHandler (mergelist, tid,
                                                            epocheInfo.getTransaction (), dbindex_)
                                          : true;
                if (ret) {
                    N::change (node, k[level - 1], N::setLeaf ((TID)mergelist));
                }
                node->writeUnlock ();
                return ret;
            } else {
                // found a key with shared prefix, introduce a new node to include both
                //
                // for (int i = 0; i < sharedlen; i++) assert (key[i] == k[i]);
                auto n4 = new N4 (&k[level], sharedlen - level);
                n4->insert (k[sharedlen], N::setLeaf (tid));
                n4->insert (key[sharedlen], nextNode);
                N::change (node, k[level - 1], n4);
                node->writeUnlock ();
                return true;
            }
        }

        level++;
        parentVersion = v;
    }
}

bool Tree::remove (const Key& k, TID tid, ThreadInfo& threadInfo,
                   DeleteRidListHandler ridlistHandler, bool* seeHybrid) {
    EpocheGuard epocheGuard (threadInfo);
restart:
    bool needRestart = false;

    N* node = nullptr;
    N* nextNode = root;
    N* parentNode = nullptr;
    uint8_t parentKey, nodeKey = 0;
    uint64_t parentVersion = 0;
    uint32_t level = 0;

    while (true) {
        parentNode = node;
        parentKey = nodeKey;
        node = nextNode;
        auto v = node->readLockOrRestart (needRestart);
        if (needRestart) goto restart;

        switch (checkPrefix (node, k, level)) {  // increases level
            case CheckPrefixResult::NoMatch:
                node->readUnlockOrRestart (v, needRestart);
                if (needRestart) goto restart;
                return false;
            case CheckPrefixResult::OptimisticMatch:
                // fallthrough
            case CheckPrefixResult::Match: {
                nodeKey = k[level];
                nextNode = N::getChild (nodeKey, node);

                node->checkOrRestart (v, needRestart);
                if (needRestart) goto restart;

                if (nextNode == nullptr) {
                    node->readUnlockOrRestart (v, needRestart);
                    if (needRestart) goto restart;
                    return false;
                }
                if (N::isLeaf (nextNode)) {
                    // target tid could be the leaf itself (most cases) or in the list
                    //

                    // check if node is a hybrid node
                    if (N::isHybrid (nextNode) && seeHybrid != nullptr) {
                        node->readUnlockOrRestart (v, needRestart);
                        if (needRestart) goto restart;
                        *seeHybrid = true;
                        return false;
                    }

                    bool ret;
                    auto tidlist = N::getLeaf (nextNode);
                    if (ridlistHandler == nullptr) {
                        // if handler is not given, we simply compare if leaf is target tid
                        ret = tidlist == tid;
                    } else {
                        node->upgradeToWriteLockOrRestart (v, needRestart);
                        if (needRestart) goto restart;

                        RLIST mergelist = (RLIST)tidlist;
                        ret = ridlistHandler (mergelist, tid, threadInfo.getTransaction ());
                        if (ret) {
                            if (tidlist != tid) {
                                // this is the case of delete from list: no index entry is removed
                                // because there are other entries there. so we can return now.
                                //
                                N::change (node, k[level], N::setLeaf ((TID)mergelist));
                                node->writeUnlock ();
                                return true;
                            }
                        }
                        node->writeUnlock ();
                        v = node->readLockOrRestart (needRestart);
                        if (needRestart) goto restart;
                    }
                    if (!ret) return false;

                    // remove the leaf from index
                    assert (parentNode == nullptr || node->getCount () != 1);
                    if (node->getCount () == 2 && parentNode != nullptr) {
                        parentNode->upgradeToWriteLockOrRestart (parentVersion, needRestart);
                        if (needRestart) goto restart;

                        node->upgradeToWriteLockOrRestart (v, needRestart);
                        if (needRestart) {
                            parentNode->writeUnlock ();
                            goto restart;
                        }
                        // 1. check remaining entries
                        N* secondNodeN;
                        uint8_t secondNodeK;
                        std::tie (secondNodeN, secondNodeK) = N::getSecondChild (node, nodeKey);
                        if (N::isLeaf (secondNodeN)) {
                            // N::remove(node, k[level]); not necessary
                            N::change (parentNode, parentKey, secondNodeN);

                            parentNode->writeUnlock ();
                            node->writeUnlockObsolete ();
                            this->epoche.markNodeForDeletion (node, threadInfo);
                        } else {
                            secondNodeN->writeLockOrRestart (needRestart);
                            if (needRestart) {
                                node->writeUnlock ();
                                parentNode->writeUnlock ();
                                goto restart;
                            }

                            // N::remove(node, k[level]); not necessary
                            N::change (parentNode, parentKey, secondNodeN);
                            parentNode->writeUnlock ();

                            secondNodeN->addPrefixBefore (node, secondNodeK);
                            secondNodeN->writeUnlock ();

                            node->writeUnlockObsolete ();
                            this->epoche.markNodeForDeletion (node, threadInfo);
                        }
                    } else {
                        N::removeAndUnlock (node, v, k[level], parentNode, parentVersion, parentKey,
                                            needRestart, threadInfo);
                        if (needRestart) goto restart;
                    }
                    return true;
                }
                level++;
                parentVersion = v;
            }
        }
    }
}

inline typename Tree::CheckPrefixResult Tree::checkPrefix (N* n, const Key& k, uint32_t& level) {
    if (n->hasPrefix ()) {
        if (k.getKeyLen () <= level + n->getPrefixLength ()) {
            return CheckPrefixResult::NoMatch;
        }
        for (uint32_t i = 0; i < std::min (n->getPrefixLength (), maxStoredPrefixLength); ++i) {
            if (n->getPrefix ()[i] != k[level]) {
                return CheckPrefixResult::NoMatch;
            }
            ++level;
        }
        if (n->getPrefixLength () > maxStoredPrefixLength) {
            level = level + (n->getPrefixLength () - maxStoredPrefixLength);
            return CheckPrefixResult::OptimisticMatch;
        }
    }
    return CheckPrefixResult::Match;
}

typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic (
    N* n, const Key& k, uint32_t& level, uint8_t& nonMatchingKey, Prefix& nonMatchingPrefix,
    LoadKeyFunctionB loadKey, bool& needRestart, TransactionRead* txn, Tree* tree) {
    // checks the prefix for insertion purpose so no visibility verification is needed
    if (n->hasPrefix ()) {
        uint32_t prevLevel = level;
        Key kt;
        for (uint32_t i = 0; i < n->getPrefixLength (); ++i) {
            if (i == maxStoredPrefixLength) {
                // FIXME: this logic is not right with db's GC because here we want a prefix if any
                // underneath the node - however, GC may destroy the version so there is no key
                // found for that particular leaf. We should introduce a getAnyChildTidIfany()
                // method which exhausts all leafs to retrieve the key value.
                //
                auto anyTID = N::getAnyChildTid (n, needRestart);
                if (needRestart) return CheckPrefixPessimisticResult::Match;
                loadKey (anyTID, kt);
            }
            uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix ()[i];
            if (curKey != k[level]) {
                nonMatchingKey = curKey;
                if (n->getPrefixLength () > maxStoredPrefixLength) {
                    if (i < maxStoredPrefixLength) {
                        auto anyTID = N::getAnyChildTid (n, needRestart);
                        if (needRestart) return CheckPrefixPessimisticResult::Match;
                        loadKey (anyTID, kt);
                    }
                    memcpy (nonMatchingPrefix, &kt[0] + level + 1,
                            std::min ((n->getPrefixLength () - (level - prevLevel) - 1),
                                      maxStoredPrefixLength));
                } else {
                    memcpy (nonMatchingPrefix, n->getPrefix () + i + 1,
                            n->getPrefixLength () - i - 1);
                }
                return CheckPrefixPessimisticResult::NoMatch;
            }
            ++level;
        }
    }
    return CheckPrefixPessimisticResult::Match;
}

typename Tree::PCCompareResults Tree::checkPrefixCompare (const N* n, const Key& k, uint8_t fillKey,
                                                          uint32_t& level, LoadKeyFunctionB loadKey,
                                                          bool& needRestart, TransactionRead* txn,
                                                          Tree* tree) {
    if (n->hasPrefix ()) {
        Key kt;
        for (uint32_t i = 0; i < n->getPrefixLength (); ++i) {
            if (i == maxStoredPrefixLength) {
                auto anyTID = N::getAnyChildTid (n, needRestart);
                if (needRestart) return PCCompareResults::Equal;
                loadKey (anyTID, kt);
            }
            uint8_t kLevel = (k.getKeyLen () > level) ? k[level] : fillKey;

            uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix ()[i];
            if (curKey < kLevel) {
                return PCCompareResults::Smaller;
            } else if (curKey > kLevel) {
                return PCCompareResults::Bigger;
            }
            ++level;
        }
    }
    return PCCompareResults::Equal;
}

typename Tree::PCEqualsResults Tree::checkPrefixEquals (const N* n, uint32_t& level,
                                                        const Key& start, const Key& end,
                                                        LoadKeyFunctionB loadKey, bool& needRestart,
                                                        TransactionRead* txn, Tree* tree) {
    if (n->hasPrefix ()) {
        Key kt;
        for (uint32_t i = 0; i < n->getPrefixLength (); ++i) {
            if (i == maxStoredPrefixLength) {
                auto anyTID = N::getAnyChildTid (n, needRestart);
                if (needRestart) return PCEqualsResults::BothMatch;
                loadKey (anyTID, kt);
            }
            uint8_t startLevel = (start.getKeyLen () > level) ? start[level] : 0;
            uint8_t endLevel = (end.getKeyLen () > level) ? end[level] : 255;

            uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix ()[i];
            if (curKey > startLevel && curKey < endLevel) {
                return PCEqualsResults::Contained;
            } else if (curKey < startLevel || curKey > endLevel) {
                return PCEqualsResults::NoMatch;
            }
            ++level;
        }
    }
    return PCEqualsResults::BothMatch;
}

size_t Tree::scanStatic (TransactionRead* txn, LookupRidListHandler handler, TID* target,
                         ScanCallback callback) {
    N::Stat stat;
    if (root) {
        N::DeepVisit ((N*)root, stat, [&] (N* node) {
            if (N::isLeaf (node)) {
                auto tidlist = (RLIST)N::getLeaf (node);
                if (handler (tidlist, txn, dbindex_, target)) callback (target);
            }
        });
        return stat.nleafs_;
    }

    return 0;
}

std::string Tree::ToStats () {
    N::Stat stat;
    uint32_t height = 0;

    if (root) {
        N::DeepVisit ((N*)root, stat, [&] (N* node) {
            if (stat.height_ > height) height = stat.height_;
        });

        std::string str;
        str += "height: " + std::to_string (height);
        str += ", #leafs: " + std::to_string (stat.nleafs_);
        str += ", #n4-n256: (" + std::to_string (stat.n4_) + ", " + std::to_string (stat.n16_) +
               ", " + std::to_string (stat.n48_) + ", " + std::to_string (stat.n256_) + ")";

        uint64_t maxsize = 4 * stat.n4_ + 16 * stat.n16_ + 48 * stat.n48_ + 256 * stat.n256_;
        double density = (stat.nleafs_ * 1.0) / maxsize;
        str += ", density: " + std::to_string (density);

        uint64_t sizebytes = sizeof (N4) * stat.n4_ + sizeof (N16) * stat.n16_ +
                             sizeof (N48) * stat.n48_ + sizeof (N256) * stat.n256_;
        str += ", memory: " + std::to_string (sizebytes / 1024 / 1024) + " M";

        return str;
    } else
        return {};
}

Tree::Iterator* Tree::NewIterator (TransactionRead* txn) const {
    return new Iterator (txn, const_cast<Tree*> (this), root);
}

Tree::Iterator::Iterator (TransactionRead* txn, Tree* tree, N* root)
    : tree_ (tree), thread_info_ (tree->getThreadInfo (txn)), root_ (root) {}

bool Tree::Iterator::Valid () { return me_ != nullptr || node_stack_.size () != 0; }

void Tree::Iterator::SeekToFirst () {
    reset ();
    me_ = root_;
    Next ();
}

/**
 * @brief Find the first key that >= target
 * @note
 *
   ?#    :  a leaf node
   ?(xx) :  ?    is the value of this node (uint8_t)
            (xx) is the remaining prefix

                                root()
                    /~~~~~~~~~~~~|~~~~~~~~~~~~~\
               ① a            ② b           ③ c(a)
            /~~~~~|~~~~\         |~~~~~\       |~~~\
      ④ b(a)  ⑤ l#  ⑥ p     ⑦ u(sh)  w#   ⑨ b#   l(ci) ⑩
         /~~\          /~~\      /~~\   ⑧           /~~\
    ⑪ s#   t# ⑫    a#    p ⑭  e#  y#           ⑰ t#    u# ⑱
                   ⑬    /~~\   ⑮   ⑯
                      a#    l#
 *                    ⑲     ⑳
 * {"abas", "abat", "al", "apa", "appa", "appl", "bushe", "bushy", "bw", "cab", "calcit", "calciu"}
 *
 * (0) Seek("bye") -> "cab"
 *      node_stack_: [nullptr,0,root] -> [root,1,②]
 *      parent_: ②
 *      ci_: 0
 *      me_: nullptr
 *      Run Next(), we go to ⑨
 * (1) Seek("abat") -> "abat"
 *      node_stack_: [nullptr,0,root] -> [root,0,①] -> [①,0,④]
 *      parent_: ④
 *      ci_: 1
 *      me_: ⑫
 * (2) Seek("abatxyz") -> "al"
 *      node_stack_: [nullptr,0,root] -> [root,0,①] -> [①,0,④]
 *      parent_: ④
 *      ci_: 1
 *      me_: ⑫
 *      target.getKeyLen() > me_len, first time we run Next(), cur_leaf_ is ⑫, then we run Next()
 *      again, cur_leaf_ is ⑤
 * (3) Seek("amd") -> "apa"
 *      node_stack_: [nullptr,0,root] -> [root,0,①]
 *      parent_: ①
 *      ci_: 2
 *      me_: ⑥
 *      Then run Next(), we go to ⑬
 * (4) Seek("bud") -> "bushe"
 *      node_stack_: [nullptr,0,root] -> [root,1,②]
 *      parent_: ②
 *      ci_: 0
 *      me_: ⑦
 *      Then run Next(), we go to ⑮
 * (5) Seek("buy") -> "bw"
 *      node_stack_: [nullptr,0,root] -> [root,1,②]
 *      parent_: ②
 *      ci_: 0
 *      me_: ⑦
 *      Obtain me_'s peer node, then run Next(). go to ⑧
 *
 */
void Tree::Iterator::Seek (const Key& target) {
    if (target.getKeyLen () == 0) {
        SeekToFirst ();
        return;
    }

    reset ();
    me_ = root_;
    uint32_t me_len = 0;

    // compare node's prefix with target
    //  0: prefix == target
    // -1: prefix  < target
    //  1: prefix  > target
    auto prefixComparetor = [&] (const N* n) {
        if (n->hasPrefix ()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength (); ++i) {
                if (i == maxStoredPrefixLength) {
                    // A node can store at most maxStoredPrefixLength prefix. If the compression
                    // causes more prefix than maxStoredPrefixLength (n->getPrefixLength() >
                    // maxStoredPrefixLength), we should load the full key of any child so the full
                    // prefix can be obtained.
                    bool should_restart = false;
                    auto anyTID = N::getAnyChildTid (n, should_restart);
                    assert (!should_restart);
                    tree_->loadKey (anyTID, kt);
                }
                // If target's key length is smaller than trie me_len, we fill the target value at
                // me_len with 0
                uint8_t target_key_at_len = (target.getKeyLen () > me_len) ? target[me_len] : 0;

                uint8_t cur_key = i >= maxStoredPrefixLength ? kt[me_len] : n->getPrefix ()[i];
                if (cur_key < target_key_at_len) {
                    return -1;
                } else if (cur_key > target_key_at_len) {
                    return 1;
                }
                ++me_len;
            }
        }
        return 0;
    };

    while (true) {
        if (me_ == nullptr) {
            // See (0)
            Next ();
            return;
        }
        // me_key_ should always >= target[me_len-1]
        assert (me_len == 0 || me_key_ >= target[me_len - 1]);
        if (N::isLeaf (me_)) {
            if (target.getKeyLen () == me_len) {
                // tree_ has target and we find it, return directly.
                // See (1)
                Next ();
                return;
            } else if (target.getKeyLen () > me_len) {
                // tree_'s key string from root_ to me_ is a prefix of target. We should go to
                // next key string See (2)
                Next ();
                int cmp = 0;
                Key seekKey;
                bool ret = GetKey (seekKey);
                if (ret) {
                    int seekKeyLen = seekKey.getKeyLen ();
                    int targetLen = target.getKeyLen ();
                    int cmp = memcmp (seekKey.getData (), target.getData (),
                                      std::min (seekKeyLen, targetLen));
                    if (cmp < 0 || targetLen > seekKeyLen) {
                        Next ();
                    }
                }
                return;
            } else {
                assert (false);
            }
        }

        assert (!N::isLeaf (me_));
        // Check me_'s value and me_'s prefix with target
        int me_key_vs_target = 0;
        if (me_ != root_) {
            assert (target.getKeyLen () >= me_len);
            if (me_key_ < target[me_len - 1]) {
                me_key_vs_target = -1;
            } else if (me_key_ > target[me_len - 1]) {
                me_key_vs_target = 1;
            }
        }
        int me_prefix_vs_target = 0;
        if (!N::isLeaf (me_)) me_prefix_vs_target = prefixComparetor (me_);

        // Try to go to next level
        if (me_key_vs_target > 0) {
            // See (3)
            Next ();
            return;
        } else if (me_key_vs_target == 0) {
            if (me_prefix_vs_target > 0) {
                // See (4)
                Next ();
                return;
            } else if (me_prefix_vs_target == 0) {
                // Both me_key_ and me_prefix are same, go to next level

                // Visit an inner node, push me_ to stack.
                node_stack_.push_back ({parent_, ci_, me_});

                // Seek the child of me
                auto [child, child_key, ith] = N::seekChild (me_, target[me_len++]);
                parent_ = me_;
                ci_ = ith;
                me_ = child;
                me_key_ = child_key;
            } else {
                // See (5)
                // Obtain the peer node of me_ and prepare for next visit
                auto [ith_child, key] = N::getIthChild (parent_, ++ci_);
                me_ = ith_child;
                me_key_ = key;
                Next ();
                return;
            }
        } else {
            // me_key_ should always >= target[me_len-1]
            assert (false);
        }
    }
}

/**
 * @brief Next is a in-order traveral of the trie. Each call of this function will make it stop at
 *        next leaf node.
 *
 * @return true  Successfully go to next leaf node.
 * @return false No leaf node left.
 */
bool Tree::Iterator::Next () {
    // 1. When me_ is an inner node (including root_), push [parent, ci, me_] to stack
    // 2. When me_ is an leaf node, not push me_ to stack. Instead, we find the peer node of me_
    // 3. When there is no peer node, me_ becomes nullptr

    while (me_ || node_stack_.size () != 0) {
        if (me_) {
            // me_ is an leaf node
            if (N::isLeaf (me_)) {
                // Visit leaf node, set current leaf to me_, set length
                // printf ("visit leafnode 0x%lx\n ", me_);
                cur_leaf_ = me_;

                // Obtain the peer node of me_ and prepare for next visit
                ci_++;
                auto [ith_child, key] = N::getIthChild (parent_, ci_);
                me_ = ith_child;
                me_key_ = key;
                return true;
            }
            // me_ is an inner node
            else {
                // Visit inner node
                // printf ("put parent: 0x%lx, ci: %d, me: 0x%lx. prefix (%u): \n", parent_, ci_,
                // me_,  me_->getPrefixLength ());

                // Push me_ to stack. This is the first time you visit me_.
                node_stack_.push_back ({parent_, ci_, me_});

                // Obtain the 0th child of me_ and prepare for next visit
                // Normally, an inner node's 0th child always valid. The only situation in which
                // me_'s 0th child is nullptr is that we have an empty trie, and the root's 0th
                // child is empty.
                parent_ = me_;
                ci_ = 0;
                auto [ith_child, key] = N::getIthChild (parent_, ci_);
                me_ = ith_child;
                me_key_ = key;
            }
        } else {
            // If me_ have no peer node, me_ becomes nullptr. This means we have visited all the
            // children of me_'s parent.

            // Pop out me_'s parent from stack.
            auto [p, i, n] = node_stack_.back ();
            assert (n == parent_);
            node_stack_.pop_back ();

            // Remove subfix from key_buf_. The parent_ node (its prefix and value)
            assert (!N::isLeaf (n));

            // Obtain the peer node of parent_ and prepare for next visit
            auto [ith_child, key] = N::getIthChild (p, i + 1);
            parent_ = p;
            ci_ = i + 1;
            me_ = ith_child;
            me_key_ = key;
        }
    }
    return false;
}

bool Tree::Iterator::GetKey (Key& key) { return tree_->loadKey (GetTID (), key); }

TID Tree::Iterator::GetTID () { return N::getLeaf (cur_leaf_); }

}  // namespace ART_OLC_ORI
