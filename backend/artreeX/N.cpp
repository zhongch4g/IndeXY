#include "N.h"

#include <assert.h>

#include <algorithm>

#include "N16.cpp"
#include "N256.cpp"
#include "N4.cpp"
#include "N48.cpp"
#include "artree-shared-headers/xoshiro256.h"

namespace ART_OLC_X {

void N::setType (NTypes type) { typeVersionLockObsolete.fetch_add (convertTypeToVersion (type)); }

uint64_t N::convertTypeToVersion (NTypes type) { return (static_cast<uint64_t> (type) << 62); }

NTypes N::getType () const {
    return static_cast<NTypes> (typeVersionLockObsolete.load (std::memory_order_relaxed) >> 62);
}

void N::writeLockOrRestart (bool& needRestart) {
    uint64_t version;
    version = readLockOrRestart (needRestart);
    if (needRestart) return;

    upgradeToWriteLockOrRestart (version, needRestart);
    if (needRestart) return;
}

void N::upgradeToWriteLockOrRestart (uint64_t& version, bool& needRestart) {
    if (typeVersionLockObsolete.compare_exchange_strong (version, version + 0b10)) {
        version = version + 0b10;
    } else {
        needRestart = true;
    }
}

void N::writeUnlock () { typeVersionLockObsolete.fetch_add (0b10); }

bool N::isAllLeaf (N* node) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<const N4*> (node);
            return n->isAllLeaf ();
        }
        case NTypes::N16: {
            auto n = static_cast<const N16*> (node);
            return n->isAllLeaf ();
        }
        case NTypes::N48: {
            auto n = static_cast<const N48*> (node);
            return n->isAllLeaf ();
        }
        case NTypes::N256: {
            auto n = static_cast<const N256*> (node);
            return n->isAllLeaf ();
        }
    }
    assert (false);
    __builtin_unreachable ();
}

std::tuple<N*, uint8_t> N::getRandomChildNonLeaf (N* node, bool isRandom) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            return n->getRandomChildNonLeaf (isRandom);
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            return n->getRandomChildNonLeaf (isRandom);
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            return n->getRandomChildNonLeaf (isRandom);
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            return n->getRandomChildNonLeaf (isRandom);
        }
    }
    assert (false);
    __builtin_unreachable ();
}

N* N::getAnyChild (const N* node) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<const N4*> (node);
            return n->getAnyChild ();
        }
        case NTypes::N16: {
            auto n = static_cast<const N16*> (node);
            return n->getAnyChild ();
        }
        case NTypes::N48: {
            auto n = static_cast<const N48*> (node);
            return n->getAnyChild ();
        }
        case NTypes::N256: {
            auto n = static_cast<const N256*> (node);
            return n->getAnyChild ();
        }
    }
    assert (false);
    __builtin_unreachable ();
}

bool N::change (N* node, uint8_t key, N* val) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            return n->change (key, val);
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            return n->change (key, val);
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            return n->change (key, val);
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            return n->change (key, val);
        }
    }
    assert (false);
    __builtin_unreachable ();
}

#ifdef UNIQUENESS_CHECK
bool N::setHybridBits (N* node, uint8_t key) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            return n->setHybridBits (key);
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            return n->setHybridBits (key);
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            return n->setHybridBits (key);
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            return n->setHybridBits (key);
        }
    }
    assert (false);
    __builtin_unreachable ();
}
#endif

template <typename curN, typename biggerN>
void N::insertGrow (curN* n, uint64_t nodeState, uint64_t v, N* parentNode, uint64_t parentVersion,
                    uint8_t keyParent, uint8_t key, N* val, bool& needRestart,
                    ThreadInfo& threadInfo, GlobalMemoryInfo* memoryInfo) {
    auto thread_id = std::hash<std::thread::id> () (std::this_thread::get_id ());
    if (!n->isFull ()) {
        if (parentNode != nullptr) {
            parentNode->readUnlockOrRestart (parentVersion, needRestart);
            if (needRestart) return;
        }
        n->upgradeToWriteLockOrRestart (v, needRestart);
        if (needRestart) return;
        n->insert (key, val);
        n->writeUnlock ();
        return;
    }

    parentNode->upgradeToWriteLockOrRestart (parentVersion, needRestart);
    if (needRestart) return;

    n->upgradeToWriteLockOrRestart (v, needRestart);
    if (needRestart) {
        parentNode->writeUnlock ();
        return;
    }

    auto nBig = new biggerN (n->getPrefix (), n->getPrefixLength ());
    memoryInfo->increaseByBytes (thread_id, sizeof (biggerN));
    n->copyTo (nBig);
    nBig->insert (key, val);
    N::change (parentNode, keyParent, reinterpret_cast<N*> ((uint64_t)(nBig) | nodeState));

    if (n->candidate) {
        n->candidate->node = nBig;
        n->candidate->density = 100.0;
        n->candidate = nullptr;
    }

    n->writeUnlockObsolete ();
    threadInfo.getEpoche ().markNodeForDeletion (n, threadInfo);

    memoryInfo->decreaseByBytes (thread_id, sizeof (curN));
    parentNode->writeUnlock ();
}

void N::insertAndUnlock (N* node, uint64_t nodeState, uint64_t v, N* parentNode,
                         uint64_t parentVersion, uint8_t keyParent, uint8_t key, N* val,
                         bool& needRestart, ThreadInfo& threadInfo, GlobalMemoryInfo* memoryInfo) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            insertGrow<N4, N16> (n, nodeState, v, parentNode, parentVersion, keyParent, key, val,
                                 needRestart, threadInfo, memoryInfo);
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            insertGrow<N16, N48> (n, nodeState, v, parentNode, parentVersion, keyParent, key, val,
                                  needRestart, threadInfo, memoryInfo);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            insertGrow<N48, N256> (n, nodeState, v, parentNode, parentVersion, keyParent, key, val,
                                   needRestart, threadInfo, memoryInfo);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            insertGrow<N256, N256> (n, nodeState, v, parentNode, parentVersion, keyParent, key, val,
                                    needRestart, threadInfo, memoryInfo);
            break;
        }
    }
}

inline N* N::getChild (const uint8_t k, const N* node) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<const N4*> (node);
            return n->getChild (k);
        }
        case NTypes::N16: {
            auto n = static_cast<const N16*> (node);
            return n->getChild (k);
        }
        case NTypes::N48: {
            auto n = static_cast<const N48*> (node);
            return n->getChild (k);
        }
        case NTypes::N256: {
            auto n = static_cast<const N256*> (node);
            return n->getChild (k);
        }
    }
    assert (false);
    __builtin_unreachable ();
}

inline bool N::isHybridStat (const uint8_t k, const N* node) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<const N4*> (node);
            return n->isHybridStat (k);
        }
        case NTypes::N16: {
            auto n = static_cast<const N16*> (node);
            return n->isHybridStat (k);
        }
        case NTypes::N48: {
            auto n = static_cast<const N48*> (node);
            return n->isHybridStat (k);
        }
        case NTypes::N256: {
            auto n = static_cast<const N256*> (node);
            return n->isHybridStat (k);
        }
    }
    assert (false);
    __builtin_unreachable ();
}

#ifdef UNIQUENESS_CHECK
inline bool N::isClearStat (const N* node) { return node->hybridBitmap.isClear (); }
#endif

void N::deleteChildren (N* node, std::function<void (TID)> callback) {
    if (N::isLeaf (node)) {
        // leave this leaf node, deleteNode() will handle it.
        return;
    }
    if (node == nullptr) {
        // TODO: Handle subtree after released
        return;
    }
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            n->deleteChildren (callback);
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            n->deleteChildren (callback);
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            n->deleteChildren (callback);
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            n->deleteChildren (callback);
            return;
        }
    }
    assert (false);
    __builtin_unreachable ();
}

template <typename curN, typename smallerN>
void N::removeAndShrink (curN* n, uint64_t v, N* parentNode, uint64_t parentVersion,
                         uint8_t keyParent, uint8_t key, bool& needRestart,
                         ThreadInfo& threadInfo) {
    if (!n->isUnderfull () || parentNode == nullptr) {
        if (parentNode != nullptr) {
            parentNode->readUnlockOrRestart (parentVersion, needRestart);
            if (needRestart) return;
        }
        n->upgradeToWriteLockOrRestart (v, needRestart);
        if (needRestart) return;

        n->remove (key);
        n->writeUnlock ();
        return;
    }
    parentNode->upgradeToWriteLockOrRestart (parentVersion, needRestart);
    if (needRestart) return;

    n->upgradeToWriteLockOrRestart (v, needRestart);
    if (needRestart) {
        parentNode->writeUnlock ();
        return;
    }

    auto nSmall = new smallerN (n->getPrefix (), n->getPrefixLength ());

    n->copyTo (nSmall);
    nSmall->remove (key);
    N::change (parentNode, keyParent, nSmall);

    n->writeUnlockObsolete ();
    threadInfo.getEpoche ().markNodeForDeletion (n, threadInfo);
    parentNode->writeUnlock ();
}

void N::removeAndUnlock (N* node, uint64_t v, uint8_t key, N* parentNode, uint64_t parentVersion,
                         uint8_t keyParent, bool& needRestart, ThreadInfo& threadInfo) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            removeAndShrink<N4, N4> (n, v, parentNode, parentVersion, keyParent, key, needRestart,
                                     threadInfo);
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            removeAndShrink<N16, N4> (n, v, parentNode, parentVersion, keyParent, key, needRestart,
                                      threadInfo);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            removeAndShrink<N48, N16> (n, v, parentNode, parentVersion, keyParent, key, needRestart,
                                       threadInfo);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            removeAndShrink<N256, N48> (n, v, parentNode, parentVersion, keyParent, key,
                                        needRestart, threadInfo);
            break;
        }
    }
}

bool N::isLocked (uint64_t version) { return ((version & 0b10) == 0b10); }

uint64_t N::readLockOrRestart (bool& needRestart) const {
    uint64_t version;
    version = typeVersionLockObsolete.load ();
    /*        do {
                version = typeVersionLockObsolete.load();
            } while (isLocked(version));*/
    if (isLockedOrObsolete (version)) {
        needRestart = true;
    }
    return version;
    // uint64_t version;
    // while (isLocked(version)) _mm_pause();
    // return version;
}

bool N::isObsolete (uint64_t version) { return (version & 1) == 1; }
bool N::isLockedOrObsolete (uint64_t version) { return version & 0b11; }

void N::checkOrRestart (uint64_t startRead, bool& needRestart) const {
    readUnlockOrRestart (startRead, needRestart);
}

void N::readUnlockOrRestart (uint64_t startRead, bool& needRestart) const {
    needRestart = (startRead != typeVersionLockObsolete.load ());
}

uint32_t N::getPrefixLength () const { return prefixCount; }

bool N::hasPrefix () const { return prefixCount > 0; }

uint32_t N::getCount () const { return count; }

const uint8_t* N::getPrefix () const { return prefix; }

void N::setPrefix (const uint8_t* prefix, uint32_t length) {
    if (length > 0) {
        memcpy (this->prefix, prefix, std::min (length, maxStoredPrefixLength));
        prefixCount = length;
    } else {
        prefixCount = 0;
    }
}
uint8_t* N::getPrefix () { return prefix; }

void N::addPrefixBefore (N* node, uint8_t key) {
    uint32_t prefixCopyCount = std::min (maxStoredPrefixLength, node->getPrefixLength () + 1);
    memmove (this->prefix + prefixCopyCount, this->prefix,
             std::min (this->getPrefixLength (), maxStoredPrefixLength - prefixCopyCount));
    memcpy (this->prefix, node->prefix, std::min (prefixCopyCount, node->getPrefixLength ()));
    if (node->getPrefixLength () < maxStoredPrefixLength) {
        this->prefix[prefixCopyCount - 1] = key;
    }
    this->prefixCount += node->getPrefixLength () + 1;
}

/**

    isLeaf | isHybird | isMigrating
(1)    1         0          0      : normal leaf node
       1         0          1      : impossible
(2)    1         1          0      : hybrid leaf node
(3)    1         1          1      : hybrid leaf node in migration (db -> artree)
(4)    0         0          0      : inner node
       0         0          1      : impossible
       0         1          0      : impossible
(5)    0         1          1      : hybrid node in migration (db -> artree)

offloading: (4) -> (2)
migrating:  (2) -> (3) -> (5) -> (4) or
            (2) -> (3) -> (1)

    63       62       61        60          59
  isLeaf isRidList isHybrid isMigrating   isDirty
 */
bool N::isLeaf (const N* n) {
    return (reinterpret_cast<uint64_t> (n) & (static_cast<uint64_t> (1) << 63)) ==
           (static_cast<uint64_t> (1) << 63);
}

bool N::isHybrid (const N* n) {
    return (reinterpret_cast<uint64_t> (n) & (static_cast<uint64_t> (1) << 61)) ==
           (static_cast<uint64_t> (1) << 61);
}

bool N::isMigrating (const N* n) {
    return (reinterpret_cast<uint64_t> (n) & (static_cast<uint64_t> (1) << 60)) ==
           (static_cast<uint64_t> (1) << 60);
}

bool N::isDirty (const N* n) {
    return (reinterpret_cast<uint64_t> (n) & (static_cast<uint64_t> (1) << 59)) ==
           (static_cast<uint64_t> (1) << 59);
}

N* N::setLeaf (TID tid) { return reinterpret_cast<N*> (tid | (static_cast<uint64_t> (1) << 63)); }

N* N::setHybrid (N* n) {
    n = reinterpret_cast<N*> (reinterpret_cast<uint64_t> (n) | (static_cast<uint64_t> (1) << 61));
    return n;
}

N* N::setMigrating (N* n) {
    n = reinterpret_cast<N*> (reinterpret_cast<uint64_t> (n) | (static_cast<uint64_t> (1) << 60));
    return n;
}

N* N::setDirty (N* n) {
    n = reinterpret_cast<N*> (reinterpret_cast<uint64_t> (n) | (static_cast<uint64_t> (1) << 59));
    return n;
}

// get the state of node, hybird, migrating ...
uint64_t N::getNodeState (N* n) {
    return reinterpret_cast<uint64_t> (n) & (0x3000'0000'0000'0000L);
}
// get the state of node dirty bit...
uint64_t N::getDirtyState (N* n) {
    return reinterpret_cast<uint64_t> (n) & (0x0800'0000'0000'0000L);
}

// filtering the stat apart from 52 bit(is Ridlist)
TID N::getLeaf (const N* n) {
    // 0b0011100000000000... -> 0b1100011111111111...
    static constexpr uint64_t kHybridMask = ~(static_cast<uint64_t> (7) << 59);
    // 0b0100011111111111...
    return (reinterpret_cast<uint64_t> (n) & ((static_cast<uint64_t> (1) << 63) - 1) & kHybridMask);
}

std::tuple<N*, uint8_t> N::getSecondChild (N* node, const uint8_t key) {
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            return n->getSecondChild (key);
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }
}

void N::deleteNode (N* node, std::function<void (TID)> callback) {
    if (N::isLeaf (node)) {
        auto tidlist = N::getLeaf (node);
        callback (tidlist);
        return;
    }
    if (node == nullptr) {
        // TODO: Handle subtree after released
        return;
    }
    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<N4*> (node);
            delete n;
            return;
        }
        case NTypes::N16: {
            auto n = static_cast<N16*> (node);
            delete n;
            return;
        }
        case NTypes::N48: {
            auto n = static_cast<N48*> (node);
            delete n;
            return;
        }
        case NTypes::N256: {
            auto n = static_cast<N256*> (node);
            delete n;
            return;
        }
    }
    delete node;
}

TID N::getAnyChildTid (const N* n, bool& needRestart) {
    // TBD: should not return a hybrid leafnode's tid
    const N* nextNode = n;

    while (true) {
        const N* node = nextNode;
        auto v = node->readLockOrRestart (needRestart);
        if (needRestart) return 0;

        nextNode = getAnyChild (node);
        node->readUnlockOrRestart (v, needRestart);
        if (needRestart) return 0;

        assert (nextNode != nullptr);
        if (isLeaf (nextNode)) {
            return getLeaf (nextNode);
        }
    }
}

uint64_t N::getChildren (const N* node, uint8_t start, uint8_t end,
                         std::tuple<uint8_t, N*> children[], uint32_t& childrenCount,
                         bool& needRestart) {
restart:
    uint64_t v;
    needRestart = false;
    v = node->readLockOrRestart (needRestart);

    // we can do a simple handling by return it immediately does not matter what's the reason for
    // restarting. Following author's intention let's try to handle locally if possible. However,
    // obsolete version won't help in local restart, we have to return and get a higher level
    // restart.
    //
    if (needRestart) {
        if (node->isLocked (v)) goto restart;
        assert (isObsolete (v));
        return v;
    }

    switch (node->getType ()) {
        case NTypes::N4: {
            auto n = static_cast<const N4*> (node);
            n->getChildren (start, end, children, childrenCount);
            break;
        }
        case NTypes::N16: {
            auto n = static_cast<const N16*> (node);
            n->getChildren (start, end, children, childrenCount);
            break;
        }
        case NTypes::N48: {
            auto n = static_cast<const N48*> (node);
            n->getChildren (start, end, children, childrenCount);
            break;
        }
        case NTypes::N256: {
            auto n = static_cast<const N256*> (node);
            n->getChildren (start, end, children, childrenCount);
            break;
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }

    node->readUnlockOrRestart (v, needRestart);
    if (needRestart) goto restart;
    return v;
}

template <typename Fn>
size_t N::DeepVisit (N* node, Stat& stat, Fn&& callback, std::string prefix) {
    if (N::isLeaf (node)) {
        stat.nleafs_++;
        return 0;
    }
    if (node == nullptr) {
        return 0;
    }

    stat.height_++;

    size_t total_size = 0;
restart:
    uint64_t v;
    bool needRestart = false;
    v = node->readLockOrRestart (needRestart);

    if (needRestart) {
        if (node->isLocked (v)) goto restart;
        assert (isObsolete (v));
        return false;
    }

    switch (node->getType ()) {
        case NTypes::N4: {
            N4* n = static_cast<N4*> (node);
            stat.n4_++;
            total_size += n->DeepVisit (stat, callback, prefix);
            break;
        }
        case NTypes::N16: {
            N16* n = static_cast<N16*> (node);
            stat.n16_++;
            total_size += n->DeepVisit (stat, callback, prefix);
            break;
        }
        case NTypes::N48: {
            N48* n = static_cast<N48*> (node);
            stat.n48_++;
            total_size += n->DeepVisit (stat, callback, prefix);
            break;
        }
        case NTypes::N256: {
            N256* n = static_cast<N256*> (node);
            stat.n256_++;
            total_size += n->DeepVisit (stat, callback, prefix);
            break;
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }

    node->readUnlockOrRestart (v, needRestart);
    if (needRestart) goto restart;

    stat.height_--;
    return total_size;
}

template <typename Fn>
size_t N::DeepVisitRelease (N* node, Stat& stat, Fn&& callback, uint64_t action) {
    if (N::isLeaf (node) || (node == nullptr)) {
        stat.nleafs_++;
        return 0;
    }
    stat.height_++;

    size_t total_size = 0;
    switch (node->getType ()) {
        case NTypes::N4: {
            N4* n = static_cast<N4*> (node);
            stat.n4_++;
            total_size += n->DeepVisitRelease (stat, callback, action);
            break;
        }
        case NTypes::N16: {
            N16* n = static_cast<N16*> (node);
            stat.n16_++;
            total_size += n->DeepVisitRelease (stat, callback, action);
            break;
        }
        case NTypes::N48: {
            N48* n = static_cast<N48*> (node);
            stat.n48_++;
            total_size += n->DeepVisitRelease (stat, callback, action);
            break;
        }
        case NTypes::N256: {
            N256* n = static_cast<N256*> (node);
            stat.n256_++;
            total_size += n->DeepVisitRelease (stat, callback, action);
            break;
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }
    stat.height_--;
    return total_size;
}

// For Iterator
std::tuple<N*, uint8_t> N::getIthChild (N* node, int i) {
    if (!node) return {nullptr, 0};
    if (N::isLeaf (node)) {
        return {nullptr, 0};
    }

    switch (node->getType ()) {
        case NTypes::N4: {
            N4* n = static_cast<N4*> (node);
            return n->getIthChild (i);
        }
        case NTypes::N16: {
            N16* n = static_cast<N16*> (node);
            return n->getIthChild (i);
        }
        case NTypes::N48: {
            N48* n = static_cast<N48*> (node);
            return n->getIthChild (i);
        }
        case NTypes::N256: {
            N256* n = static_cast<N256*> (node);
            return n->getIthChild (i);
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }

    assert (false);
    __builtin_unreachable ();
    return {nullptr, 0};
}

// find node's first child that have leaf node >= val
// return [child, child_key, ith]
std::tuple<N*, uint8_t, uint8_t> N::seekChild (N* node, uint8_t key) {
    if (!node) return {nullptr, 0, 0};
    if (N::isLeaf (node)) {
        return {nullptr, 0, 0};
    }

    switch (node->getType ()) {
        case NTypes::N4: {
            N4* n = static_cast<N4*> (node);
            return n->seekChild (key);
        }
        case NTypes::N16: {
            N16* n = static_cast<N16*> (node);
            return n->seekChild (key);
        }
        case NTypes::N48: {
            N48* n = static_cast<N48*> (node);
            return n->seekChild (key);
        }
        case NTypes::N256: {
            N256* n = static_cast<N256*> (node);
            return n->seekChild (key);
        }
        default: {
            assert (false);
            __builtin_unreachable ();
        }
    }

    assert (false);
    __builtin_unreachable ();
    return {nullptr, 0, 0};
}

}  // namespace ART_OLC_X