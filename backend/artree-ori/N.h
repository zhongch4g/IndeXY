//
// Created by florian on 05.08.15.
//

#ifndef ART_OPTIMISTIC_LOCK_COUPLING_N_H
#define ART_OPTIMISTIC_LOCK_COUPLING_N_H
// #define ART_NOREADLOCK
// #define ART_NOWRITELOCK
#include <stdint.h>
#include <string.h>

#include <atomic>

#include "artree-shared-headers/Epoche.h"
#include "artree-shared-headers/Key.h"

using namespace ART;
namespace ART_OLC_ORI {
/*
 * SynchronizedTree
 * LockCouplingTree
 * LockCheckFreeReadTree
 * UnsynchronizedTree
 */

enum class NTypes : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3 };

static constexpr uint32_t maxStoredPrefixLength = 11;

using Prefix = uint8_t[maxStoredPrefixLength];

// Type Version Lock Obsolete
// for easy gdb
// usage: p/x (struct ART_OLC::TVLO_t) node.typeVersionLockObsolete
struct TVLO_t {
    struct detail_t {
        uint64_t obsolete_ : 1;
        uint64_t lock_ : 1;
        uint64_t version_ : 60;
        uint64_t type_ : 2;
    };

    union {
        detail_t detail_;
        uint64_t asUint64_;
    };
};

class N {
protected:
    N (NTypes type, const uint8_t* prefix, uint32_t prefixLength) {
        setType (type);
        setPrefix (prefix, prefixLength);
    }

    N (const N&) = delete;

    N (N&&) = delete;

    // 2b type 60b version 1b lock 1b obsolete
    std::atomic<uint64_t> typeVersionLockObsolete{0b100};
    // version 1, unlocked, not obsolete
    uint16_t prefixCount = 0;

    uint16_t count = 0;
    Prefix prefix;

    void setType (NTypes type);

    static uint64_t convertTypeToVersion (NTypes type);

public:
    struct Stat {
        // accumulated numbers
        uint32_t nleafs_ = 0;
        uint32_t n4_ = 0;
        uint32_t n16_ = 0;
        uint32_t n48_ = 0;
        uint32_t n256_ = 0;

        // temp variables
        uint32_t height_ = 0;
    };

    NTypes getType () const;

    uint32_t getCount () const;

    static bool isLocked (uint64_t version);

    void writeLockOrRestart (bool& needRestart);

    void upgradeToWriteLockOrRestart (uint64_t& version, bool& needRestart);

    void writeUnlock ();

    uint64_t readLockOrRestart (bool& needRestart) const;

    /**
     * returns true if node hasn't been changed in between
     */
    void checkOrRestart (uint64_t startRead, bool& needRestart) const;
    void readUnlockOrRestart (uint64_t startRead, bool& needRestart) const;

    static bool isObsolete (uint64_t version);
    static bool isLockedOrObsolete (uint64_t version);

    /**
     * can only be called when node is locked
     */
    void writeUnlockObsolete () { typeVersionLockObsolete.fetch_add (0b11); }

    static N* getChild (const uint8_t k, const N* node);

    static void insertAndUnlock (N* node, uint64_t v, N* parentNode, uint64_t parentVersion,
                                 uint8_t keyParent, uint8_t key, N* val, bool& needRestart,
                                 ThreadInfo& threadInfo);

    static bool change (N* node, uint8_t key, N* val);

    static void removeAndUnlock (N* node, uint64_t v, uint8_t key, N* parentNode,
                                 uint64_t parentVersion, uint8_t keyParent, bool& needRestart,
                                 ThreadInfo& threadInfo);

    bool hasPrefix () const;

    const uint8_t* getPrefix () const;

    void setPrefix (const uint8_t* prefix, uint32_t length);

    void addPrefixBefore (N* node, uint8_t key);

    uint32_t getPrefixLength () const;

    static TID getLeaf (const N* n);

    static bool isLeaf (const N* n);

    static N* setLeaf (TID tid);

    static bool isHybrid (const N* n);

    static N* setHybrid (N*);

    static bool isAllLeaf (N*);

    static std::tuple<N*, uint8_t> getRandomChildNonLeaf (N*);

    static N* getAnyChild (const N* n);

    static TID getAnyChildTid (const N* n, bool& needRestart);

    static void deleteChildren (N* node, void (*callback) (TID ridlist));

    static void deleteNode (N* node, void (*callback) (TID ridlist));

    static std::tuple<N*, uint8_t> getSecondChild (N* node, const uint8_t k);

    template <typename curN, typename biggerN>
    static void insertGrow (curN* n, uint64_t v, N* parentNode, uint64_t parentVersion,
                            uint8_t keyParent, uint8_t key, N* val, bool& needRestart,
                            ThreadInfo& threadInfo);

    template <typename curN, typename smallerN>
    static void removeAndShrink (curN* n, uint64_t v, N* parentNode, uint64_t parentVersion,
                                 uint8_t keyParent, uint8_t key, bool& needRestart,
                                 ThreadInfo& threadInfo);

    static uint64_t getChildren (const N* node, uint8_t start, uint8_t end,
                                 std::tuple<uint8_t, N*> children[], uint32_t& childrenCount,
                                 bool& needRestart);

    template <typename Fn>
    static bool DeepVisit (N* node, Stat& stat, Fn&& callback);

    // For Iterator
    // return [child, child_key]
    static std::tuple<N*, uint8_t> getIthChild (N* node, int i);

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    static std::tuple<N*, uint8_t, uint8_t> seekChild (N* node, uint8_t key);
};

static_assert (sizeof (N) == 24, "N is not 24 bytes");

class N4 : public N {
public:
    uint8_t keys[4];
    N* children[4] = {nullptr, nullptr, nullptr, nullptr};

public:
    N4 (const uint8_t* prefix, uint32_t prefixLength) : N (NTypes::N4, prefix, prefixLength) {}

    void insert (uint8_t key, N* n);

    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* val);

    N* getChild (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf ();

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    std::tuple<N*, uint8_t> getSecondChild (const uint8_t key) const;

    void deleteChildren (void (*callback) (TID ridlist));

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    void DeepVisit (Stat& stat, Fn&& callback);

    // For Iterator
    // return [child, child_key]
    std::tuple<N*, uint8_t> getIthChild (int i) const;

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    std::tuple<N*, uint8_t, uint8_t> seekChild (uint8_t key) const;
};

class N16 : public N {
public:
    uint8_t keys[16];
    N* children[16];

    static uint8_t flipSign (uint8_t keyByte) {
        // Flip the sign bit, enables signed SSE comparison of unsigned values, used by Node16
        return keyByte ^ 128;
    }

    static inline unsigned ctz (uint16_t x) {
        // Count trailing zeros, only defined for x>0
#ifdef __GNUC__
        return __builtin_ctz (x);
#else
        // Adapted from Hacker's Delight
        unsigned n = 1;
        if ((x & 0xFF) == 0) {
            n += 8;
            x = x >> 8;
        }
        if ((x & 0x0F) == 0) {
            n += 4;
            x = x >> 4;
        }
        if ((x & 0x03) == 0) {
            n += 2;
            x = x >> 2;
        }
        return n - (x & 1);
#endif
    }

    N* const* getChildPos (const uint8_t k) const;

public:
    N16 (const uint8_t* prefix, uint32_t prefixLength) : N (NTypes::N16, prefix, prefixLength) {
        memset (keys, 0, sizeof (keys));
        memset (children, 0, sizeof (children));
    }

    void insert (uint8_t key, N* n);

    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* val);

    N* getChild (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf ();

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    void deleteChildren (void (*callback) (TID ridlist));

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    void DeepVisit (Stat& stat, Fn&& callback);

    // For Iterator
    // return [child, child_key]
    std::tuple<N*, uint8_t> getIthChild (int i) const;

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    std::tuple<N*, uint8_t, uint8_t> seekChild (uint8_t key) const;
};

class N48 : public N {
public:
    uint8_t childIndex[256];
    N* children[48];

public:
    static const uint8_t emptyMarker = 48;

    N48 (const uint8_t* prefix, uint32_t prefixLength) : N (NTypes::N48, prefix, prefixLength) {
        memset (childIndex, emptyMarker, sizeof (childIndex));
        memset (children, 0, sizeof (children));
    }

    void insert (uint8_t key, N* n);

    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* val);

    N* getChild (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf ();

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    void deleteChildren (void (*callback) (TID ridlist));

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    void DeepVisit (Stat& stat, Fn&& callback);

    // For Iterator
    // return [child, child_key]
    std::tuple<N*, uint8_t> getIthChild (int i) const;

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    std::tuple<N*, uint8_t, uint8_t> seekChild (uint8_t key) const;
};

class N256 : public N {
public:
    N* children[256];

public:
    N256 (const uint8_t* prefix, uint32_t prefixLength) : N (NTypes::N256, prefix, prefixLength) {
        memset (children, '\0', sizeof (children));
    }

    void insert (uint8_t key, N* val);

    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* n);

    N* getChild (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf ();

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    void deleteChildren (void (*callback) (TID ridlist));

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    void DeepVisit (Stat& stat, Fn&& callback);

    // For Iterator
    // return [child, child_key]
    std::tuple<N*, uint8_t> getIthChild (int i) const;

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    std::tuple<N*, uint8_t, uint8_t> seekChild (uint8_t key) const;
};
}  // namespace ART_OLC_ORI
#endif  // ART_OPTIMISTIC_LOCK_COUPLING_N_H
