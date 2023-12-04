//
// Created by florian on 05.08.15.
//

#ifndef ART_OPTIMISTIC_LOCK_COUPLING_N_X_H
#define ART_OPTIMISTIC_LOCK_COUPLING_N_X_H
// #define ART_NOREADLOCK
// #define ART_NOWRITELOCK
// #define ATOMIC_ACCESS_BIT

#include <stdint.h>
#include <string.h>

#include <atomic>
#include <mutex>

#include "ARTCounter.h"
#include "artree-shared-headers/Epoche.h"
#include "artree-shared-headers/Key.h"

using namespace ART;
namespace ART_OLC_X {
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

class N;
struct candidate_t {
    double density;
    N* node;
    std::string prefix;
    std::atomic<uint64_t> versionLockObsolete{0b100};
    candidate_t () : density (0), node (nullptr), prefix ("") {}
    candidate_t (double d, N* n, const std::string& k) {
        density = d;
        node = n;
        prefix = k;
    }

    candidate_t (const candidate_t& c) {
        density = c.density;
        node = c.node;
        prefix = c.prefix;
        versionLockObsolete.store (c.versionLockObsolete.load ());
    };

    candidate_t& operator= (const candidate_t& o) {
        density = o.density;
        node = o.node;
        prefix = o.prefix;
        versionLockObsolete.store (o.versionLockObsolete.load ());
        return *this;
    }

    bool operator< (const candidate_t& can) const { return density < can.density; }

    void writeLock (uint64_t& version, bool& needRestart) {
        if (versionLockObsolete.compare_exchange_strong (version, version + 0b10)) {
            version = version + 0b10;
        } else {
            needRestart = true;
        }
    }
    void writeUnlock () { versionLockObsolete.fetch_add (0b10); }
    bool isLockedOrObsolete (uint64_t version) { return version & 0b11; }

    uint64_t readLockOrRestart (bool& needRestart) {
        uint64_t version;
        version = versionLockObsolete.load ();
        if (isLockedOrObsolete (version)) {
            needRestart = true;
        }
        return version;
    }
    void checkOrRestart (uint64_t startRead, bool& needRestart) const {
        readUnlockOrRestart (startRead, needRestart);
    }

    void readUnlockOrRestart (uint64_t startRead, bool& needRestart) const {
        needRestart = (startRead != versionLockObsolete.load ());
    }
};

class N {
protected:
    N (NTypes type, const uint8_t* prefix, uint32_t prefixLength) {
        setType (type);
        setPrefix (prefix, prefixLength);
    }
    ~N () {}

    N (const N&) = delete;

    N (N&&) = delete;

public:
    // 2b type 60b version 1b lock 1b obsolete
    std::atomic<uint64_t> typeVersionLockObsolete{0b100};
    // version 1, unlocked, not obsolete
    uint16_t prefixCount = 0;

    Prefix prefix;

    void setType (NTypes type);

    static uint64_t convertTypeToVersion (NTypes type);

public:
    uint16_t count = 0;

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

    struct ClockStat {
        // will usually occupy 1 byte:
        // 1 bits: value of b1
        // 1 bits: value of b2

        uint8_t access_bit : 1;
        uint8_t unload_bit : 1;
        ClockStat (const ClockStat& c) = default;
    };

    struct HybridBitmap {
        uint64_t bitsSegment1_;
        uint64_t bitsSegment2_;
        uint64_t bitsSegment3_;
        uint64_t bitsSegment4_;
        bool isHybrid (uint64_t pos) const {
            if (pos < 64) {
                // pos < 64, add bitsSegment1_
                uint64_t location = static_cast<uint64_t> (1) << pos;
                return bitsSegment1_ & location;
            } else if (pos < 128) {
                // 128 > pos > 64, add bitsSegment2_
                uint64_t location = static_cast<uint64_t> (1) << (pos - 64);
                return bitsSegment2_ & location;
            } else if (pos < 192) {
                // 128< pos < 192, add bitsSegment3_
                uint64_t location = static_cast<uint64_t> (1) << (pos - 128);
                return bitsSegment3_ & location;
            } else if (pos < 256) {
                // 192 < pos < 256, add bitsSegment4_
                uint64_t location = static_cast<uint64_t> (1) << (pos - 192);
                return bitsSegment4_ & location;
            }
            assert (false);
            __builtin_unreachable ();
        }
        void setHybrid (uint64_t pos) {
            if (pos < 64) {
                // pos < 64, add bitsSegment1_
                uint64_t location = static_cast<uint64_t> (1) << pos;
                // assert (!(bitsSegment1_ & location));
                bitsSegment1_ |= location;
            } else if (pos < 128) {
                // 128 > pos > 64, add bitsSegment2_
                uint64_t location = static_cast<uint64_t> (1) << (pos - 64);
                // assert (!(bitsSegment2_ & location));
                bitsSegment2_ |= location;
            } else if (pos < 192) {
                // 128< pos < 192, add bitsSegment3_
                uint64_t location = static_cast<uint64_t> (1) << (pos - 128);
                // assert (!(bitsSegment3_ & location));
                bitsSegment3_ |= location;
            } else if (pos < 256) {
                // 192 < pos < 256, add bitsSegment4_
                uint64_t location = static_cast<uint64_t> (1) << (pos - 192);
                // assert (!(bitsSegment4_ & location));
                bitsSegment4_ |= location;
            }
        }

        void memmove (uint64_t pos, uint64_t move_len) {
            if (pos < 64) {
                // For N4
                uint64_t MASK = (static_cast<uint64_t> (1) << pos) - 1;
                uint64_t rMASK = ~MASK;
                bitsSegment1_ = ((bitsSegment1_ & rMASK) << 1) & (MASK);
            }
        }

        bool isClear () const {
            return (bitsSegment1_ + bitsSegment2_ + bitsSegment3_ + bitsSegment4_) == 0;
        }

        void toString () {
            std::string str;
            uint64_t i = 0;
            while (i < 64) {
                if (bitsSegment1_ & (static_cast<uint64_t> (1) << i)) {
                    str = " 1" + str;
                } else {
                    str = " 0" + str;
                }
                i++;
            }
            i = 0;
            while (i < 64) {
                if (bitsSegment2_ & (static_cast<uint64_t> (1) << i)) {
                    str = " 1" + str;
                } else {
                    str = " 0" + str;
                }
                i++;
            }
            i = 0;
            while (i < 64) {
                if (bitsSegment3_ & (static_cast<uint64_t> (1) << i)) {
                    str = " 1" + str;
                } else {
                    str = " 0" + str;
                }
                i++;
            }
            i = 0;
            while (i < 64) {
                if (bitsSegment4_ & (static_cast<uint64_t> (1) << i)) {
                    str = " 1" + str;
                } else {
                    str = " 0" + str;
                }
                i++;
            }
            printf ("%s", str.c_str ());
        }
    };

public:
    double subtree_size = 0;
#ifdef ATOMIC_ACCESS_BIT
    std::atomic<uint8_t> clockStat = 0b11;
#else
    uint8_t clockStat = 0b11;
#endif
    candidate_t* candidate = nullptr;
#ifdef UNIQUENESS_CHECK
    HybridBitmap hybridBitmap{0, 0, 0, 0};
#endif

public:
#ifdef ATOMIC_ACCESS_BIT
    // 1 bits: value of b1 1 bits: value of b2
    void setClockAccessBit () { clockStat.fetch_or (0b10); }
    void clearClockAccessBit () { clockStat.fetch_and (0b01); }
    void setClockUnloadBit () { clockStat.fetch_or (0b01); }
    void clearClockUnloadBit () { clockStat.fetch_and (0b10); }
    // 1 bits: value of b1 1 bits: value of b2
    bool isClockAccessBit () { return 0b10 & clockStat.load (); }
    bool isClockUnloadBit () { return 0b01 & clockStat.load (); }
#else
    // 1 bits: value of b1 1 bits: value of b2
    void setClockAccessBit () { clockStat |= (0b10); }
    void clearClockAccessBit () { clockStat &= (0b01); }
    void setClockUnloadBit () { clockStat |= (0b01); }
    void clearClockUnloadBit () { clockStat &= (0b10); }
    // 1 bits: value of b1 1 bits: value of b2
    bool isClockAccessBit () { return 0b10 & clockStat; }
    bool isClockUnloadBit () { return 0b01 & clockStat; }
#endif

public:
    NTypes getType () const;

    uint32_t getCount () const;
    uint32_t getCounts ();

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
    static bool isHybridStat (const uint8_t k, const N* node);
#ifdef UNIQUENESS_CHECK
    static bool isClearStat (const N* node);
#endif

    static void insertAndUnlock (N* node, uint64_t nodeState, uint64_t v, N* parentNode,
                                 uint64_t parentVersion, uint8_t keyParent, uint8_t key, N* val,
                                 bool& needRestart, ThreadInfo& threadInfo,
                                 GlobalMemoryInfo* memoryInfo);

    static bool change (N* node, uint8_t key, N* val);
#ifdef UNIQUENESS_CHECK
    static bool setHybridBits (N* node, uint8_t key);
#endif

    static void removeAndUnlock (N* node, uint64_t v, uint8_t key, N* parentNode,
                                 uint64_t parentVersion, uint8_t keyParent, bool& needRestart,
                                 ThreadInfo& threadInfo);

    bool hasPrefix () const;

    const uint8_t* getPrefix () const;
    uint8_t* getPrefix ();

    void setPrefix (const uint8_t* prefix, uint32_t length);

    void addPrefixBefore (N* node, uint8_t key);

    uint32_t getPrefixLength () const;

    static TID getLeaf (const N* n);

    static bool isLeaf (const N* n);

    static N* setLeaf (TID tid);

    static bool isHybrid (const N* n);

    static bool isMigrating (const N* n);

    static bool isDirty (const N* n);

    static N* setHybrid (N*);

    static N* setMigrating (N*);

    static N* setDirty (N*);

    static uint64_t getNodeState (N*);

    static uint64_t getDirtyState (N*);

    static bool isAllLeaf (N*);

    // randomly pick a node, or pick the left most node, whose children are all leafnodes
    static std::tuple<N*, uint8_t> getRandomChildNonLeaf (N*, bool /* random or not */);

    static N* getAnyChild (const N* n);

    static TID getAnyChildTid (const N* n, bool& needRestart);

    static void deleteChildren (N* node, std::function<void (TID)>);

    static void deleteNode (N* node, std::function<void (TID)>);

    static std::tuple<N*, uint8_t> getSecondChild (N* node, const uint8_t k);

    template <typename curN, typename biggerN>
    static void insertGrow (curN* n, uint64_t nodeState, uint64_t v, N* parentNode,
                            uint64_t parentVersion, uint8_t keyParent, uint8_t key, N* val,
                            bool& needRestart, ThreadInfo& threadInfo,
                            GlobalMemoryInfo* memoryInfo);

    template <typename curN, typename smallerN>
    static void removeAndShrink (curN* n, uint64_t v, N* parentNode, uint64_t parentVersion,
                                 uint8_t keyParent, uint8_t key, bool& needRestart,
                                 ThreadInfo& threadInfo);

    static uint64_t getChildren (const N* node, uint8_t start, uint8_t end,
                                 std::tuple<uint8_t, N*> children[], uint32_t& childrenCount,
                                 bool& needRestart);

    template <typename Fn>
    static size_t DeepVisit (N* node, Stat& stat, Fn&& callback, std::string prefix);

    template <typename Fn>
    static size_t DeepVisitRelease (N* node, Stat& stat, Fn&& callback, uint64_t action);

    // For Iterator
    // return [child, child_key]
    static std::tuple<N*, uint8_t> getIthChild (N* node, int i);

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    static std::tuple<N*, uint8_t, uint8_t> seekChild (N* node, uint8_t key);
};
static_assert (sizeof (N) == 48, "N is not 48 bytes");

class N4 : public N {
public:
    uint8_t keys[4];
    N* children[4] = {nullptr, nullptr, nullptr, nullptr};

public:
    N4 (const uint8_t* prefix, uint32_t prefixLength) : N (NTypes::N4, prefix, prefixLength) {}
    ~N4 () = default;

    void insert (uint8_t key, N* n);
#ifdef UNIQUENESS_CHECK
    void insertWithHybridStat (uint8_t key, N* n, bool isHybrid);
#endif
    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* val);
#ifdef UNIQUENESS_CHECK
    bool setHybridBits (uint8_t key);
#endif

    N* getChild (const uint8_t k) const;
    bool isHybridStat (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    void gatherChildsNKeys ();

    void gatherSubChildsNKeys (uint32_t nlevel);

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf (bool isRandom);

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    std::tuple<N*, uint8_t> getSecondChild (const uint8_t key) const;

    void deleteChildren (std::function<void (TID)>);

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    size_t DeepVisit (Stat& stat, Fn&& callback, std::string prefix);

    template <typename Fn>
    size_t DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action);

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
    ~N16 () = default;

    void insert (uint8_t key, N* n);
#ifdef UNIQUENESS_CHECK
    void insertWithHybridStat (uint8_t key, N* n, bool isHybrid);
#endif
    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* val);
#ifdef UNIQUENESS_CHECK
    bool setHybridBits (uint8_t key);
#endif

    N* getChild (const uint8_t k) const;
    bool isHybridStat (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    void gatherChildsNKeys ();

    void gatherSubChildsNKeys (uint32_t nlevel);

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf (bool isRandom);

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    void deleteChildren (std::function<void (TID)>);

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    size_t DeepVisit (Stat& stat, Fn&& callback, std::string prefix);

    template <typename Fn>
    size_t DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action);

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
    ~N48 () = default;

    void insert (uint8_t key, N* n);
#ifdef UNIQUENESS_CHECK
    void insertWithHybridStat (uint8_t key, N* n, bool isHybrid);
#endif
    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* val);
#ifdef UNIQUENESS_CHECK
    bool setHybridBits (uint8_t key);
#endif

    N* getChild (const uint8_t k) const;
    bool isHybridStat (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    void gatherChildsNKeys ();

    void gatherSubChildsNKeys (uint32_t nlevel);

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf (bool isRandom);

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    void deleteChildren (std::function<void (TID)>);

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    size_t DeepVisit (Stat& stat, Fn&& callback, std::string prefix);

    template <typename Fn>
    size_t DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action);

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

    ~N256 () = default;

    void insert (uint8_t key, N* val);
#ifdef UNIQUENESS_CHECK
    void insertWithHybridStat (uint8_t key, N* n, bool isHybrid);
#endif
    template <class NODE>
    void copyTo (NODE* n) const;

    bool change (uint8_t key, N* n);
#ifdef UNIQUENESS_CHECK
    bool setHybridBits (uint8_t key);
#endif

    N* getChild (const uint8_t k) const;
    bool isHybridStat (const uint8_t k) const;

    void remove (uint8_t k);

    N* getAnyChild () const;

    void gatherChildsNKeys ();

    void gatherSubChildsNKeys (uint32_t nlevel);

    N* getRandomChild () const;

    std::tuple<N*, uint8_t> getRandomChildNonLeaf (bool isRandom);

    bool isAllLeaf () const;

    bool isFull () const;

    bool isUnderfull () const;

    void deleteChildren (std::function<void (TID)>);

    uint64_t getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const;

    template <typename Fn>
    size_t DeepVisit (Stat& stat, Fn&& callback, std::string prefix);

    template <typename Fn>
    size_t DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action);

    // For Iterator
    // return [child, child_key]
    std::tuple<N*, uint8_t> getIthChild (int i) const;

    // find node's first child that have leaf node >= key
    // return [child, child_key, ith]
    std::tuple<N*, uint8_t, uint8_t> seekChild (uint8_t key) const;
};
}  // namespace ART_OLC_X
#endif  // ART_OPTIMISTIC_LOCK_COUPLING_N_H
