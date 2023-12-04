#include <assert.h>
#include <emmintrin.h>  // x86 SSE intrinsics
#include <immintrin.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_X {

bool N16::isAllLeaf () const {
    uint16_t mask = 0;
    for (int i = 0; i < 16; i += 4) {
        __m256d pointers = _mm256_loadu_pd (reinterpret_cast<const double*> (&(children[i])));
        // move most significant bit to maski
        int maski = _mm256_movemask_pd (pointers);
        mask = (mask << 4) | (maski & 0xF);
    }
    return __builtin_popcount (((1 << count) - 1) & mask) == count;
}

std::tuple<N*, uint8_t> N16::getRandomChildNonLeaf (bool isRandom) {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = isRandom ? random () % count : 0;
    for (int i = rnd_i; i < count; i++) {
        uint8_t key = flipSign (keys[i]);
        N** childPos = const_cast<N**> (getChildPos (key));
        if (childPos != nullptr && !N::isLeaf (*childPos)) return {*childPos, key};
    }
    for (int i = rnd_i - 1; i >= 0; i--) {
        uint8_t key = flipSign (keys[i]);
        N** childPos = const_cast<N**> (getChildPos (key));
        if (childPos != nullptr && !N::isLeaf (*childPos)) return {*childPos, key};
    }
    // means all children are leafnode
    return {emptyNode, 0};
}

bool N16::isFull () const { return count == 16; }

bool N16::isUnderfull () const { return count == 3; }

void N16::insert (uint8_t key, N* n) {
    uint8_t keyByteFlipped = flipSign (key);
    __m128i cmp = _mm_cmplt_epi8 (_mm_set1_epi8 (keyByteFlipped),
                                  _mm_loadu_si128 (reinterpret_cast<__m128i*> (keys)));
    uint16_t bitfield = _mm_movemask_epi8 (cmp) & (0xFFFF >> (16 - count));
    unsigned pos = bitfield ? ctz (bitfield) : count;
#ifdef UNIQUENESS_CHECK
    hybridBitmap.memmove (pos, 1);
#endif
    memmove (keys + pos + 1, keys + pos, count - pos);
    memmove (children + pos + 1, children + pos, (count - pos) * sizeof (uintptr_t));
    keys[pos] = keyByteFlipped;
    children[pos] = n;
    count++;
}
#ifdef UNIQUENESS_CHECK
void N16::insertWithHybridStat (uint8_t key, N* n, bool isHybrid) {
    uint8_t keyByteFlipped = flipSign (key);
    __m128i cmp = _mm_cmplt_epi8 (_mm_set1_epi8 (keyByteFlipped),
                                  _mm_loadu_si128 (reinterpret_cast<__m128i*> (keys)));
    uint16_t bitfield = _mm_movemask_epi8 (cmp) & (0xFFFF >> (16 - count));
    unsigned pos = bitfield ? ctz (bitfield) : count;
    hybridBitmap.memmove (pos, 1);
    memmove (keys + pos + 1, keys + pos, count - pos);
    memmove (children + pos + 1, children + pos, (count - pos) * sizeof (uintptr_t));
    keys[pos] = keyByteFlipped;
    children[pos] = n;
    if (isHybrid) hybridBitmap.setHybrid (pos);
    count++;
}
#endif
template <class NODE>
void N16::copyTo (NODE* n) const {
    for (unsigned i = 0; i < count; i++) {
#ifdef UNIQUENESS_CHECK
        auto isHybrid = hybridBitmap.isHybrid (i);
        n->insertWithHybridStat (flipSign (keys[i]), children[i], isHybrid);
#endif
        n->insert (flipSign (keys[i]), children[i]);
    }
    // Transfer node stat to the new node
    n->candidate = candidate;
#ifdef ATOMIC_ACCESS_BIT
    n->clockStat.store (clockStat.load ());
#else
    n->clockStat = clockStat;
#endif
}

bool N16::change (uint8_t key, N* val) {
    N** childPos = const_cast<N**> (getChildPos (key));
    assert (childPos != nullptr);
    *childPos = val;
    return true;
}

#ifdef UNIQUENESS_CHECK
bool N16::setHybridBits (uint8_t key) {
    __m128i cmp = _mm_cmpeq_epi8 (_mm_set1_epi8 (flipSign (key)),
                                  _mm_loadu_si128 (reinterpret_cast<const __m128i*> (keys)));
    unsigned bitfield = _mm_movemask_epi8 (cmp) & ((1 << count) - 1);
    uint32_t i = ctz (bitfield);
    hybridBitmap.setHybrid (i);
    return true;
}
#endif

N* const* N16::getChildPos (const uint8_t k) const {
    __m128i cmp = _mm_cmpeq_epi8 (_mm_set1_epi8 (flipSign (k)),
                                  _mm_loadu_si128 (reinterpret_cast<const __m128i*> (keys)));
    unsigned bitfield = _mm_movemask_epi8 (cmp) & ((1 << count) - 1);
    if (bitfield) {
        return &children[ctz (bitfield)];
    } else {
        return nullptr;
    }
}

N* N16::getChild (const uint8_t k) const {
    N* const* childPos = getChildPos (k);
    if (childPos == nullptr) {
        return nullptr;
    } else {
        return *childPos;
    }
}

#ifdef UNIQUENESS_CHECK
bool N16::isHybridStat (const uint8_t k) const {
    __m128i cmp = _mm_cmpeq_epi8 (_mm_set1_epi8 (flipSign (k)),
                                  _mm_loadu_si128 (reinterpret_cast<const __m128i*> (keys)));
    unsigned bitfield = _mm_movemask_epi8 (cmp) & ((1 << count) - 1);
    uint32_t i = ctz (bitfield);
    return hybridBitmap.isHybrid (i);
}
#endif

void N16::remove (uint8_t k) {
    N* const* leafPlace = getChildPos (k);
    assert (leafPlace != nullptr);
    std::size_t pos = leafPlace - children;
    memmove (keys + pos, keys + pos + 1, count - pos - 1);
    memmove (children + pos, children + pos + 1, (count - pos - 1) * sizeof (N*));
    count--;
    assert (getChild (k) == nullptr);
}

N* N16::getAnyChild () const {
    for (int i = 0; i < count; ++i) {
        if (N::isLeaf (children[i])) {
            return children[i];
        }
    }
    return children[0];
}

void N16::deleteChildren (std::function<void (TID)> callback) {
    for (std::size_t i = 0; i < count; ++i) {
        N::deleteChildren (children[i], callback);
        N::deleteNode (children[i], callback);
    }
}

uint64_t N16::getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                           uint32_t& childrenCount) const {
    childrenCount = 0;
    auto startPos = getChildPos (start);
    auto endPos = getChildPos (end);
    if (startPos == nullptr) {
        startPos = this->children;
    }
    if (endPos == nullptr) {
        endPos = this->children + (count - 1);
    }
    for (auto p = startPos; p <= endPos; ++p) {
        children[childrenCount] = std::make_tuple (flipSign (keys[p - this->children]), *p);
        childrenCount++;
    }
    return childrenCount;
}

template <typename Fn>
size_t N16::DeepVisit (Stat& stat, Fn&& callback, std::string prefix) {
    size_t total_size = 0;
    std::string new_prefix =
        prefix + (this->hasPrefix ()
                      ? std::string ((char*)this->prefix,
                                     std::min (this->getPrefixLength (), maxStoredPrefixLength))
                      : "");
    for (int i = 0; i < count; ++i) {
        N* node = this->children[i];
        // new_prefix.push_back (this->keys[i]);
        new_prefix.push_back (N16::flipSign (this->keys[i]));
        total_size += N::DeepVisit (node, stat, callback, new_prefix);
        callback (node, new_prefix);
        new_prefix.pop_back ();
    }
    this->subtree_size = total_size + sizeof (N16);
    return this->subtree_size;
}

template <typename Fn>
size_t N16::DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action) {
    size_t total_size = 0;

    for (int i = 0; i < count; ++i) {
        N* node = this->children[i];
        total_size += N::DeepVisitRelease (node, stat, callback, action);
        callback (node, "");
        if ((action == 1) && N::isLeaf (this->children[i])) {
            // clear the dirty bit
            if (N::isDirty (this->children[i])) {
                auto nodeStat = N::getNodeState (this->children[i]);
                this->children[i] = N::setLeaf (N::getLeaf (this->children[i]) | nodeStat);
            }
        }
        if ((action == 2) && !N::isLeaf (this->children[i])) {
            // free the space
            auto child = this->children[i];
            if (child == nullptr) {
                continue;
            }
            if (child->candidate != nullptr) {
                child->candidate->node = nullptr;
                child->candidate->density = 100.0;
                child->candidate = nullptr;
            }
            switch (child->getType ()) {
                case NTypes::N4: {
                    N4* n = static_cast<N4*> (child);
                    delete (n);
                    this->children[i] = nullptr;
                    break;
                }
                case NTypes::N16: {
                    N16* n = static_cast<N16*> (child);
                    delete (n);
                    this->children[i] = nullptr;
                    break;
                }
                case NTypes::N48: {
                    N48* n = static_cast<N48*> (child);
                    delete (n);
                    this->children[i] = nullptr;
                    break;
                }
                case NTypes::N256: {
                    N256* n = static_cast<N256*> (child);
                    delete (n);
                    this->children[i] = nullptr;
                    break;
                }
                default: {
                    assert (false);
                    __builtin_unreachable ();
                }
            }
        }
    }
    this->subtree_size = 0;
    if (action == 2) {
        this->subtree_size = total_size + sizeof (N16);
    }
    return this->subtree_size;
}

// For Iterator
// return [child, child_key]
std::tuple<N*, uint8_t> N16::getIthChild (int ith) const {
    if (ith >= count) return {nullptr, 0};
    return {children[ith], flipSign (keys[ith])};
}

// find node's first child that have leaf node >= key
// return [child, child_key, ith]
std::tuple<N*, uint8_t, uint8_t> N16::seekChild (uint8_t key) const {
    uint8_t keyByteFlipped = flipSign (key);
    __m128i cmp = _mm_cmpgt_epi8 (_mm_set1_epi8 (keyByteFlipped),
                                  _mm_loadu_si128 (reinterpret_cast<const __m128i*> (keys)));
    uint16_t bitfield = _mm_movemask_epi8 (cmp);
    bitfield = ~bitfield & (0xFFFF >> (16 - count));
    unsigned pos = bitfield ? ctz (bitfield) : count;
    if (pos == count) return {nullptr, 0, 0};
    return {children[pos], flipSign (keys[pos]), pos};
}

}  // namespace ART_OLC_X