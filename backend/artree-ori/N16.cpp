#include <assert.h>
#include <emmintrin.h>  // x86 SSE intrinsics
#include <immintrin.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_ORI {

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

std::tuple<N*, uint8_t> N16::getRandomChildNonLeaf () {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = random () % count;
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
    memmove (keys + pos + 1, keys + pos, count - pos);
    memmove (children + pos + 1, children + pos, (count - pos) * sizeof (uintptr_t));
    keys[pos] = keyByteFlipped;
    children[pos] = n;
    count++;
}

template <class NODE>
void N16::copyTo (NODE* n) const {
    for (unsigned i = 0; i < count; i++) {
        n->insert (flipSign (keys[i]), children[i]);
    }
}

bool N16::change (uint8_t key, N* val) {
    N** childPos = const_cast<N**> (getChildPos (key));
    assert (childPos != nullptr);
    *childPos = val;
    return true;
}

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

void N16::deleteChildren (void (*callback) (TID ridlist)) {
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
void N16::DeepVisit (Stat& stat, Fn&& callback) {
    for (int i = 0; i < count; ++i) {
        N* node = this->children[i];
        N::DeepVisit (node, stat, callback);
        callback (node);
    }
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

}  // namespace ART_OLC