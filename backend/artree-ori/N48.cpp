#include <assert.h>
#include <immintrin.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_ORI {

bool N48::isAllLeaf () const {
    for (uint32_t i = 0; i < count; ++i) {
        if (!N::isLeaf (children[i])) {
            return false;
        }
    }
    return true;
}

std::tuple<N*, uint8_t> N48::getRandomChildNonLeaf () {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = random () % 256;
    for (int key = rnd_i; key < 256; key++) {
        uint8_t pos = childIndex[key];
        if (pos == emptyMarker) continue;
        if (children[pos] != nullptr && !N::isLeaf (children[pos])) return {children[pos], key};
    }
    for (int key = rnd_i - 1; key >= 0; key--) {
        uint8_t pos = childIndex[key];
        if (pos == emptyMarker) continue;
        if (children[pos] != nullptr && !N::isLeaf (children[pos])) return {children[pos], key};
    }
    // means all children are leafnode
    return {emptyNode, 0};
}

bool N48::isFull () const { return count == 48; }

bool N48::isUnderfull () const { return count == 12; }

void N48::insert (uint8_t key, N* n) {
    unsigned pos = count;
    if (children[pos]) {
        for (pos = 0; children[pos] != nullptr; pos++)
            ;
    }
    children[pos] = n;
    childIndex[key] = (uint8_t)pos;
    count++;
}

template <class NODE>
void N48::copyTo (NODE* n) const {
    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            n->insert (i, children[childIndex[i]]);
        }
    }
}

bool N48::change (uint8_t key, N* val) {
    children[childIndex[key]] = val;
    return true;
}

N* N48::getChild (const uint8_t k) const {
    if (childIndex[k] == emptyMarker) {
        return nullptr;
    } else {
        return children[childIndex[k]];
    }
}

void N48::remove (uint8_t k) {
    assert (childIndex[k] != emptyMarker);
    children[childIndex[k]] = nullptr;
    childIndex[k] = emptyMarker;
    count--;
    assert (getChild (k) == nullptr);
}

N* N48::getAnyChild () const {
    N* anyChild = nullptr;
    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            if (N::isLeaf (children[childIndex[i]])) {
                return children[childIndex[i]];
            } else {
                anyChild = children[childIndex[i]];
            };
        }
    }
    return anyChild;
}

void N48::deleteChildren (void (*callback) (TID ridlist)) {
    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            N::deleteChildren (children[childIndex[i]], callback);
            N::deleteNode (children[childIndex[i]], callback);
        }
    }
}

uint64_t N48::getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                           uint32_t& childrenCount) const {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        auto cachedIndex = this->childIndex[i];
        if (cachedIndex != emptyMarker) {
            children[childrenCount] = std::make_tuple (i, this->children[cachedIndex]);
            childrenCount++;
        }
    }

    return childrenCount;
}

template <typename Fn>
void N48::DeepVisit (Stat& stat, Fn&& callback) {
    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            auto node = children[childIndex[i]];
            N::DeepVisit (node, stat, callback);
            callback (node);
        }
    }
}

// For Iterator
// return [child, child_key]
std::tuple<N*, uint8_t> N48::getIthChild (int ith) const {
    static auto kEmptyMarkerVec = _mm256_set1_epi8 (emptyMarker);
    if (ith >= count) return {nullptr, 0};

    for (int i = 0; i < 256; i += 32) {
        auto cmp = _mm256_cmpeq_epi8 (
            kEmptyMarkerVec,
            _mm256_lddqu_si256 (reinterpret_cast<const __m256i*> (childIndex + i)));
        uint32_t bitfield = _mm256_movemask_epi8 (cmp);
        bitfield = ~bitfield;
        // count how many childindex is non emptyMarker until now
        if (bitfield == 0) continue;
        int cur_count = __builtin_popcount (bitfield);
        if (cur_count > ith) {
            // means the Ith child is in this 32 byte
            while (ith) {
                // remove the lowest bit 1
                bitfield &= (bitfield - 1);
                ith--;
            }
            uint8_t pos = __builtin_ctz (bitfield);
            uint8_t key = i + pos;
            return {children[childIndex[key]], key};
        }
        ith -= cur_count;
    }
    return {nullptr, 0};
}

// find node's first child that have leaf node >= key
// return [child, child_key, ith]
std::tuple<N*, uint8_t, uint8_t> N48::seekChild (uint8_t key) const {
    static auto kEmptyMarkerVec = _mm256_set1_epi8 (emptyMarker);
    int childcount = 0;
    int i = 32;

    // Bypass the keys < key, count the child count [0, key)
    for (; i < 256 && i < key; i += 32) {
        auto cmp = _mm256_cmpeq_epi8 (
            kEmptyMarkerVec,
            _mm256_lddqu_si256 (reinterpret_cast<const __m256i*> (childIndex + i - 32)));
        uint32_t bitfield = _mm256_movemask_epi8 (cmp);
        bitfield = ~bitfield;
        // count how many childindex is non emptyMarker until now
        childcount += __builtin_popcount (bitfield);
    }
    i -= 32;

    // TODO: better method without comparing one by one?
    for (; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            if (i >= key) {
                return {children[childIndex[i]], i, childcount};
            }
            childcount++;
        }
    }
    return {nullptr, 0, 0};
}

}  // namespace ART_OLC