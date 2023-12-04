#include <assert.h>
#include <immintrin.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_X {

bool N48::isAllLeaf () const {
    for (uint32_t i = 0; i < count; ++i) {
        if (!N::isLeaf (children[i])) {
            return false;
        }
    }
    return true;
}

std::tuple<N*, uint8_t> N48::getRandomChildNonLeaf (bool isRandom) {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = isRandom ? random () % 256 : 0;
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

#ifdef UNIQUENESS_CHECK
void N48::insertWithHybridStat (uint8_t key, N* n, bool isHybrid) {
    unsigned pos = count;
    if (children[pos]) {
        for (pos = 0; children[pos] != nullptr; pos++)
            ;
    }
    children[pos] = n;
    childIndex[key] = (uint8_t)pos;
    if (isHybrid) hybridBitmap.setHybrid (childIndex[key]);
    count++;
}
#endif

template <class NODE>
void N48::copyTo (NODE* n) const {
    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
#ifdef UNIQUENESS_CHECK
            auto isHybrid = hybridBitmap.isHybrid (childIndex[i]);
            n->insertWithHybridStat (i, children[childIndex[i]], isHybrid);
#endif
            n->insert (i, children[childIndex[i]]);
        }
    }
    // Transfer node stat to the new node
    n->candidate = candidate;
#ifdef ATOMIC_ACCESS_BIT
    n->clockStat.store (clockStat.load ());
#else
    n->clockStat = clockStat;
#endif
}

bool N48::change (uint8_t key, N* val) {
    children[childIndex[key]] = val;
    return true;
}

#ifdef UNIQUENESS_CHECK
bool N48::setHybridBits (uint8_t key) {
    uint32_t i = childIndex[key];
    hybridBitmap.setHybrid (i);
    assert ((i >= 0) && (i < 48));
    return true;
}
#endif

N* N48::getChild (const uint8_t k) const {
    if (childIndex[k] == emptyMarker) {
        return nullptr;
    } else {
        return children[childIndex[k]];
    }
}

#ifdef UNIQUENESS_CHECK
bool N48::isHybridStat (const uint8_t k) const { return hybridBitmap.isHybrid (childIndex[k]); }
#endif

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

void N48::deleteChildren (std::function<void (TID)> callback) {
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
size_t N48::DeepVisit (Stat& stat, Fn&& callback, std::string prefix) {
    size_t total_size = 0;
    std::string new_prefix =
        prefix + (this->hasPrefix ()
                      ? std::string ((char*)this->prefix,
                                     std::min (this->getPrefixLength (), maxStoredPrefixLength))
                      : "");
    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            auto node = children[childIndex[i]];
            new_prefix.push_back ((char)i);
            total_size += N::DeepVisit (node, stat, callback, new_prefix);
            callback (node, new_prefix);
            new_prefix.pop_back ();
        }
    }
    this->subtree_size = total_size + sizeof (N48);
    return this->subtree_size;
}

template <typename Fn>
size_t N48::DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action) {
    size_t total_size = 0;

    for (unsigned i = 0; i < 256; i++) {
        if (childIndex[i] != emptyMarker) {
            auto node = children[childIndex[i]];
            total_size += N::DeepVisitRelease (node, stat, callback, action);
            callback (node, "");
            if ((action == 1) && N::isLeaf (children[childIndex[i]])) {
                // clear the dirty bit
                if (N::isDirty (children[childIndex[i]])) {
                    auto nodeStat = N::getNodeState (children[childIndex[i]]);
                    children[childIndex[i]] =
                        N::setLeaf (N::getLeaf (children[childIndex[i]]) | nodeStat);
                }
            }
            if ((action == 2) && !N::isLeaf (children[childIndex[i]])) {
                // free the space
                auto child = children[childIndex[i]];
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
                        children[childIndex[i]] = nullptr;
                        childIndex[i] = emptyMarker;
                        break;
                    }
                    case NTypes::N16: {
                        N16* n = static_cast<N16*> (child);
                        delete (n);
                        children[childIndex[i]] = nullptr;
                        childIndex[i] = emptyMarker;
                        break;
                    }
                    case NTypes::N48: {
                        N48* n = static_cast<N48*> (child);
                        delete (n);
                        children[childIndex[i]] = nullptr;
                        childIndex[i] = emptyMarker;
                        break;
                    }
                    case NTypes::N256: {
                        N256* n = static_cast<N256*> (child);
                        delete (n);
                        children[childIndex[i]] = nullptr;
                        childIndex[i] = emptyMarker;
                        break;
                    }
                    default: {
                        assert (false);
                        __builtin_unreachable ();
                    }
                }
            }
        }
    }
    this->subtree_size = 0;
    if (action == 2) {
        this->subtree_size = total_size + sizeof (N48);
    }
    return this->subtree_size;
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

}  // namespace ART_OLC_X