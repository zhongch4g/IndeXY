#include <assert.h>
#include <immintrin.h>
#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <iostream>

#include "N.h"

namespace ART_OLC_X {

bool N256::isAllLeaf () const {
    for (uint32_t i = 0; i < 256; ++i) {
        if (children[i] != nullptr && !N::isLeaf (children[i])) {
            return false;
        }
    }
    return true;
}

std::tuple<N*, uint8_t> N256::getRandomChildNonLeaf (bool isRandom) {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = isRandom ? random () % 256 : 0;
    for (int key = rnd_i; key < 256; key++) {
        if (children[key] != nullptr && !N::isLeaf (children[key])) return {children[key], key};
    }
    for (int key = rnd_i - 1; key >= 0; key--) {
        if (children[key] != nullptr && !N::isLeaf (children[key])) return {children[key], key};
    }
    // means all children are leafnode
    return {emptyNode, 0};
}

bool N256::isFull () const { return false; }

bool N256::isUnderfull () const { return count == 37; }

void N256::deleteChildren (std::function<void (TID)> callback) {
    for (uint64_t i = 0; i < 256; ++i) {
        if (children[i] != nullptr) {
            N::deleteChildren (children[i], callback);
            N::deleteNode (children[i], callback);
        }
    }
}

void N256::insert (uint8_t key, N* val) {
    children[key] = val;
    count++;
}

#ifdef UNIQUENESS_CHECK
void N256::insertWithHybridStat (uint8_t key, N* val, bool isHybrid) {
    children[key] = val;
    count++;
    if (isHybrid) {
        hybridBitmap.setHybrid (key);
    }
}
#endif

template <class NODE>
void N256::copyTo (NODE* n) const {
    for (int i = 0; i < 256; ++i) {
        if (children[i] != nullptr) {
#ifdef UNIQUENESS_CHECK
            auto isHybrid = hybridBitmap.isHybrid (i);
            n->insertWithHybridStat (i, children[i], isHybrid);
#endif
            n->insert (i, children[i]);
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

bool N256::change (uint8_t key, N* n) {
    children[key] = n;
    return true;
}

#ifdef UNIQUENESS_CHECK
bool N256::setHybridBits (uint8_t key) {
    hybridBitmap.setHybrid (key);
    return true;
}
#endif

N* N256::getChild (const uint8_t k) const { return children[k]; }

#ifdef UNIQUENESS_CHECK
bool N256::isHybridStat (const uint8_t k) const { return hybridBitmap.isHybrid (k); }
#endif

void N256::remove (uint8_t k) {
    children[k] = nullptr;
    count--;
}

N* N256::getAnyChild () const {
    N* anyChild = nullptr;
    for (uint64_t i = 0; i < 256; ++i) {
        if (children[i] != nullptr) {
            if (N::isLeaf (children[i])) {
                return children[i];
            } else {
                anyChild = children[i];
            }
        }
    }
    return anyChild;
}

uint64_t N256::getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                            uint32_t& childrenCount) const {
    childrenCount = 0;
    for (unsigned i = start; i <= end; i++) {
        if (this->children[i] != nullptr) {
            children[childrenCount] = std::make_tuple (i, this->children[i]);
            childrenCount++;
        }
    }
    return childrenCount;
}

template <typename Fn>
size_t N256::DeepVisit (Stat& stat, Fn&& callback, std::string prefix) {
    size_t total_size = 0;
    std::string new_prefix =
        prefix + (this->hasPrefix ()
                      ? std::string ((char*)this->prefix,
                                     std::min (this->getPrefixLength (), maxStoredPrefixLength))
                      : "");
    for (unsigned i = 0; i < 256; i++) {
        if (this->children[i] != nullptr) {
            auto node = this->children[i];
            new_prefix.push_back ((char)i);
            total_size += N::DeepVisit (node, stat, callback, new_prefix);
            callback (node, new_prefix);
            new_prefix.pop_back ();
        }
    }
    this->subtree_size = total_size + sizeof (N256);
    return this->subtree_size;
}

template <typename Fn>
size_t N256::DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action) {
    size_t total_size = 0;

    for (unsigned i = 0; i < 256; i++) {
        if (this->children[i] != nullptr) {
            auto node = this->children[i];

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
    }
    this->subtree_size = 0;
    if (action == 2) {
        this->subtree_size = total_size + sizeof (N256);
    }
    return this->subtree_size;
}

// For Iterator
// return [child, child_key]
std::tuple<N*, uint8_t> N256::getIthChild (int ith) const {
    // TODO: replace this with O(1) algorithm
    if (ith >= count) return {nullptr, 0};
    int childcount = 0;
    for (uint64_t i = 0; i < 256; ++i) {
        if (children[i] != nullptr) {
            if (childcount == ith) {
                return {children[i], i};
            }
            childcount++;
        }
    }
    return {nullptr, 0};
}

// find node's first child that have leaf node >= key
// return [child, child_key, ith]
std::tuple<N*, uint8_t, uint8_t> N256::seekChild (uint8_t key) const {
    // TODO: replace this with O(1) algorithm
    int childcount = 0;
    for (unsigned i = 0; i < 256; i++) {
        if (children[i] != nullptr) {
            if (i >= key) {
                return {children[i], i, childcount};
            }
            childcount++;
        }
    }
    return {nullptr, 0, 0};
}

}  // namespace ART_OLC_X