#include <assert.h>
#include <immintrin.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_ORI {

bool N256::isAllLeaf () const {
    for (uint32_t i = 0; i < 256; ++i) {
        if (children[i] != nullptr && !N::isLeaf (children[i])) {
            return false;
        }
    }
    return true;
}

std::tuple<N*, uint8_t> N256::getRandomChildNonLeaf () {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = random () % 256;
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

void N256::deleteChildren (void (*callback) (TID ridlist)) {
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

template <class NODE>
void N256::copyTo (NODE* n) const {
    for (int i = 0; i < 256; ++i) {
        if (children[i] != nullptr) {
            n->insert (i, children[i]);
        }
    }
}

bool N256::change (uint8_t key, N* n) {
    children[key] = n;
    return true;
}

N* N256::getChild (const uint8_t k) const { return children[k]; }

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
void N256::DeepVisit (Stat& stat, Fn&& callback) {
    for (unsigned i = 0; i < 256; i++) {
        if (this->children[i] != nullptr) {
            auto node = this->children[i];
            N::DeepVisit (node, stat, callback);
            callback (node);
        }
    }
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

}  // namespace ART_OLC