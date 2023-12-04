#include <assert.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_ORI {

bool N4::isAllLeaf () const {
    for (uint32_t i = 0; i < count; ++i) {
        if (!N::isLeaf (children[i])) {
            return false;
        }
    }
    return true;
}

std::tuple<N*, uint8_t> N4::getRandomChildNonLeaf () {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = random () % count;
    for (int i = rnd_i; i < count; i++) {
        if (!N::isLeaf (children[i])) return {children[i], keys[i]};
    }
    for (int i = rnd_i - 1; i >= 0; i--) {
        if (!N::isLeaf (children[i])) return {children[i], keys[i]};
    }
    // means all children are leafnode
    return {emptyNode, 0};
}

void N4::deleteChildren (void (*callback) (TID ridlist)) {
    for (uint32_t i = 0; i < count; ++i) {
        N::deleteChildren (children[i], callback);
        N::deleteNode (children[i], callback);
    }
}

bool N4::isFull () const { return count == 4; }

bool N4::isUnderfull () const { return false; }

void N4::insert (uint8_t key, N* n) {
    unsigned pos;
    for (pos = 0; (pos < count) && (keys[pos] < key); pos++)
        ;
    memmove (keys + pos + 1, keys + pos, count - pos);
    memmove (children + pos + 1, children + pos, (count - pos) * sizeof (N*));
    keys[pos] = key;
    children[pos] = n;
    count++;
}

template <class NODE>
void N4::copyTo (NODE* n) const {
    for (uint32_t i = 0; i < count; ++i) {
        n->insert (keys[i], children[i]);
    }
}

bool N4::change (uint8_t key, N* val) {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] == key) {
            children[i] = val;
            return true;
        }
    }
    assert (false);
    __builtin_unreachable ();
}

N* N4::getChild (const uint8_t k) const {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] == k) {
            return children[i];
        }
    }
    return nullptr;
}

void N4::remove (uint8_t k) {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] == k) {
            memmove (keys + i, keys + i + 1, count - i - 1);
            memmove (children + i, children + i + 1, (count - i - 1) * sizeof (N*));
            count--;
            return;
        }
    }
}

N* N4::getAnyChild () const {
    N* anyChild = nullptr;
    for (uint32_t i = 0; i < count; ++i) {
        if (N::isLeaf (children[i])) {
            return children[i];
        } else {
            anyChild = children[i];
        }
    }
    return anyChild;
}

std::tuple<N*, uint8_t> N4::getSecondChild (const uint8_t key) const {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] != key) {
            return std::make_tuple (children[i], keys[i]);
        }
    }
    return std::make_tuple (nullptr, 0);
}

uint64_t N4::getChildren (uint8_t start, uint8_t end, std::tuple<uint8_t, N*>*& children,
                          uint32_t& childrenCount) const {
    childrenCount = 0;
    for (uint32_t i = 0; i < count; ++i) {
        if (this->keys[i] >= start && this->keys[i] <= end) {
            children[childrenCount] = std::make_tuple (this->keys[i], this->children[i]);
            childrenCount++;
        }
    }
    return childrenCount;
}

template <typename Fn>
void N4::DeepVisit (Stat& stat, Fn&& callback) {
    for (uint32_t i = 0; i < count; ++i) {
        N* node = this->children[i];
        N::DeepVisit (node, stat, callback);
        callback (node);
    }
}

// For Iterator
// return [child, child_key]
std::tuple<N*, uint8_t> N4::getIthChild (int ith) const {
    if (ith >= count) return {nullptr, 0};
    return {children[ith], keys[ith]};
}

// find node's first child that have leaf node >= key
// return [child, child_key, ith]
std::tuple<N*, uint8_t, uint8_t> N4::seekChild (uint8_t key) const {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] >= key) {
            return {children[i], keys[i], i};
        }
    }
    return {nullptr, 0, 0};
}

}  // namespace ART_OLC