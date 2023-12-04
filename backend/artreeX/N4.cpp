#include <assert.h>

#include <algorithm>

#include "N.h"

namespace ART_OLC_X {

bool N4::isAllLeaf () const {
    for (uint32_t i = 0; i < count; ++i) {
        if (!N::isLeaf (children[i])) {
            return false;
        }
    }
    return true;
}

std::tuple<N*, uint8_t> N4::getRandomChildNonLeaf (bool isRandom) {
    static N* emptyNode = nullptr;
    uint32_t rnd_i = isRandom ? random () % count : 0;
    for (int i = rnd_i; i < count; i++) {
        if (!N::isLeaf (children[i])) return {children[i], keys[i]};
    }
    for (int i = rnd_i - 1; i >= 0; i--) {
        if (!N::isLeaf (children[i])) return {children[i], keys[i]};
    }
    // means all children are leafnode
    return {emptyNode, 0};
}

void N4::deleteChildren (std::function<void (TID)> callback) {
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
#ifdef UNIQUENESS_CHECK
    hybridBitmap.memmove (pos, 1);
#endif
    memmove (keys + pos + 1, keys + pos, count - pos);
    memmove (children + pos + 1, children + pos, (count - pos) * sizeof (N*));
    keys[pos] = key;
    children[pos] = n;
    count++;
}
#ifdef UNIQUENESS_CHECK
void N4::insertWithHybridStat (uint8_t key, N* n, bool isHybrid) {
    unsigned pos;
    for (pos = 0; (pos < count) && (keys[pos] < key); pos++)
        ;
    hybridBitmap.memmove (pos, 1);
    memmove (keys + pos + 1, keys + pos, count - pos);
    memmove (children + pos + 1, children + pos, (count - pos) * sizeof (N*));
    keys[pos] = key;
    children[pos] = n;
    if (isHybrid) hybridBitmap.setHybrid (pos);
    count++;
}
#endif
template <class NODE>
void N4::copyTo (NODE* n) const {
    for (uint32_t i = 0; i < count; ++i) {
#ifdef UNIQUENESS_CHECK
        auto isHybrid = this->hybridBitmap.isHybrid (i);
        n->insertWithHybridStat (keys[i], children[i], isHybrid);
#endif
        n->insert (keys[i], children[i]);
    }
    // Transfer node stat to the new node
    n->candidate = candidate;
#ifdef ATOMIC_ACCESS_BIT
    n->clockStat.store (clockStat.load ());
#else
    n->clockStat = clockStat;
#endif
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
#ifdef UNIQUENESS_CHECK
bool N4::setHybridBits (uint8_t key) {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] == key) {
            hybridBitmap.setHybrid (i);
            return true;
        }
    }
    assert (false);
    __builtin_unreachable ();
}
#endif
N* N4::getChild (const uint8_t k) const {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] == k) {
            return children[i];
        }
    }
    return nullptr;
}

#ifdef UNIQUENESS_CHECK
bool N4::isHybridStat (const uint8_t k) const {
    for (uint32_t i = 0; i < count; ++i) {
        if (keys[i] == k) {
            return hybridBitmap.isHybrid (i);
        }
    }
    return false;
}
#endif

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
size_t N4::DeepVisit (Stat& stat, Fn&& callback, std::string prefix) {
    size_t total_size = 0;
    std::string new_prefix =
        prefix + (this->hasPrefix ()
                      ? std::string ((char*)this->prefix,
                                     std::min (this->getPrefixLength (), maxStoredPrefixLength))
                      : "");
    for (uint32_t i = 0; i < count; ++i) {
        N* node = this->children[i];

        new_prefix.push_back (this->keys[i]);
        total_size += N::DeepVisit (node, stat, callback, new_prefix);
        callback (node, new_prefix);
        new_prefix.pop_back ();
    }
    this->subtree_size = total_size + sizeof (N4);
    return this->subtree_size;
}

template <typename Fn>
size_t N4::DeepVisitRelease (Stat& stat, Fn&& callback, uint64_t action) {
    size_t total_size = 0;

    for (uint32_t i = 0; i < count; ++i) {
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
            // free the space TODO: release the real value
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
        this->subtree_size = total_size + sizeof (N4);
    }
    return this->subtree_size;
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

}  // namespace ART_OLC_X