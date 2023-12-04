#include "ARTInterface.hpp"

namespace ART_INTERFACE {

// =====================================================================================

ARTree::ARTree () {}

void ARTree::CreateARTree (loadKeyCallbackFn loadkey, getObjectFn fn1, getRidFn fn2,
                           releaseValueFn fn3, std::string idx_name) {
    tree_ = new Tree (loadkey);
    INFO ("Create a ARTree instance");
    getObject = fn1;
    getRid = fn2;
    releaseValue = fn3;
    idx_name_ = idx_name;
}

bool ARTree::Insert (const IndexKey& key, TID rid) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    bool ret = tree_->insert (key, rid, thread);
    return ret;
}

TID ARTree::Lookup (IndexKey& key) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    auto tid = tree_->lookup (key, thread);

    return tid;  // it finds target in memory
}

TID ARTree::LookupRange (const IndexKey& keyleft, const IndexKey& keyright) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    TID mtagets[1000];
    Key ckey;
    size_t found = 0;

    auto find = tree_->lookupRange (false, keyleft, keyright, ckey, mtagets, 1000, found, thread,
                                    &seeHybrid);
    if (found > 0) {
        return 1;
    }
    return 0;
}

bool ARTree::LookupIterator (const IndexKey& target, TID rid) {
    auto thread = tree_->getThreadInfo ();
    auto iter = tree_->NewIterator (nullptr);
    iter->Seek (target);
    for (uint64_t i = 0; i < 100 && iter->Valid (); i++) {
        iter->Next ();
    }
    return 1;
}

bool ARTree::Delete (const IndexKey& key, TID rid) {
    auto thread = tree_->getThreadInfo ();
    bool seeHybrid = false;
    bool res = tree_->remove (key, rid, thread);
    return res;
}

std::string ARTree::ToStats () { return tree_->ToStats (); }

void ARTree::ReleaseX () {
    tree_->ReleaseTree ([] (TID ridlist) {});
}
};  // namespace ART_INTERFACE
