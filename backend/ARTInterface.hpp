#pragma once
#ifndef ART_INTERFACE_H
#define ART_INTERFACE_H

#include "leanstore/LeanStore.hpp"
// -------------------------------------------------------------------------------------
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
// -------------------------------------------------------------------------------------
#include <jemalloc/jemalloc.h>
#include <tbb/parallel_for.h>

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <iomanip>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "XYConfig.hpp"
#include "artree-ori/Tree.h"
#include "artree-shared-headers/Key.h"
#include "logger.h"

// =====================================================================
using namespace ART_OLC_ORI;
// =====================================================================
// Get LoadKey callback function for ARTree
using loadKeyCallbackFn = bool (*) (TID tid, Key& key);
// Get real value based on pointer and release the memory
using getObjectFn = std::function<void (uint64_t, std::string&)>;
// Allocate a new space for value and get its pointer (Row id)
using getRidFn = std::function<void (uint64_t&, char*)>;
// Get the value via RID and release the value
using releaseValueFn = std::function<void (uint64_t, std::atomic<uint64_t>&)>;
// =====================================================================

namespace ART_INTERFACE {

class ARTree {
public:
    ARTree ();
    void CreateARTree (loadKeyCallbackFn loadkey, getObjectFn fn1, getRidFn fn2, releaseValueFn fn3,
                       std::string idx_name);
    bool Insert (const IndexKey& key, TID rid);
    TID Lookup (IndexKey& key);
    TID LookupRange (const IndexKey& keyleft, const IndexKey& keyright);
    bool LookupIterator (const IndexKey& target, TID rid);
    bool Delete (const IndexKey& key, TID rid);
    std::string ToStats ();
    void ReleaseX ();
    // =====================================================================

public:
    getObjectFn getObject{};
    getRidFn getRid{};
    releaseValueFn releaseValue{};

public:
    ART_OLC_ORI::Tree* tree_;

    std::string idx_name_;
};

};  // namespace ART_INTERFACE

#endif
