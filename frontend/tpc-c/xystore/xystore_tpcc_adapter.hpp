#pragma once
#include "../../shared/types.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/LeanStore.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
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
#include <vector>

#include "XYStore.hpp"
// -------------------------------------------------------------------------------------
using namespace exdindex;
template <typename Record>
class StructureXY {
public:
    StructureXY () {}
    // For Rocksdb
    StructureXY (std::string name, std::shared_ptr<XYStore> xystore) : name_ (name) {
        artkvs = xystore->registerARTKVS (&Record::loadKey, Record::getValue, Record::getRid,
                                          Record::releaseValue, name_);
    }
    ~StructureXY () {}

    // -------------------------------------------------------------------------------------
    void insert (const typename Record::Key& key, const Record& record) {
        Key k;
        Record::TransformKey (key, k);
        Record* r = new Record ();
        artkvs->vMemoryAllocated.fetch_add (malloc_usable_size (r));
        memcpy (r, (u8*)(&record), sizeof (Record));
        artkvs->Insert (k, reinterpret_cast<uint64_t> (r));
    }
    // -------------------------------------------------------------------------------------
    void lookup1 (const typename Record::Key& key, const std::function<void (const Record&)>& fn) {
        Key k;
        uint64_t da = 0;
        Record::TransformKey (key, k);
        TID tid = artkvs->Lookup (k, da);
        ensure (tid > 0);
        Record& record = *reinterpret_cast<Record*> ((void*)tid);
        fn (record);
    }
    // -------------------------------------------------------------------------------------
    template <class Field>
    Field lookupField (const typename Record::Key& key, Field Record::*f) {
        Field local_f;
        bool found = false;
        lookup1 (key, [&] (const Record& record) {
            found = true;
            local_f = (record).*f;
        });
        ensure (found);
        return local_f;
    }
    // -------------------------------------------------------------------------------------
    template <class Fn>
    void update1 (const typename Record::Key& key, const Fn& fn,
                  leanstore::storage::btree::WALUpdateGenerator) {
        Key k;
        uint64_t da = 0;
        Record::TransformKey (key, k);
        TID tid = artkvs->Lookup (k, da);
        ensure (tid > 0);
        char* r = reinterpret_cast<char*> (tid);
        Record& record = *reinterpret_cast<Record*> (r);
        fn (record);
    }
    // -------------------------------------------------------------------------------------
    bool erase (const typename Record::Key& key, TID rid) {
        Key k;
        Record::TransformKey (key, k);
        artkvs->Delete (k, rid);
        return 0;
    }
    // -------------------------------------------------------------------------------------
    void scan (const typename Record::Key& key,
               const std::function<bool (const typename Record::Key&, const Record&)>& fn,
               std::function<void ()>) {
        Key k;
        Record::TransformKey (key, k);
        uint64_t rid = 0;
        // artkvs->Scan();
        // Record& r = *reinterpret_cast<Record*> ((char*)rid);
        // fn (key, r);
    }
    // -------------------------------------------------------------------------------------
    void scanDesc (const typename Record::Key& key,
                   const std::function<bool (const typename Record::Key&, const Record&)>& fn,
                   std::function<void ()>) {
        Key k;
        Record::TransformKey (key, k);
        uint64_t rid = 0;
        Record& r = *reinterpret_cast<Record*> ((char*)rid);
        fn (key, r);
    }

    void printIndexes () {
        printf ("[%s-ACCURATE] %s\n", this->table_.c_str (), artkvs->ToStats ().c_str ());
    }

public:
    std::string name_;
    ARTKVS::ARTreeKVS* artkvs;
};
