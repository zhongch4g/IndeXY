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

#include "../../shared/ori_artree.hpp"
#include "artree-ori/Tree.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/write_batch_with_index.h"

static thread_local rocksdb::WriteBatch writebatch;

// -------------------------------------------------------------------------------------
template <class Record>
struct ARTLSMAdapter {
    ARTLSM<Record>* artlsm_;
    LSM* lsm_;
    std::string idx_name;

    void inline TransformKey (const uint32_t& k, Key& ko) {
        ko.setKeyLen (sizeof (k));
        reinterpret_cast<uint32_t*> (&ko[0])[0] = __builtin_bswap32 (k);
    }

    inline size_t GenTID (uint64_t& ik, Key& mk, int klen) { return ik; }

    ARTLSMAdapter () : artlsm_ (nullptr) {}  // Hack
    ARTLSMAdapter (ARTLSM<Record>& art, LSM& lsm, std::string table) : artlsm_ (&art), lsm_ (&lsm) {
        idx_name = table;
        artlsm_->idx_name = table;
        artlsm_->tree_ = new ART_OLC_ORI::Tree (Record::loadKeyOri);
        artlsm_->lsm_ = lsm_;

        artlsm_->CreateIndex (Record::getValue, Record::getRid, Record::releaseValue);

        printf ("Initialize index %s tree addr %p\n", idx_name.c_str (), (void*)artlsm_->tree_);
    }
    // -------------------------------------------------------------------------------------
    void insert (const typename Record::Key& key, const Record& record) {
        Key k;
        Record::TransformKey (key, k);
        Record* r = new Record ();  // TODO: Deallocate the space
        memcpy (r, (u8*)(&record), sizeof (Record));
        artlsm_->UpsertImpl (k, reinterpret_cast<uint64_t> (r));
    }
    // -------------------------------------------------------------------------------------
    void lookup (const typename Record::Key& key) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = artlsm_->LookupImpl (k);
        assert (tid > 0);
    }
    // -------------------------------------------------------------------------------------
    void lookup1 (const typename Record::Key& key, const std::function<void (const Record&)>& fn) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = artlsm_->LookupImpl (k);
        assert (tid > 0);
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
        assert (found);
        return local_f;
    }
    // -------------------------------------------------------------------------------------
    template <class Fn>
    void update1 (const typename Record::Key& key, const Fn& fn,
                  leanstore::storage::btree::WALUpdateGenerator) {
        // TODO: The artlsm requires delete operation to update a record
        // TODO: Remember to deleting the old memory space
        // Record r;
        // lookup1 (key, [&] (const Record& rec) { r = rec; });
        // fn (r);
        // insert (r.key, r);

        Key k;
        Record::TransformKey (key, k);
        TID tid = artlsm_->LookupImpl (k);
        assert (tid > 0);
        char* r = reinterpret_cast<char*> (tid);
        Record& record = *reinterpret_cast<Record*> (r);
        fn (record);
        // Record* rd = reinterpret_cast<Record*> (r);
        // Record::ToString (rd);
        // erase (key, tid);
        // insert (key, record);
    }
    // -------------------------------------------------------------------------------------
    bool erase (const typename Record::Key& key, TID rid) {
        Key k;
        Record::TransformKey (key, k);
        artlsm_->DeleteImpl (k, rid);
        return 0;
    }
    // -------------------------------------------------------------------------------------
    void scan (const typename Record::Key& key,
               const std::function<bool (const typename Record::Key&, const Record&)>& fn,
               std::function<void ()>) {
        Key k;
        Record::TransformKey (key, k);
        uint64_t rid = 0;
        Record& r = *reinterpret_cast<Record*> ((char*)rid);
        fn (key, r);
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
        printf ("[%s-REAL] %s\n", this->idx_name.c_str (), artlsm_->ToStats ().c_str ());
    }

    void printIndexesRocksdb () {
        printf ("[%s-REAL] %s\n", this->idx_name.c_str (), artlsm_->ToStatsInfo ().c_str ());
    }
};
