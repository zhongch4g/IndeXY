#pragma once
// -------------------------------------------------------------------------------------
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

#include <atomic>
#include <vector>

#include "XYStore.hpp"

#define VALUE_SIZE 8
using namespace exdindex;

template <size_t value_size>
struct YCSBStruct_art_t {
    struct Key {
        uint64_t id;
    };
    Key key;
    char data_[value_size];

    template <class T>
    static void TransformKey (T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (YCSBStruct_art_t::Key));
        reinterpret_cast<uint64_t*> (&index_key[0])[0] = __builtin_bswap64 (key.id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (YCSBStruct_art_t::Key));
        YCSBStruct_art_t* ycsb = reinterpret_cast<YCSBStruct_art_t*> (tid);
        if (value_size > 8) {
            reinterpret_cast<uint64_t*> (&akey[0])[0] = __builtin_bswap64 (ycsb->key.id);
        } else {
            reinterpret_cast<uint64_t*> (&akey[0])[0] = __builtin_bswap64 (tid);
        }

        return true;
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        if (value_size > 8) {
            rValue = std::string ((char*)rid, sizeof (YCSBStruct_art_t));
        } else {
            rValue.resize (8);                      // make sure the string has enough space
            std::memcpy (rValue.data (), &rid, 8);  // copy the binary data
        }
    }

    static void getRid (uint64_t& rid, char* rValue) {
        if (value_size > 8) {
            YCSBStruct_art_t* val = new YCSBStruct_art_t ();
            memcpy (val, rValue, sizeof (YCSBStruct_art_t));
            rid = reinterpret_cast<uint64_t> (val);
        } else {
            std::memcpy (&rid, rValue, 8);  // retrieve the binary data
        }
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        if (value_size > 8) {
            YCSBStruct_art_t* valp = reinterpret_cast<YCSBStruct_art_t*> (rid);
            deallocated.fetch_add (malloc_usable_size (valp));
            delete valp;
        }
    }
};

using ycsb_structure_art_t = YCSBStruct_art_t<VALUE_SIZE>;

template <typename Record>
class OriginalART {
public:
    OriginalART () {}  // Hack

    OriginalART (std::string name, std::shared_ptr<XYStore> xystore) : name_ (name) {
        art = xystore->registerARTree (ycsb_structure_art_t::loadKey,
                                       ycsb_structure_art_t::getValue, ycsb_structure_art_t::getRid,
                                       ycsb_structure_art_t::releaseValue, name_);
    }
    // -------------------------------------------------------------------------------------
    void insert (const typename Record::Key& key, uint64_t& tid) {
        if (VALUE_SIZE > 8) {
            Key k;
            Record::TransformKey (key, k);

            Record* r = new Record ();
            r->key.id = key.id;
            art->Insert (k, reinterpret_cast<uint64_t> (r));
        } else {
            Key k;
            Record::TransformKey (key, k);
            art->Insert (k, tid);
        }
    }
    // -------------------------------------------------------------------------------------
    TID lookup (const typename Record::Key& key) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = art->Lookup (k);
        return tid;
    }

    void scan (const typename Record::Key& key) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = art->LookupIterator (k, key.id);
    }

public:
    std::string name_;
    ART_INTERFACE::ARTree* art;
};
