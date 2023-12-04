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
using namespace exdindex;

template <size_t value_size>
struct YCSBStruct_t {
    struct Key {
        uint64_t id;
    };
    Key key;
    char data_[value_size];

    template <class T>
    static void TransformKey (T& key, IndexKey& index_key) {
        index_key.setKeyLen (sizeof (YCSBStruct_t::Key));
        reinterpret_cast<uint64_t*> (&index_key[0])[0] = __builtin_bswap64 (key.id);
    }

    static bool loadKey (TID tid, IndexKey& akey) {
        akey.setKeyLen (sizeof (YCSBStruct_t::Key));
        YCSBStruct_t* ycsb = reinterpret_cast<YCSBStruct_t*> (tid);
        if (value_size > 8) {
            reinterpret_cast<uint64_t*> (&akey[0])[0] = __builtin_bswap64 (ycsb->key.id);
        } else {
            reinterpret_cast<uint64_t*> (&akey[0])[0] = __builtin_bswap64 (tid);
        }

        return true;
    }

    static void getValue (uint64_t rid, std::string& rValue) {
        if (value_size > 8) {
            rValue = std::string ((char*)rid, sizeof (YCSBStruct_t));
        } else {
            rValue.resize (8);                      // make sure the string has enough space
            std::memcpy (rValue.data (), &rid, 8);  // copy the binary data
        }
    }

    static void getRid (uint64_t& rid, char* rValue) {
        if (value_size > 8) {
            YCSBStruct_t* val = new YCSBStruct_t ();
            memcpy (val, rValue, sizeof (YCSBStruct_t));
            rid = reinterpret_cast<uint64_t> (val);
        } else {
            std::memcpy (&rid, rValue, 8);  // retrieve the binary data
        }
    }

    static void releaseValue (uint64_t rid, std::atomic<uint64_t>& deallocated) {
        if (value_size > 8) {
            YCSBStruct_t* valp = reinterpret_cast<YCSBStruct_t*> (rid);
            deallocated.fetch_add (malloc_usable_size (valp));
            delete valp;
        }
    }
};

using ycsb_structure_t = YCSBStruct_t<CMAKE_CONFIG_VALUE_SIZE>;

template <typename Record>
class StructureXY {
public:
    StructureXY () {}  // Hack

    StructureXY (std::string name, std::shared_ptr<XYStore> xystore) : name_ (name) {
        INFO ("CMAKE_CONFIG_VALUE_SIZE %lu", CMAKE_CONFIG_VALUE_SIZE);
        artkvs = xystore->registerARTKVS (ycsb_structure_t::loadKey, ycsb_structure_t::getValue,
                                          ycsb_structure_t::getRid, ycsb_structure_t::releaseValue,
                                          name_);
    }
    // -------------------------------------------------------------------------------------
    void insert (const typename Record::Key& key, uint64_t& tid) {
        if (CMAKE_CONFIG_VALUE_SIZE > 8) {
            Key k;
            Record::TransformKey (key, k);

            Record* r = new Record ();
            r->key.id = key.id;
            artkvs->vMemoryAllocated.fetch_add (malloc_usable_size (r));
            artkvs->Insert (k, reinterpret_cast<uint64_t> (r));
        } else {
            Key k;
            Record::TransformKey (key, k);
            artkvs->Insert (k, tid);
        }
    }
    // -------------------------------------------------------------------------------------
    TID lookup (const typename Record::Key& key, uint64_t& disk_access) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = artkvs->Lookup (k, disk_access);
        return tid;
    }

    TID warmuplookup (const typename Record::Key& key, uint64_t& rid, uint64_t& disk_access) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = artkvs->Lookup2 (k, rid, disk_access);
        return tid;
    }

    void scan (const typename Record::Key& key) {
        Key k;
        Record::TransformKey (key, k);
        TID tid = artkvs->LookupIterator (k, key.id);
    }

public:
    std::string name_;
    ARTKVS::ARTreeKVS* artkvs;
};
