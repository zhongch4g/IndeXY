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
#include "rocksdb/db.h"
#include <atomic>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"


// -------------------------------------------------------------------------------------

struct RocksDB {
   rocksdb::DB* db = nullptr;

   rocksdb::WriteOptions wo;
   rocksdb::ReadOptions ro;
   enum class DB_TYPE : u8 { DB, TransactionDB, OptimisticDB };
   const DB_TYPE type;
   // -------------------------------------------------------------------------------------
   RocksDB(std::string path, DB_TYPE type = DB_TYPE::DB) : type(type)
   {
      wo.disableWAL = true;
      wo.sync = false;
      // -------------------------------------------------------------------------------------
      rocksdb::Options db_options;
      db_options.use_direct_reads = true;
      db_options.use_direct_io_for_flush_and_compaction = true;
      // db_options.db_write_buffer_size = 0;  // disabled
      db_options.write_buffer_size = 256 * 1024 * 1024; // keep the default
      // db_options.write_buffer_size = 64 * 1024 * 1024; keep the default
      db_options.create_if_missing = true;
      db_options.manual_wal_flush = true;
      db_options.compression = rocksdb::CompressionType::kNoCompression;
      // db_options.OptimizeLevelStyleCompaction(FLAGS_dram_gib * 1024 * 1024 * 1024);
      db_options.row_cache = rocksdb::NewLRUCache(FLAGS_dram_gib * 1024 * 1024 * 1024);
      rocksdb::Status s;
      std::cout << "Start RocksDB Initialization ." << std::endl;
      s = rocksdb::DB::Open(db_options, path, &db);
      if (!s.ok())
         cerr << s.ToString() << endl;
      assert(s.ok());
      std::cout << "RocksDB Initialized ." << std::endl;
   }

   ~RocksDB() { delete db; }
   void startTX() {}
   void commitTX() {}
   void prepareThread() {}
};
// -------------------------------------------------------------------------------------
template <class Record>
struct RocksDBAdapter {
   using SEP = u32;  // use 32-bits integer as separator instead of column family
   rocksdb::DB* db = nullptr;
   rocksdb::WriteOptions wo;
   rocksdb::ReadOptions ro;
   rocksdb::BlockBasedTableOptions tableOptions;
   std::string dbname_;
   RocksDBAdapter() {

   }

   void InitializeDB() {
      wo.disableWAL = true;
      wo.sync = false;
      // -------------------------------------------------------------------------------------
      tableOptions.whole_key_filtering = true;
      tableOptions.filter_policy.reset (rocksdb::NewBloomFilterPolicy (10, false));

      rocksdb::Options db_options;
      // db_options.use_direct_reads = true;
      // db_options.use_direct_io_for_flush_and_compaction = true;
      // db_options.db_write_buffer_size = 0;  // disabled
      db_options.write_buffer_size = 256 * 1024 * 1024; //keep the default
      db_options.create_if_missing = true;
      // db_options.manual_wal_flush = true;
      db_options.compression = rocksdb::CompressionType::kNoCompression;
      // db_options.OptimizeLevelStyleCompaction(FLAGS_dram_gib * 1024 * 1024 * 1024);
      db_options.row_cache = rocksdb::NewLRUCache(10 * 1024 * 1024 * 1024);
      db_options.IncreaseParallelism (4);
      db_options.OptimizeUniversalStyleCompaction();

      db_options.table_factory.reset (rocksdb::NewBlockBasedTableFactory (tableOptions));
      
      rocksdb::Status s;
      std::cout << "Start RocksDB Initialization ." << std::endl;
      dbname_ = "/dev/shm/artree_" + std::to_string ((uint64_t)this);
      s = rocksdb::DB::Open(db_options, dbname_, &db);
      if (!s.ok())
         cerr << s.ToString() << endl;
      assert(s.ok());
      std::cout << "RocksDB Initialized ." << std::endl;
   }
   // -------------------------------------------------------------------------------------
   template <typename T>
   rocksdb::Slice RSlice(T* ptr, u64 len)
   {
      return rocksdb::Slice(reinterpret_cast<const char*>(ptr), len);
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldRecord(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      
      s = db->Put(wo, RSlice(folded_key, folded_key_len), RSlice(&record, sizeof(record)));
      ensure(s.ok());
   }
   // -------------------------------------------------------------------------------------
   void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& fn)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldRecord(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::PinnableSlice value;
      rocksdb::Status s;
      s = db->Get(ro, db->DefaultColumnFamily(), RSlice(folded_key, folded_key_len), &value);
      
      assert(s.ok());
      const Record& record = *reinterpret_cast<const Record*>(value.data());
      fn(record);
      value.Reset();
   }
   // -------------------------------------------------------------------------------------
   void update1(const typename Record::Key& key, const std::function<void(Record&)>& fn, leanstore::storage::btree::WALUpdateGenerator)
   {
      Record r;
      lookup1(key, [&](const Record& rec) { r = rec; });
      fn(r);
      insert(key, r);
   }
   // -------------------------------------------------------------------------------------
   bool erase(const typename Record::Key& key)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldRecord(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Status s;
      s = db->Delete(wo, RSlice(folded_key, folded_key_len));
      if (s.ok()) {
        return true;
      } else {
        return false;
      }

   }
   // -------------------------------------------------------------------------------------
   template <class T>
   uint32_t getId(const T& str)
   {
      return __builtin_bswap32(*reinterpret_cast<const uint32_t*>(str.data())) ^ (1ul << 31);
   }
   //             [&](const neworder_t::Key& key, const neworder_t&) {
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& fn, std::function<void()>)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldRecord(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = db->NewIterator(ro);
      for (it->Seek(RSlice(folded_key, folded_key_len)); it->Valid() && getId(it->key()) == Record::id; it->Next()) {
         typename Record::Key s_key;
         Record::unfoldRecord(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key,
                 const std::function<bool(const typename Record::Key&, const Record&)>& fn,
                 std::function<void()>)
   {
      u8 folded_key[Record::maxFoldLength() + sizeof(SEP)];
      const u32 folded_key_len = fold(folded_key, Record::id) + Record::foldRecord(folded_key + sizeof(SEP), key);
      // -------------------------------------------------------------------------------------
      rocksdb::Iterator* it = db->NewIterator(ro);
      for (it->SeekForPrev(RSlice(folded_key, folded_key_len)); it->Valid() && getId(it->key()) == Record::id; it->Prev()) {
         typename Record::Key s_key;
         Record::unfoldRecord(reinterpret_cast<const u8*>(it->key().data() + sizeof(SEP)), s_key);
         const Record& s_value = *reinterpret_cast<const Record*>(it->value().data());
         if (!fn(s_key, s_value))
            break;
      }
      assert(it->status().ok());
      delete it;
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      Field local_f;
      bool found = false;
      lookup1(key, [&](const Record& record) {
         found = true;
         local_f = (record).*f;
      });
      assert(found);
      return local_f;
   }
};
