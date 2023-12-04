#include "gflags/gflags.h"

DEFINE_double (x_dram_gib, 1, "consumption for ARTs");
DEFINE_string (x_idx, "ARTree", "config X index");
DEFINE_string (y_kvs, "Leanstore", "config Y key value store");
DEFINE_uint32 (public_list_len, 1000000, "length of public list");
DEFINE_uint32 (start_level, 3, "TPCC3, YCSB2");
DEFINE_string (bg_threads_enabled_idx, "ycsb", "list of idxes that enabled bg threads");
DEFINE_string (dbname, "random", "name of rocksdb");