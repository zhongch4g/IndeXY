#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "ARTInterface.hpp"
#include "ARTKVSInterface.hpp"
#include "XYConfig.hpp"
// =====================================================================
// shared headers
// =====================================================================
#include "logger.h"

namespace exdindex {

class XYStore {
    static void getMemory (int* currRealMem, int* peakRealMem, int* currVirtMem, int* peakVirtMem) {
        // stores each word in status file
        char buffer[1024] = "";

        // linux file contains this-process info
        FILE* file = fopen ("/proc/self/status", "r");

        // read the entire file
        while (fscanf (file, " %1023s", buffer) == 1) {
            if (strcmp (buffer, "VmRSS:") == 0) {
                if (!(fscanf (file, " %d", currRealMem) == 3)) {
                    assert (1);
                }
            }
            if (strcmp (buffer, "VmHWM:") == 0) {
                if (!(fscanf (file, " %d", peakRealMem) == 3)) {
                    assert (1);
                }
            }
            if (strcmp (buffer, "VmSize:") == 0) {
                if (!(fscanf (file, " %d", currVirtMem) == 3)) {
                    assert (1);
                }
            }
            if (strcmp (buffer, "VmPeak:") == 0) {
                if (!(fscanf (file, " %d", peakVirtMem) == 3)) {
                    assert (1);
                }
            }
        }
        fclose (file);
    }

    using loadKeyFn = bool (*) (TID tid, Key& key);
    struct GlobalStats {
        uint64_t total_memory_consumption;
        uint64_t accumulated_tx_counter = 0;
    };

public:
    std::atomic<bool> bg_threads_keep_running = true;
    std::unordered_map<std::string, bool> bg_threads_enabled_idx;
    std::unordered_map<std::string, ARTKVS::ARTreeKVS*> artkvss;
    std::unordered_map<std::string, ART_INTERFACE::ARTree*> artt;
    std::atomic<bool> keep_running = true;
    std::atomic<bool> bg_threads_switch = true;
    GlobalStats global_stats;

public:
    double totalMemoryUsage = 0;
    double totalMemoryUsage2 = 0;
    double memoryUsageMonitor = 0;
    uint64_t totalReleased = 0, totalUnloaded = 0, totalDiskRead = 0;
    uint64_t tempReleased = 0, tempUnloaded = 0, tempDiskRead = 0;
    int currRealMem = 0, peakRealMem = 0, currVirtMem = 0, peakVirtMem = 0;

public:
    XYStore ();
    ~XYStore ();
    void backgroundThreadsManager ();
    ARTKVS::ARTreeKVS* registerARTKVS (loadKeyFn loadkey, getObjectFn fn1, getRidFn fn2,
                                       releaseValueFn fn3, std::string name);
    ART_INTERFACE::ARTree* registerARTree (loadKeyFn loadkey, getObjectFn fn1, getRidFn fn2,
                                           releaseValueFn fn3, std::string name);
    void artkvsMonitor ();
    void artkvsStopMonitor ();
    void artkvsStatistics ();
    void memoryUsage ();
    GlobalStats getGlobalStats ();

#ifdef WITH_LEANSTORE
    leanstore::LeanStore leanstore_db;
#endif
#ifdef WITH_ROCKSDB
    rocksdb::DB* rocksdb_db;
#endif
#ifdef MULTI_KVS
    leanstore::LeanStore leanstore_db;
    rocksdb::DB* rocksdb_db;
#endif
};

};  // namespace exdindex