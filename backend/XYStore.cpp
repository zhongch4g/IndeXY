#include "XYStore.hpp"

namespace exdindex {

XYStore::XYStore () {
    // 1. Initialize background threads enabled indexes
    const char* bg_threads_enabled_idxes = FLAGS_bg_threads_enabled_idx.c_str ();
    while (bg_threads_enabled_idxes) {
        const char* sep = strchr (bg_threads_enabled_idxes, ',');
        std::string name;
        if (sep == nullptr) {
            name = bg_threads_enabled_idxes;
            bg_threads_enabled_idxes = nullptr;
        } else {
            name = std::string (bg_threads_enabled_idxes, sep - bg_threads_enabled_idxes);
            bg_threads_enabled_idxes = sep + 1;
        }
        bg_threads_enabled_idx[name] = true;
        INFO ("Enable background threads for %s", name.c_str ());
    }
}

XYStore::~XYStore () {
    bg_threads_keep_running = false;
    memoryUsage ();
    for (auto artk : artkvss) {
        artk.second->stopBackgroundThreads ();
        // artk.second->ReleaseX (); // Delete the ART
    }
}

XYStore::GlobalStats XYStore::getGlobalStats () { return global_stats; }

void XYStore::backgroundThreadsManager () {
    // Determine which indexes need to be unloaded
    // Current: Specify only one index
    // Future work: Use one big node connect all the indexes
    INFO ("Background threads manager \n");
    std::thread monitor_thread ([&] () {
        while (bg_threads_keep_running) {
            memoryUsageMonitor = 0;
            for (auto artk : artkvss) {
                memoryUsageMonitor += \ 
                (artk.second->idxMemoryUsageInfo () + artk.second->vMemoryInfo ()) /
                                      1024.0 / 1024.0;
            }
            INFO ("[MemoryUsageMonitor] %2.1f MB\n", memoryUsageMonitor);
            std::this_thread::sleep_for (std::chrono::milliseconds (1000));
        }
    });
    monitor_thread.detach ();
}

ARTKVS::ARTreeKVS* XYStore::registerARTKVS (loadKeyFn loadkey, getObjectFn fn1, getRidFn fn2,
                                            releaseValueFn fn3, std::string name) {
    // Create the artree and kvs combination
#ifdef WITH_LEANSTORE
    INFO ("IndeXY(LeanStore) Registered ..");
    artkvss[name] = new ARTKVS::ARTreeKVS (leanstore_db, fn1, fn2, fn3, name);
#endif
#ifdef WITH_ROCKSDB
    INFO ("IndeXY(RocksDB) Registered ..");
    artkvss[name] = new ARTKVS::ARTreeKVS (rocksdb_db, fn1, fn2, fn3, name);
#endif
#ifdef MULTI_KVS
    artkvss[name] = new ARTKVS::ARTreeKVS (leanstore_db, rocksdb_db, fn1, fn2, fn3, name);
#endif
    if (artkvss[name] == nullptr) {
        perror ("Bad Initialization ..\n");
        exit(1);
    }
    artkvss[name]->CreateARTreeKVS (loadkey, fn1, fn2, fn3, name);
    return artkvss[name];
}

ART_INTERFACE::ARTree* XYStore::registerARTree (loadKeyFn loadkey, getObjectFn fn1, getRidFn fn2,
                                                releaseValueFn fn3, std::string name) {
    artt[name] = new ART_INTERFACE::ARTree ();
    artt[name]->CreateARTree (loadkey, fn1, fn2, fn3, name);
    INFO ("Create ARTree");
    return artt[name];
}

void XYStore::artkvsMonitor () {
    std::thread monitor_thread ([&] () {
        double MEMORY_LIMIT = FLAGS_x_dram_gib * 1024;
        INFO ("[MEMORY_LIMIT] ART %2.4f MB KVS %2.4f MB", MEMORY_LIMIT, FLAGS_dram_gib * 1024.0);
        while (bg_threads_keep_running) {
            totalMemoryUsage = 0;
            for (auto artk : artkvss) {
                totalMemoryUsage += \ 
                (artk.second->idxMemoryUsageInfo () + artk.second->vMemoryInfo ()) /
                                    1024.0 / 1024.0;
            }

            if (totalMemoryUsage >= (MEMORY_LIMIT * 0.95)) {
                keep_running = false;
                for (auto artk : artkvss) {
                    artk.second->ARTcache = false;
                }
            } else {
                keep_running = true;
                for (auto artk : artkvss) {
                    artk.second->ARTcache = true;
                }
            }

            if (totalMemoryUsage >= (MEMORY_LIMIT * 0.95)) {
                for (auto idx : bg_threads_enabled_idx) {
                    artkvss[idx.first]->release_keep_running = true;
                }
            } else if (totalMemoryUsage < (MEMORY_LIMIT * 0.94)) {
                for (auto idx : bg_threads_enabled_idx) {
                    artkvss[idx.first]->release_keep_running = false;
                }
            }

            if (bg_threads_switch && (totalMemoryUsage > (MEMORY_LIMIT * 0.95))) {
                bg_threads_switch.store (false);
                INFO ("[Start the BG Thread]\n");
                INFO ("[TotalMemoryUsage] %2.1f MB\n", totalMemoryUsage);
                for (auto idx : bg_threads_enabled_idx) {
                    artkvss[idx.first]->startBackgroundThreads ();
                }
            }
            std::this_thread::sleep_for (std::chrono::microseconds (100));
        }
    });
    monitor_thread.detach ();
}

void XYStore::artkvsStopMonitor () {
    INFO ("[Stop the BG Thread]\n");
    INFO ("[TotalMemoryUsage] %2.1f MB\n", totalMemoryUsage);
    for (auto idx : bg_threads_enabled_idx) {
        artkvss[idx.first]->stopBackgroundThreads ();
    }
}

void XYStore::artkvsStatistics () {
    std::thread monitor_thread2 ([&] () {
        double MEMORY_LIMIT = FLAGS_x_dram_gib * 1024;
        INFO ("[Statistics] ART %2.4f MB KVS %2.4f MB", MEMORY_LIMIT, FLAGS_dram_gib * 1024.0);
        while (bg_threads_keep_running) {
            totalMemoryUsage2 = 0;
            tempReleased = 0, tempUnloaded = 0, tempDiskRead = 0;
            for (auto artk : artkvss) {
                totalMemoryUsage2 += \ 
                (artk.second->idxMemoryUsageInfo () + artk.second->vMemoryInfo ()) /
                                    1024.0 / 1024.0;
                tempReleased += artk.second->release_counter;
                tempUnloaded += artk.second->unload_counter;
                tempDiskRead += artk.second->diskread_;
            }
            uint64_t releaseGap = tempReleased - totalReleased;
            uint64_t unloadGap = tempUnloaded - totalUnloaded;
            uint64_t diskReadGap = tempDiskRead - totalDiskRead;

            totalReleased = tempReleased;
            totalUnloaded = tempUnloaded;
            totalDiskRead = tempDiskRead;
            INFO ("[Counters] %lu %lu %lu %lu %lu %lu\n", tempReleased, tempUnloaded,
            tempDiskRead, releaseGap, unloadGap, diskReadGap);

            bool is_release = false;
            for (auto idx : bg_threads_enabled_idx) {
                is_release = is_release || artkvss[idx.first]->release_keep_running;
            }
            INFO ("[TotalMemoryUsage] %2.1f MB RELEASE %d\n", totalMemoryUsage2, is_release);

            getMemory (&currRealMem, &peakRealMem, &currVirtMem, &peakVirtMem);
            INFO ("[RSS] %dMB,%dMB,%dMB,%dMB\n", currRealMem / 1024, peakRealMem / 1024,
                  currVirtMem / 1024, peakVirtMem / 1024);
            std::this_thread::sleep_for (std::chrono::milliseconds (1000));
        }
    });
    monitor_thread2.detach ();
}

void XYStore::memoryUsage () {
    double memoryUsage = 0;
    for (auto artk : artkvss) {
        INFO ("%s : %s", artk.first.c_str (), artk.second->ToStats ().c_str ())
        memoryUsage += \ 
                (artk.second->idxMemoryUsageInfo () + artk.second->vMemoryInfo ()) /
                       1024.0 / 1024.0;
    }
    INFO ("IdxVal %2.5f KVS %2.5f", memoryUsage, FLAGS_dram_gib);
}

};  // namespace exdindex