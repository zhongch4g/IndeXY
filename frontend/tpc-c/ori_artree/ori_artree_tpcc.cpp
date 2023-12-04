
#include "../../shared/types.hpp"
#include "Units.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "../../shared/artlsm_tpcc_schema.hpp"
#include "ori_artree_adapter.hpp"

// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/utils/JumpMU.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
// -------------------------------------------------------------------------------------
using namespace std;
// -------------------------------------------------------------------------------------
DEFINE_uint32 (tpcc_warehouse_count, 1, "");
DEFINE_bool (tpcc_warehouse_affinity, false, "");
DEFINE_bool (tpcc_cross_warehouses, true, "");
DEFINE_bool (tpcc_remove, true, "");
// DEFINE_double (dram_gib, 1, "");
// DEFINE_uint32 (worker_threads, 4, "");

// -------------------------------------------------------------------------------------
// warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item,
// stock

std::vector<std::string> idxes{"warehouse", "district", "customer", "customerwdl",
                               "history",   "neworder", "order",    "order_wdc",
                               "orderline", "item",     "stock"};

ARTLSMAdapter<warehouse_t> warehouse;
ARTLSMAdapter<district_t> district;
ARTLSMAdapter<customer_t> customer;
ARTLSMAdapter<customer_wdl_t> customerwdl;
ARTLSMAdapter<history_t> history;
ARTLSMAdapter<neworder_t> neworder;
ARTLSMAdapter<order_t> order;
ARTLSMAdapter<order_wdc_t> order_wdc;
ARTLSMAdapter<orderline_t> orderline;
ARTLSMAdapter<item_t> item;
ARTLSMAdapter<stock_t> stock;
#include "../../shared/artlsm_tpcc_workload.hpp"

void getMemory (int* currRealMem, int* peakRealMem, int* currVirtMem, int* peakVirtMem) {
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

int currRealMem = 0, peakRealMem = 0, currVirtMem = 0, peakVirtMem = 0;
// -------------------------------------------------------------------------------------
int main (int argc, char** argv) {
    gflags::SetUsageMessage ("ARTLSM TPC-C");
    gflags::ParseCommandLineFlags (&argc, &argv, true);

    chrono::high_resolution_clock::time_point begin, end;

    warehouseCount = FLAGS_tpcc_warehouse_count;
    // -------------------------------------------------------------------------------------
    // create each in-memory index (artree)
    ARTLSM<warehouse_t> warehouse_idx;
    ARTLSM<district_t> district_idx;
    ARTLSM<customer_t> customer_idx;
    ARTLSM<customer_wdl_t> customerwdl_idx;
    ARTLSM<history_t> history_idx;
    ARTLSM<neworder_t> neworder_idx;
    ARTLSM<order_t> order_idx;
    ARTLSM<order_wdc_t> order_wdc_idx;
    ARTLSM<orderline_t> orderline_idx;
    ARTLSM<item_t> item_idx;
    ARTLSM<stock_t> stock_idx;
    // create on-disk index LSM (rocksdb)
    LSM lsm = LSM ();

    warehouse = ARTLSMAdapter<warehouse_t> (warehouse_idx, lsm, "warehouse");
    district = ARTLSMAdapter<district_t> (district_idx, lsm, "district");
    customer = ARTLSMAdapter<customer_t> (customer_idx, lsm, "customer");
    customerwdl = ARTLSMAdapter<customer_wdl_t> (customerwdl_idx, lsm, "customerwdl");
    history = ARTLSMAdapter<history_t> (history_idx, lsm, "history");
    neworder = ARTLSMAdapter<neworder_t> (neworder_idx, lsm, "neworder");
    order = ARTLSMAdapter<order_t> (order_idx, lsm, "order");
    order_wdc = ARTLSMAdapter<order_wdc_t> (order_wdc_idx, lsm, "orderwdc");
    orderline = ARTLSMAdapter<orderline_t> (orderline_idx, lsm, "orderline");
    item = ARTLSMAdapter<item_t> (item_idx, lsm, "item");
    stock = ARTLSMAdapter<stock_t> (stock_idx, lsm, "stock");
    // -------------------------------------------------------------------------------------
    std::vector<thread> threads;
    std::atomic<u32> g_w_id (1);
    printf ("warehouseCount = %u, worker_threads = %u\n", warehouseCount, FLAGS_worker_threads);
    printf (
        "warehouse %lu, district %lu b customer %lu customerwdl %lu history %lu neworder %lu order "
        "%lu order_wdc %lu orderline %lu "
        "item %lu stock %lu\n",
        sizeof (warehouse_t), sizeof (district_t), sizeof (customer_t), sizeof (customer_wdl_t),
        sizeof (history_t), sizeof (neworder_t), sizeof (order_t), sizeof (order_wdc_t),
        sizeof (orderline_t), sizeof (item_t), sizeof (stock_t));
    printf (
        "percentage of orderline = %2.1f\n",
        sizeof (orderline_t) * 1.0 /
            (sizeof (warehouse_t) + sizeof (district_t) + sizeof (customer_t) +
             sizeof (customer_wdl_t) + sizeof (history_t) + sizeof (neworder_t) + sizeof (order_t) +
             sizeof (order_wdc_t) + sizeof (orderline_t) + sizeof (item_t) + sizeof (stock_t)));

    loadItem ();
    loadWarehouse ();
    for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
        threads.emplace_back ([&] () {
            while (true) {
                const u32 w_id = g_w_id++;
                if (w_id > FLAGS_tpcc_warehouse_count) {
                    return;
                }
                jumpmuTry () {
                    loadStock (w_id);
                    loadDistrict (w_id);
                    for (Integer d_id = 1; d_id <= 10; d_id++) {
                        loadCustomer (w_id, d_id);
                        loadOrders (w_id, d_id);
                    }
                }
                jumpmuCatch () { ensure (false); }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join ();
    }
    threads.clear ();
    printf ("==========================================\n");
    printf ("      Finished the warehouse loading \n");
    printf ("==========================================\n");
    // -------------------------------------------------------------------------------------
    getMemory (&currRealMem, &peakRealMem, &currVirtMem, &peakVirtMem);
    printf ("1->%dM,%dM,%dM,%dM\n", currRealMem / 1024, peakRealMem / 1024, currVirtMem / 1024,
            peakVirtMem / 1024);

    // -------------------------------------------------------------------------------------
    atomic<u64> running_threads_counter (0);
    atomic<u64> keep_running (true);
    std::atomic<u64> thread_committed[FLAGS_worker_threads];
    std::atomic<u64> thread_aborted[FLAGS_worker_threads];
    std::atomic<u64> thread_counter[FLAGS_worker_threads];

    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
        thread_counter[t_i] = 0;
    }
    // -------------------------------------------------------------------------------------
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
        thread_committed[t_i] = 0;
        thread_aborted[t_i] = 0;
        // -------------------------------------------------------------------------------------
        threads.emplace_back ([&, t_i] () {
            running_threads_counter++;
            if (FLAGS_pin_threads) {
                leanstore::utils::pinThisThread (t_i);
            }
            while (keep_running) {
                jumpmuTry () {
                    Integer w_id;
                    if (FLAGS_tpcc_warehouse_affinity) {
                        w_id = t_i + 1;
                    } else {
                        w_id = urand (1, FLAGS_tpcc_warehouse_count);
                    }
                    auto neworder_tx = tx (w_id);
                    if (neworder_tx == 4) thread_committed[t_i]++;
                }
                jumpmuCatch () { thread_aborted[t_i]++; }
            }
            running_threads_counter--;
        });
    }
    // -------------------------------------------------------------------------------------
    threads.emplace_back ([&] () {
        running_threads_counter++;
        u64 time = 0;
        cout << "t,tag,tx_committed,tx_aborted" << endl;
        while (keep_running) {
            cout << time++ << "," << FLAGS_tag << ",";
            u64 total_committed = 0, total_aborted = 0;
            for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
                total_committed += thread_committed[t_i].exchange (0);
                total_aborted += thread_aborted[t_i].exchange (0);
                thread_counter[t_i] += total_committed;
            }
            cout << total_committed << "," << total_aborted << endl;
            // ================================================================================
            sleep (1);
        }
        running_threads_counter--;
    });
    sleep (FLAGS_run_for_seconds);
    keep_running = false;
    while (running_threads_counter) {
    }
    for (auto& thread : threads) {
        thread.join ();
    }
    for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
        cout << thread_counter[t_i] << ",";
    }

    return 0;
}
