#include "../../shared/types.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "../../shared/artlsm_tpcc_schema.hpp"
#include "xystore_tpcc_adapter.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>

#include "PerfEvent.hpp"
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
DEFINE_uint64 (run_until_tx, 0, "");

// -------------------------------------------------------------------------------------
// warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item,
// stock

std::vector<std::string> idxes{"warehouse", "district", "customer", "customerwdl",
                               "history",   "neworder", "order",    "order_wdc",
                               "orderline", "item",     "stock"};

StructureXY<warehouse_t> warehouse;
StructureXY<district_t> district;
StructureXY<customer_t> customer;
StructureXY<customer_wdl_t> customerwdl;
StructureXY<history_t> history;
StructureXY<neworder_t> neworder;
StructureXY<order_t> order;
StructureXY<order_wdc_t> order_wdc;
StructureXY<orderline_t> orderline;
StructureXY<item_t> item;
StructureXY<stock_t> stock;
#include "../../shared/artlsm_tpcc_workload.hpp"

// -------------------------------------------------------------------------------------
int main (int argc, char** argv) {
    gflags::SetUsageMessage ("ExdIndex TPC-C");
    gflags::ParseCommandLineFlags (&argc, &argv, true);
    INFO ("ExdIndex TPC-C");
    DEBUG ("ExdIndex TPC-C");
    chrono::high_resolution_clock::time_point begin, end;
    warehouseCount = FLAGS_tpcc_warehouse_count;
    // -------------------------------------------------------------------------------------
    std::shared_ptr<XYStore> xystore = std::make_shared<XYStore> ();

    warehouse = StructureXY<warehouse_t> ("warehouse", xystore);
    district = StructureXY<district_t> ("district", xystore);
    customer = StructureXY<customer_t> ("customer", xystore);
    customerwdl = StructureXY<customer_wdl_t> ("customerwdl", xystore);
    history = StructureXY<history_t> ("history", xystore);
    neworder = StructureXY<neworder_t> ("neworder", xystore);
    order = StructureXY<order_t> ("order", xystore);
    order_wdc = StructureXY<order_wdc_t> ("order_wdc", xystore);
    orderline = StructureXY<orderline_t> ("orderline", xystore);
    item = StructureXY<item_t> ("item", xystore);
    stock = StructureXY<stock_t> ("stock", xystore);

    xystore->backgroundThreadsManager ();
    xystore->artkvsMonitor ();
    xystore->artkvsStatistics ();

    std::vector<thread> threads;
    std::atomic<u32> g_w_id (1);
    INFO ("WH %u WT %u\n", warehouseCount, FLAGS_worker_threads);
    INFO (
        "warehouse %lu, district %lu b customer %lu customerwdl %lu history %lu neworder %lu order "
        "%lu order_wdc %lu orderline %lu "
        "item %lu stock %lu\n",
        sizeof (warehouse_t), sizeof (district_t), sizeof (customer_t), sizeof (customer_wdl_t),
        sizeof (history_t), sizeof (neworder_t), sizeof (order_t), sizeof (order_wdc_t),
        sizeof (orderline_t), sizeof (item_t), sizeof (stock_t));
    INFO ("Loading the warehouse .. \n");

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
    xystore->memoryUsage ();
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
                    // stop the TXs
                    if (xystore->keep_running) {
                        tx (w_id);
                        thread_committed[t_i]++;
                    }
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
            sleep (1);
        }
        running_threads_counter--;
    });
    // Shutdown threads
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
    xystore->memoryUsage ();
    return 0;
}
