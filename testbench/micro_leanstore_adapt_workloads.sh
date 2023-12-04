# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

sleep 5

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme0n1 -r -fs -T --output ../stat/ycsb_artlsm_random_480m_1th_2GB_dstat.csv 1 &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 >../stat/ycsb_artlsm_random_480m_1th_2GB_iostat.log &
    PIDPSTATSTAT2=$!
}

micro_leanstore_adapt_workloads2() {
    database="database_320M_rnd_db"

    for seg_length in 1 5 10 15 20; do
        numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
            --report_interval=1 \
            --prepare_trace=false \
            --segment_length=${seg_length} \
            --num=0 \
            --batch=100 \
            --read=640000000 \
            --percentile=0.7 \
            --benchmarks=readtrace,createworkloadshiftingtrace,readworkload4,readworkload1,readworkload2,readworkload3,readworkload4 \
            --worker_threads=4 \
            --pp_threads=4 \
            --ssd_path=./${database} \
            --tracefile=${database}.trc \
            --dram_gib=5 \
            --csv_path=./log \
            --cool_pct=40 \
            --free_pct=1 \
            --contention_split=false \
            --xmerge=false \
            --print_tx_console=true \
            --recover=true \
            --persist=false \
            --recover_file=${database}.json > ../results/workload-shifting/data/ycsb_leanstore_5GB_640M_zipfan_0.7_workload_shifting_seg${seg_length}.data

        mv log.log ../results/workload-shifting/data/ycsb_leanstore_5GB_640M_zipfan_0.7_workload_shifting_seg${seg_length}.log
        # mv log_dt.csv ../throughput/experiments/paper-adapt-workloads/data/ycsb_leanstore_5GB_640M_zipfan_0.8_log_dt.csv
        # mv log_cr.csv ../throughput/experiments/paper-adapt-workloads/data/ycsb_leanstore_5GB_640M_zipfan_0.8_log_cr.csv

        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 5
    done
}

micro_leanstore_adapt_workloads3() {
    database="database_320M_rnd_db"

    for seg_length in 10 5 1; do
        numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
            --report_interval=1 \
            --prepare_trace=false \
            --segment_length=${seg_length} \
            --num=0 \
            --batch=100 \
            --read=640000000 \
            --percentile=0.8 \
            --benchmarks=readtrace,createworkloadshiftingtrace,readworkload4,readworkload1,readworkload2,readworkload3,readworkload4 \
            --worker_threads=4 \
            --pp_threads=4 \
            --ssd_path=./${database} \
            --tracefile=${database}.trc \
            --dram_gib=5 \
            --csv_path=./log \
            --cool_pct=40 \
            --free_pct=1 \
            --contention_split=false \
            --xmerge=false \
            --print_tx_console=true \
            --recover=true \
            --persist=false \
            --recover_file=${database}.json > ../results/workload-shifting/data/ycsb_leanstore_5GB_640M_zipfan_0.8_workload_shifting_seg${seg_length}.data

        mv log.log ../results/workload-shifting/data/ycsb_leanstore_5GB_640M_zipfan_0.8_workload_shifting_seg${seg_length}.log
        # mv log_dt.csv ../throughput/experiments/paper-adapt-workloads/data/ycsb_leanstore_5GB_640M_zipfan_0.8_log_dt.csv
        # mv log_cr.csv ../throughput/experiments/paper-adapt-workloads/data/ycsb_leanstore_5GB_640M_zipfan_0.8_log_cr.csv

        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 5
    done
}

# micro_leanstore_adapt_workloads2

# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches

sleep 5

micro_leanstore_adapt_workloads3
 