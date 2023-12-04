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

tpcc_leanstore() {
    rm ssd_file
    touch ssd_file
    rm *.csv
    numactl -N 0 sudo ../build/frontend/leanstore_tpcc \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --dram_gib=30 \
        --tpcc_warehouse_count=100 \
        --notpcc_warehouse_affinity \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split \
        --xmerge \
        --print_tx_console \
        --run_for_seconds=600 >../throughput/experiments/paper-tpcc/tpcc_leanstore_30GB_4_thrd_4KB_page.data

    mv log.log ../throughput/experiments/paper-tpcc/tpcc_leanstore_30GB_4_thrd_4KB_page.log
}

tpcc_leanstore2() {
    for i in 4 8 16; do
        rm ssd_file
        touch ssd_file
        rm *.csv
        numactl -N 0 sudo ../build/frontend/leanstore_tpcc \
            --ssd_path=./ssd_file \
            --worker_threads=${i} \
            --pp_threads=4 \
            --dram_gib=30 \
            --tpcc_warehouse_count=100 \
            --notpcc_warehouse_affinity \
            --csv_path=./log \
            --cool_pct=40 \
            --free_pct=1 \
            --contention_split \
            --xmerge \
            --print_tx_console \
            --run_until_tx=100000000 >../results/tpcc/data/tpcc_leanstore_30GB_${i}_thrd_4KB_page_2TX.data

        mv log.log ../results/tpcc/data/tpcc_leanstore_30GB_${i}_thrd_4KB_page_2TX.log
    done
}

tpcc_leanstore2
