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

micro_leanstore_sequential_write() {
    rm ssd_file
    touch ssd_file
    numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
        --num=800000000 \
        --benchmarks=readtrace,load \
        --tracefile=trace_rnd_48bit.trc \
        --report_interval=1 \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --dram_gib=5 \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split=true \
        --xmerge=true \
        --print_tx_console=true \
        --batch=100 \
        --prepare_trace=false \
        --is_seq=false >../results/sequential-write/data/leanstore_5GB_memory_4_thrd_4KB_page_seq_write.data

    mv log.log ../results/sequential-write/data/leanstore_5GB_memory_4_thrd_4KB_page_seq_write.log
}

micro_leanstore_sequential_write
