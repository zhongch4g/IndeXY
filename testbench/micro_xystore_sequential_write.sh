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

underlying_storage=$1

micro_xystore_seqential_write() {
    rm ssd_file
    touch ssd_file
    rm -rf random
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=2 \
        --public_list_len=15000000 \
        --report_interval=1 \
        --num=800000000 \
        --benchmarks=readtrace,load,stats \
        --tracefile=trace_rnd_48bit.trc \
        --read=100 \
        --ssd_path=./ssd_file \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split=true \
        --xmerge=true \
        --worker_threads=4 \
        --pp_threads=4 \
        --batch=10 \
        --print_tx_console=false \
        --prepare_trace=false \
        --is_seq=false >../results/sequential-write/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_seq_write.data

    mv log.log ../results/sequential-write/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_seq_write.log
}

micro_xystore_seqential_write
