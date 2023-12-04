# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme0n1 -r -fs -T --output ../stat/ycsb_artlsm_random_480m_1th_2GB_dstat.csv 1 &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 >../stat/ycsb_artlsm_random_480m_1th_2GB_iostat.log &
    PIDPSTATSTAT2=$!
}

micro_original_art_random_write() {
    numactl -N 0 sudo ../build/frontend/original_artree_ycsb_bench \
        --report_interval=1 \
        --num=640000000 \
        --benchmarks=load2,stats \
        --read=100 \
        --worker_threads=4 \
        --batch=10 \
        --print_tx_console=false \
        --prepare_trace=false \
        --ssd_path=./ssd_file \
        --tracefile=RandomGenerateFile \
        --percentile=0.9
    # > ../throughput/experiments/paper-ycsb-random-write/data/original_art_5GB_memory_4_thrd_vm_48bit.data

    # mv log.log ../throughput/experiments/paper-ycsb-random-write/data/original_art_5GB_memory_4_thrd_vm_48bit.log
}

micro_original_art_random_write

# # kill stats
# set +e
# kill -n 9 $PIDPSTATSTAT1
# kill -n 9 $PIDPSTATSTAT2
# set -e

sleep 1
