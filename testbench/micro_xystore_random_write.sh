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
echo ${underlying_storage}

micro_xystore_random_write() {
    rm ssd_file
    touch ssd_file
    rm -rf random
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --is_seq=false \
        --prepare_trace=true \
        --num=800000000 \
        --batch=100 \
        --benchmarks=randomizeworkload,load,stats \
        --report_interval=1 \
        --read=100 \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=2 \
        --public_list_len=15000000 \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --csv_path=./log \
        --cool_pct=10 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --print_tx_console=false >../results/random-write/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_load.data

    mv log.log ../results/random-write/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_load.log
}

micro_xystore_random_write2() {
    rm ssd_file
    touch ssd_file
    rm -rf random
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --is_seq=false \
        --prepare_trace=false \
        --num=640000000 \
        --batch=100 \
        --benchmarks=load2,stats \
        --report_interval=1 \
        --read=100 \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=2 \
        --public_list_len=15000000 \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --csv_path=./log \
        --cool_pct=10 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --print_tx_console=false >../results/random-write/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_load2.data

    mv log.log ../results/random-write/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_load2.log
}

micro_xystore_random_write

# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
sleep 5

# micro_xystore_random_write2