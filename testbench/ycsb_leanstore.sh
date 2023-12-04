# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

sleep 5

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme0n1 -r -fs -T --output ../throughput/experiments/paper-ycsb/data-random/leanstore_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSBE_dstat.csv &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 nvme0n1 >../throughput/experiments/paper-ycsb/data-random/leanstore_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSBE_iostat.log &
    PIDPSTATSTAT2=$!
}

ycsb_leanstore() {
    rm ssd_file
    touch ssd_file
    numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
        --is_seq=true \
        --read=20000000 \
        --num=640000000 \
        --benchmarks=load,ycsbd \
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
        --batch=100 >../throughput/experiments/paper-ycsb/data/leanstore_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSBD.data

    mv log.log ../throughput/experiments/paper-ycsb/data/leanstore_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSBD.log
}

# ycsbd,ycsba,ycsbb,ycsbc,ycsbf
ycsb_leanstore_random() {
    database="database_320M_rnd_db"
    numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
        --report_interval=1 \
        --num=0 \
        --batch=100 \
        --read=640000000 \
        --percentile=0.7 \
        --benchmarks=readtrace,readwithdist,ycsbd,ycsba,ycsbb,ycsbc,ycsbf,ycsbe \
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
        --recover_file=${database}.json >../throughput/experiments/paper-ycsb/data_s18/leanstore_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSB_skew_0_7.data

    mv log.log ../throughput/experiments/paper-ycsb/data_s18/leanstore_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSB_skew_0_7.log
}
# ,ycsba,ycsbb,ycsbc,ycsbf,ycsbe
ycsb_leanstore_random_load() {
    rm ssd_file
    touch ssd_file
    rm *.csv
    database="ssd_file"
    numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
        --report_interval=1 \
        --num=0 \
        --batch=100 \
        --read=640000000 \
        --percentile=0.7 \
        --benchmarks=readtrace,createtracewithzipfian,randomizeworkload,load,ycsbd \
        --worker_threads=4 \
        --pp_threads=4 \
        --ssd_path=./${database} \
        --tracefile=database_320M_rnd_db.trc \
        --dram_gib=5 \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split=true \
        --xmerge=true \
        --print_tx_console=true >../results/ycsb/data/leanstore_5GB_memory_4_thrd_4KB_page_YCSB_skew_0_7.data

    mv log.log ../results/ycsb/data/leanstore_5GB_memory_4_thrd_4KB_page_YCSB_skew_0_7.log
}

# collect_pstat_stats
# collect_iostat_stats

ycsb_leanstore_random_load

# kill stats
# set +e
# kill $PIDPSTATSTAT1
# kill $PIDPSTATSTAT2
# set -e
# sudo kill -9 $(pidof iostat)
# sudo kill -9 $(pidof dstat)
# sleep 30
