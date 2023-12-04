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

micro_leanstore_random_read() {
    database="database_320M_rnd_db"
    for cnst in 0.99 0.95 0.9 0.5; do
        numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
            --report_interval=1 \
            --num=0 \
            --batch=100 \
            --read=640000000 \
            --percentile=$cnst \
            --benchmarks=readtrace,createtracewithzipfian,readcustomize,readcustomize \
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
            --recover_file=${database}.json >../results/vary-skewness/data/ycsb_leanstore_5GB_4_thrd_640M_zipfan_${cnst}_without_warmup.data

        mv log.log ../results/vary-skewness/data/ycsb_leanstore_5GB_4_thrd_640M_zipfan_${cnst}_without_warmup.log
        mv log_dt.csv ../results/vary-skewness/data/ycsb_leanstore_5GB_4_thrd_640M_zipfan_${cnst}_log_dt_without_warmup.csv
        mv log_cr.csv ../results/vary-skewness/data/ycsb_leanstore_5GB_4_thrd_640M_zipfan_${cnst}_log_cr_without_warmup.csv

        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 10
    done
}

micro_leanstore_random_read_one_page() {
    database="database_320M_rnd_db"
    # 1MB(65536) 2MB (131072) 4MB (262144) 8MB (524288) 16MB (1048576) 32MB (2097152) 64MB (4194304) 128MB (8388608)
    for i in 200000 400000 600000 800000 1000000 1100000 1200000 1300000 1400000 2000000 32000000 64000000 110000000 119000000 119500000 120000000 125000000 130000000; do
        numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
            --report_interval=1 \
            --num=0 \
            --batch=100 \
            --read=$i \
            --benchmarks=readtrace,createtracewithinterval,randomizeworkload,readcustomize,readcustomize,readcustomize,readcustomize,readcustomize \
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
            --print_tx_console=false \
            --recover=true \
            --persist=false \
            --recover_file=${database}.json > ../results/working-set-size/data/leanstore_5GB_4_thrd_working_set_size_${i}.data

        mv log.log ../results/working-set-size/data/leanstore_5GB_4_thrd_working_set_size_${i}.log
    done

}

micro_leanstore_random_read_one_page

# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches

sleep 5

micro_leanstore_random_read
