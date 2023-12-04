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

# ART L
micro_xystore_random_read_artl() {
    database="database_320M_rnd_db"
    for cnst in 0.99 0.95 0.9 0.8 0.7 0.6 0.5; do
        echo ${cnst}
        numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
            --x_dram_gib=4.5 \
            --dram_gib=0.5 \
            --start_level=3 \
            --public_list_len=15000000 \
            --report_interval=1 \
            --num=0 \
            --batch=100 \
            --read=640000000 \
            --percentile=$cnst \
            --benchmarks=readtrace,createtracewithzipfian,readwithwarmup,readcustomize,stats \
            --worker_threads=4 \
            --pp_threads=4 \
            --ssd_path=./${database} \
            --tracefile=${database}.trc \
            --csv_path=./log \
            --cool_pct=40 \
            --free_pct=1 \
            --contention_split=false \
            --xmerge=false \
            --print_tx_console=false \
            --recover=true \
            --persist=false \
            --recover_file=${database}.json >../results/vary-skewness/data/IndeX_leanstore_5GB_640M_zipfian_${cnst}.data

        mv log.log ../results/vary-skewness/data/IndeX_leanstore_5GB_640M_zipfian_${cnst}.log
    done
}

micro_xystore_random_read_artr() {
    # 0.99 0.97 0.93 0.9 0.8 0.6 0.5 0.3 0.1
    database="rocksdb_320M_rnd_db"
    # for cnst in 0.99 0.95 0.9 0.8 0.7 0.6 0.5; do
    for cnst in 0.99 0.95 0.9 0.8 0.7 0.6 0.5; do
        numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
            --x_dram_gib=4.5 \
            --dram_gib=0.5 \
            --start_level=3 \
            --public_list_len=15000000 \
            --report_interval=1 \
            --num=0 \
            --batch=100 \
            --read=640000000 \
            --percentile=$cnst \
            --dbname=${database} \
            --benchmarks=readtrace,createtracewithzipfian,readwithwarmup,readcustomize,stats \
            --worker_threads=4 \
            --tracefile=database_320M_rnd_db.trc \
            --print_tx_console=false >../results/vary-skewness/data/IndeX_rocksdb_5GB_640M_zipfian_${cnst}.data

        mv log.log ../results/vary-skewness/data/IndeX_rocksdb_5GB_640M_zipfian_${cnst}.log
    done
}

micro_xystore_random_read_one_page_artl() {
    database="database_320M_rnd_db"
    # 4MB (262144) 8MB (524288) 16MB (1048576) 20MB 24MB 28MB 32MB (2097152)
    # for i in 200000 400000 600000 800000 1000000 1100000 1200000 1300000 1400000 2000000 32000000 64000000 110000000 119000000 119500000 120000000 125000000 130000000; do
    for i in 160000000 170000000 180000000 190000000 200000000; do
        numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
            --x_dram_gib=4.5 \
            --dram_gib=0.5 \
            --start_level=3 \
            --public_list_len=15000000 \
            --report_interval=1 \
            --num=0 \
            --batch=100 \
            --read=$i \
            --benchmarks=readtrace,createtracewithinterval,randomizeworkload,readwithwarmup,readcustomize,readcustomize,readcustomize \
            --worker_threads=4 \
            --pp_threads=4 \
            --ssd_path=./${database} \
            --tracefile=${database}.trc \
            --csv_path=./log \
            --cool_pct=40 \
            --free_pct=1 \
            --contention_split=false \
            --xmerge=false \
            --print_tx_console=false \
            --recover=true \
            --persist=false \
            --recover_file=${database}.json > ../results/working-set-size/data/IndeX_leanstore_5GB_one_page_${i}.data

        mv log.log ../results/working-set-size/data/IndeX_leanstore_5GB_one_page_${i}.log
    done
}

micro_xystore_random_read_one_page_artr() {
    database="rocksdb_320M_rnd_db"
    # 4MB (262144) 8MB (524288) 16MB (1048576) 20MB 24MB 28MB 32MB (2097152)
    # for i in 200000 400000 600000 800000 1000000 1100000 1200000 1300000 1400000 2000000 32000000 64000000 110000000 119000000 119500000 120000000 125000000 130000000 160000000 170000000 180000000 190000000 200000000; do
    for i in 200000 400000 600000 800000 1000000 1100000 1200000 1300000 1400000 2000000 32000000 64000000 110000000 119000000 119500000 120000000 125000000 130000000 160000000 170000000 180000000 190000000 200000000; do
        numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
            --x_dram_gib=4.5 \
            --dram_gib=0.5 \
            --start_level=3 \
            --public_list_len=15000000 \
            --report_interval=1 \
            --num=0 \
            --batch=100 \
            --read=$i \
            --dbname=${database} \
            --benchmarks=readtrace,createtracewithinterval,randomizeworkload,readwithwarmup,stats,readcustomize,readcustomize,readcustomize \
            --worker_threads=4 \
            --tracefile=database_320M_rnd_db.trc \
            --print_tx_console=false > ../results/working-set-size/data/IndeX_rocksdb_5GB_one_page_${i}.data

        mv log.log ../results/working-set-size/data/IndeX_rocksdb_5GB_one_page_${i}.log
    done
}

cd ..
sudo rm -rf build
mkdir build
cd build
if [ "$underlying_storage" = "rocksdb" ]; then
    echo "Using Rocksdb"
    sudo cmake -DCMAKE_BUILD_TYPE=release \
    -DWITH_LEANSTORE=OFF -DWITH_ROCKSDB=ON ..

    sudo make -j8
    cd ..
    cd testbench
    pwd

    micro_xystore_random_read_one_page_artr

    # clear the cache
    sync
    sysctl -q -w vm.drop_caches=3
    echo 3 >/proc/sys/vm/drop_caches

    micro_xystore_random_read_artr

elif [ "$underlying_storage" = "leanstore" ]; then 
    echo "Using Leanstore"
    sudo cmake -DCMAKE_BUILD_TYPE=release \
    -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..

    sudo make -j8
    cd ..
    cd testbench

    micro_xystore_random_read_one_page_artl

    # clear the cache
    sync
    sysctl -q -w vm.drop_caches=3
    echo 3 >/proc/sys/vm/drop_caches

    micro_xystore_random_read_artl

else
    echo "Invalid engine"
fi
