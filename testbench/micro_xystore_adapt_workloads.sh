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

micro_xystore_adapt_workloads2() {
    database="database_320M_rnd_db"
    for seg_length in 1; do
        for level in 3; do
            numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
                --x_dram_gib=4.5 \
                --dram_gib=0.5 \
                --start_level=${level} \
                --public_list_len=15000000 \
                --report_interval=1 \
                --prepare_trace=false \
                --segment_length=${seg_length} \
                --num=0 \
                --batch=100 \
                --read=640000000 \
                --percentile=0.7 \
                --benchmarks=readtrace,createworkloadshiftingtrace,readworkload4warmup,readworkload1,readworkload2,readworkload3,readworkload4,stats \
                --worker_threads=4 \
                --pp_threads=4 \
                --ssd_path=./${database} \
                --tracefile=${database}.trc \
                --dbname=rocksdb_320M_rnd_db \
                --csv_path=./log \
                --cool_pct=40 \
                --free_pct=1 \
                --contention_split=false \
                --xmerge=false \
                --print_tx_console=false \
                --recover=true \
                --persist=false \
                --recover_file=${database}.json >../results/workload-shifting/data/IndeX_${underlying_storage}_5GB_640M_zipfian_0.7_workload_shifting_read_bit_seg${seg_length}_lvl${level}.data

            mv log.log ../results/workload-shifting/data/IndeX_${underlying_storage}_5GB_640M_zipfian_0.7_workload_shifting_read_bit_seg${seg_length}_lvl${level}.log
            
            sync
            sysctl -q -w vm.drop_caches=3
            echo 3 >/proc/sys/vm/drop_caches
            sleep 5
        done
    done

}

micro_xystore_adapt_workloads3() {
    database="database_320M_rnd_db"
    for seg_length in 5 10; do
        for level in 3; do
            numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
                --x_dram_gib=4.5 \
                --dram_gib=0.5 \
                --start_level=${level} \
                --public_list_len=15000000 \
                --report_interval=1 \
                --prepare_trace=false \
                --segment_length=${seg_length} \
                --num=0 \
                --batch=100 \
                --read=640000000 \
                --percentile=0.7 \
                --benchmarks=readtrace,createworkloadshiftingtrace,readworkload4warmup,readworkload1,readworkload2,readworkload3,readworkload4 \
                --worker_threads=4 \
                --pp_threads=4 \
                --ssd_path=./${database} \
                --tracefile=${database}.trc \
                --dbname=rocksdb_320M_rnd_db \
                --csv_path=./log \
                --cool_pct=40 \
                --free_pct=1 \
                --contention_split=false \
                --xmerge=false \
                --print_tx_console=false \
                --recover=true \
                --persist=false \
                --recover_file=${database}.json >../results/workload-shifting/data/IndeX_${underlying_storage}_5GB_640M_zipfian_0.8_workload_shifting_read_bit_seg${seg_length}_lvl${level}.data

            mv log.log ../results/workload-shifting/data/IndeX_${underlying_storage}_5GB_640M_zipfian_0.8_workload_shifting_read_bit_seg${seg_length}_lvl${level}.log
            
            sync
            sysctl -q -w vm.drop_caches=3
            echo 3 >/proc/sys/vm/drop_caches
            sleep 5
        done
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

    # micro_xystore_adapt_workloads2

    sync
    sysctl -q -w vm.drop_caches=3
    echo 3 >/proc/sys/vm/drop_caches
    sleep 5

    micro_xystore_adapt_workloads3

elif [ "$underlying_storage" = "leanstore" ]; then 
    echo "Using Leanstore"
    sudo cmake -DCMAKE_BUILD_TYPE=release \
    -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..

    sudo make -j8
    cd ..
    cd testbench

    # micro_xystore_adapt_workloads2

    sync
    sysctl -q -w vm.drop_caches=3
    echo 3 >/proc/sys/vm/drop_caches
    sleep 5

    micro_xystore_adapt_workloads3

else
    echo "Invalid engine"
fi


