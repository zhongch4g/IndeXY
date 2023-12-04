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

tpcc_ExdIndex() {
    rm ssd_file
    touch ssd_file
    rm *.csv
    rm -rf random
    rm -rf artree_*
    numactl -N 0 sudo ../build/frontend/xystore_tpcc \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --x_dram_gib=25 \
        --dram_gib=5 \
        --bg_threads_enabled_idx=orderline \
        --start_level=3 \
        --public_list_len=15000000 \
        --tpcc_warehouse_count=100 \
        --notpcc_warehouse_affinity \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split \
        --xmerge \
        --print_tx_console \
        --run_for_seconds=600 >../throughput/experiments/paper-tpcc/tpcc_exdIndex_ARTR_30GB_4_thrd_4KB_page.data

    mv log.log ../throughput/experiments/paper-tpcc/tpcc_exdIndex_ARTR_30GB_4_thrd_4KB_page.log
}

tpcc_ExdIndex2() {
    for i in 2 4 8 16; do
        rm ssd_file
        touch ssd_file
        rm *.csv
        rm -rf random
        rm -rf artree_*
        numactl -N 0 sudo ../build/frontend/xystore_tpcc \
            --ssd_path=./ssd_file \
            --worker_threads=${i} \
            --pp_threads=4 \
            --x_dram_gib=29.5 \
            --dram_gib=0.5 \
            --bg_threads_enabled_idx=orderline \
            --dbname=tpcc \
            --start_level=3 \
            --public_list_len=15000000 \
            --tpcc_warehouse_count=100 \
            --notpcc_warehouse_affinity \
            --csv_path=./log \
            --cool_pct=40 \
            --free_pct=1 \
            --contention_split \
            --xmerge \
            --print_tx_console \
            --run_for_seconds=600 >../results/tpcc/data/tpcc_IndeX_${underlying_storage}_30GB_${i}_thrd_4KB_page_2TX.data

        mv log.log ../results/tpcc/data/tpcc_IndeX_${underlying_storage}_30GB_${i}_thrd_4KB_page_2TX.log
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

elif [ "$underlying_storage" = "leanstore" ]; then 
    echo "Using Leanstore"
    sudo cmake -DCMAKE_BUILD_TYPE=release \
    -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..

    sudo make -j8
    cd ..
    cd testbench

else
    echo "Invalid engine"
fi

tpcc_ExdIndex2
