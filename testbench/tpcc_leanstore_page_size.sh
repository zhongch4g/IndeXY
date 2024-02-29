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

tpcc_leanstore2() {
    for page_size in 4 8 16; do
        cd ..
        sudo rm -rf build
        mkdir build
        cd build
        sudo cmake -DCMAKE_BUILD_TYPE=release -DCMAKE_CONFIG_PAGE_SIZE=${page_size} ..
        sudo make -j4
        cd ..
        cd testbench

        rm ssd_file
        touch ssd_file
        rm *.csv
        numactl -N 0 sudo ../build/frontend/leanstore_tpcc \
            --ssd_path=./ssd_file \
            --worker_threads=8 \
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
            --run_until_tx=60000000 >../results/tpcc/data/tpcc_leanstore_30GB_8_thrd_${page_size}KB_page_2TX_pg.data

        mv log.log ../results/tpcc/data/tpcc_leanstore_30GB_8_thrd_${page_size}KB_page_2TX_pg.log

        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 5
    done
}

tpcc_leanstore2
