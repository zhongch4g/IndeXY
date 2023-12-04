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

###
# 671 million 8Byte value
# 270 million 32Byte value
# 79 million 128Byte value
# 21 million 512Byte value
# 11 million 1024Byte value
# 8.4 million 1280Byte value
underlying_storage=$1
value_size=(8 32 128 512 1024 1280)
n_requests=(671000000 270000000 79000000 21000000 11000000 8400000)

micro_xystore_random_write() {
    for index in 0 1 2 3 4 5; do
        page_size=4 # modify this page_size mannully
        echo "value size : " ${value_size[${index}]}
        echo "# of requests : " ${n_requests[${index}]}

        cd ..
        sudo rm -rf build
        mkdir build
        cd build
        if [ "$underlying_storage" = "rocksdb" ]; then
            echo "Using Rocksdb"
            sudo cmake -DCMAKE_BUILD_TYPE=release \
            -DCMAKE_CONFIG_VALUE_SIZE=${value_size[${index}]} \
            -DWITH_LEANSTORE=OFF -DWITH_ROCKSDB=ON ..
        elif [ "$underlying_storage" = "leanstore" ]; then 
            echo "Using Leanstore"
            sudo cmake -DCMAKE_BUILD_TYPE=release \
            -DCMAKE_CONFIG_VALUE_SIZE=${value_size[${index}]} \
            -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..
        else
            echo "Invalid engine"
        fi

        sudo make -j20
        cd ..
        cd testbench

        rm ssd_file
        touch ssd_file
        rm -rf random
        numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
            --x_dram_gib=4.5 \
            --dram_gib=0.5 \
            --start_level=2 \
            --public_list_len=15000000 \
            --report_interval=1 \
            --num=${n_requests[${index}]} \
            --benchmarks=randomizeworkload,load,stats \
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
            --prepare_trace=true \
            --is_seq=false >../results/value-size/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_4KB_page_${value_size[${index}]}B.data

        mv log.log ../results/value-size/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_4KB_page_${value_size[${index}]}B.log
        
        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 5
    done
}

micro_xystore_random_write_vary_page_size() {
    for page_size in 8 16; do

        cd ..
        sudo rm -rf build
        mkdir build
        cd build
        if [ "$underlying_storage" = "rocksdb" ]; then
            echo "Using Rocksdb"
            sudo cmake -DCMAKE_BUILD_TYPE=release \
            -DWITH_LEANSTORE=OFF -DWITH_ROCKSDB=ON ..
        elif [ "$underlying_storage" = "leanstore" ]; then 
            echo "Using Leanstore"
            sudo cmake -DCMAKE_BUILD_TYPE=release \
            -DCMAKE_CONFIG_PAGE_SIZE=${page_size} \
            -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..
        else
            echo "Invalid engine"
        fi

        sudo make -j20
        cd ..
        cd testbench

        rm ssd_file
        touch ssd_file
        rm -rf random
        numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
            --x_dram_gib=4.5 \
            --dram_gib=0.5 \
            --start_level=2 \
            --public_list_len=15000000 \
            --report_interval=1 \
            --num=640000000 \
            --benchmarks=randomizeworkload,load,stats \
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
            --prepare_trace=true \
            --is_seq=false >../results/value-size/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_${page_size}KB_page_8B.data

        mv log.log ../results/value-size/data/IndeX_${underlying_storage}_5GB_memory_4_thrd_${page_size}KB_page_8B.log
        
        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 5
    done
}

micro_xystore_random_write
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
sleep 5
micro_xystore_random_write_vary_page_size

