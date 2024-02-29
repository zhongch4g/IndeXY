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
value_size=(8 32 128 512 1024 1280)
n_requests=(671000000 270000000 79000000 21000000 11000000 8400000)

micro_xystore_random_write() {
    for index in 0 1 2 3 4 5; do
        echo "value size : " ${value_size[${index}]}
        echo "# of requests : " ${n_requests[${index}]}

        sudo rm -rf rocksdb_db
        numactl -N 0 sudo ../build/frontend/rocksdb_ycsb_bench \
            --report_interval=1 \
            --num=${n_requests[${index}]} \
            --benchmarks=randomizeworkload,load_batch \
            --value_size=${value_size[${index}]} \
            --read=100 \
            --ssd_path=rocksdb_db \
            --worker_threads=4 \
            --batch=100 \
            --prepare_trace=true \
            --is_seq=false >../results/value-size/data/rocksdb_5GB_memory_4_thrd_${value_size[${index}]}B.data

        mv log.log ../results/value-size/data/rocksdb_5GB_memory_4_thrd_${value_size[${index}]}B.log
        
        sync
        sysctl -q -w vm.drop_caches=3
        echo 3 >/proc/sys/vm/drop_caches
        sleep 5
    done
}

cd ..
sudo rm -rf build
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=release ..
make -j12
cd ..
cd testbench

sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT


micro_xystore_random_write

