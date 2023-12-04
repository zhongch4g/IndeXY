# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

sleep 5

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme0n1 -r -fs -T --output ../throughput/experiments/paper-ycsb/data-random-extra-update1/exdindex_artl_valuesize_8_5GB_memory_4_thrd_4KB_page_dstat.csv &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 nvme0n1 >../throughput/experiments/paper-ycsb/data-random-extra-update1/exdindex_artl_valuesize_8_5GB_memory_4_thrd_4KB_page_iostat.log &
    PIDPSTATSTAT2=$!
}

underlying_storage=$1

ycsb_xystore() {
    rm ssd_file
    touch ssd_file
    rm -rf artree_*
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --is_seq=true \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=2 \
        --public_list_len=15000000 \
        --report_interval=1 \
        --num=640000000 \
        --read=20000000 \
        --benchmarks=load,stats,ycsbd \
        --ssd_path=./ssd_file \
        --csv_path=./log \
        --cool_pct=10 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --worker_threads=4 \
        --pp_threads=4 \
        --batch=100 \
        --print_tx_console=false >../throughput/experiments/paper-ycsb/data/ycsb_IndeX_${underlying_storage}_valuesize_8_5GB_memory_4_thrd_YCSBD.data

    mv log.log ../throughput/experiments/paper-ycsb/data/ycsb_IndeX_${underlying_storage}_valuesize_8_5GB_memory_4_thrd_YCSBD.log
}
#,ycsbd,ycsba,ycsbb,ycsbc,ycsbf,ycsbe,stats
ycsb_xystore_random_artl() {
    rm ssd_file
    touch ssd_file
    database="ssd_file"
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=3 \
        --public_list_len=15000000 \
        --report_interval=1 \
        --num=0 \
        --batch=100 \
        --read=640000000 \
        --percentile=0.7 \
        --benchmarks=readtrace,createtracewithzipfian,randomizeworkload,load,ycsbd,ycsba,ycsbb,ycsbc,ycsbf,ycsbe,stats \
        --worker_threads=4 \
        --pp_threads=4 \
        --ssd_path=./${database} \
        --tracefile=database_320M_rnd_db.trc \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split=true \
        --xmerge=true \
        --print_tx_console=false \
        --recover=false \
        --persist=false >../results/ycsb/data/ycsb_IndeX_${underlying_storage}_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSB_skew_0_7_random_load.data

    mv log.log ../results/ycsb/data/ycsb_IndeX_${underlying_storage}_valuesize_8_5GB_memory_4_thrd_4KB_page_YCSB_skew_0_7_random_load.log
}

ycsb_xystore_random_artr() {
    rm ssd_file
    touch ssd_file
    rm -rf random
    # database="ssd_file" # ,ycsbd,ycsba,ycsbb,ycsbc,ycsbf,ycsbe,stats
    # database="database_320M_rnd_db"
    # database="rocksdb_db"
    database="random"
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=3 \
        --report_interval=1 \
        --public_list_len=15000000 \
        --num=0 \
        --batch=100 \
        --read=640000000 \
        --percentile=0.7 \
        --benchmarks=readtrace,createtracewithzipfian,randomizeworkload,load,ycsba,ycsbb,ycsbc,ycsbf,ycsbe,stats \
        --worker_threads=4 \
        --pp_threads=4 \
        --ssd_path=./${database} \
        --dbname=${database} \
        --tracefile=database_320M_rnd_db.trc \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --print_tx_console=false \
        --recover=false \
        --persist=false >../results/ycsb/data/exdindex_artr_valuesize_8_5GB_memory_4_thrd_YCSB_skew_0_7_random_load_final.data

    mv log.log ../results/ycsb/data/exdindex_artr_valuesize_8_5GB_memory_4_thrd_YCSB_skew_0_7_random_load_final.log
}

# collect_pstat_stats
# collect_iostat_stats

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
    ycsb_xystore_random_artr

elif [ "$underlying_storage" = "leanstore" ]; then 
    echo "Using Leanstore"
    sudo cmake -DCMAKE_BUILD_TYPE=release \
    -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..

    sudo make -j8
    cd ..
    cd testbench
    ycsb_xystore_random_artl
else
    echo "Invalid engine"
fi


# kill stats
# set +e
# kill $PIDPSTATSTAT1
# kill $PIDPSTATSTAT2
# set -e
# sudo kill -9 $(pidof iostat)
# sudo kill -9 $(pidof dstat)
# sleep 30
