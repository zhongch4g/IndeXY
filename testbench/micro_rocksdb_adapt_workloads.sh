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

micro_rocksdb_adapt_workloads() {
database="rocksdb_320M_rnd_db"
for seg_length in 10 5 1; do
    numactl -N 0 sudo ../build/frontend/rocksdb_ycsb_bench \
    --report_interval=1 \
    --num=0 \
    --batch=100 \
    --read=640000000 \
    --segment_length=${seg_length} \
    --percentile=0.7 \
    --ssd_path=${database} \
    --benchmarks=readtrace,createworkloadshiftingtrace,readworkload4,readworkload1,readworkload2,readworkload3,readworkload4 \
    --worker_threads=4 \
    --tracefile=database_320M_rnd_db.trc \
    --print_tx_console=false \
    --is_readonly=true > ../results/workload-shifting/data/rocksdb_0.7_seg${seg_length}.data

    mv log.log ../results/workload-shifting/data/rocksdb_0.7_seg${seg_length}.log

    sync
    sysctl -q -w vm.drop_caches=3
    echo 3 >/proc/sys/vm/drop_caches
    sleep 5
done
}

micro_rocksdb_adapt_workloads