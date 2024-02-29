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

micro_rocksdb_random_read() {
database="rocksdb_320M_rnd_db"
for cnst in 0.99 0.95 0.9 0.5; do
    numactl -N 0 sudo ../build/frontend/rocksdb_ycsb_bench \
        --report_interval=1 \
        --num=0 \
        --batch=100 \
        --read=640000000 \
        --percentile=$cnst \
        --ssd_path=${database} \
        --benchmarks=readtrace,createtracewithzipfian,readcustomize,readcustomize \
        --worker_threads=4 \
        --tracefile=database_320M_rnd_db.trc \
        --print_tx_console=false \
        --is_readonly=true > ../results/vary-skewness/data/rocksdb_5GB_one_page_${cnst}.data

    mv log.log ../results/vary-skewness/data/rocksdb_5GB_one_page_${cnst}.log
done
}


micro_rocksdb_random_read_one_page() {

database="rocksdb_320M_rnd_db"
for i in 200000 400000 600000 800000 1000000 1100000 1200000 1300000 1400000 2000000 32000000 64000000 110000000 119000000 119500000 120000000 125000000 130000000 160000000 170000000 180000000 190000000 200000000; do
    numactl -N 0 sudo ../build/frontend/rocksdb_ycsb_bench \
        --report_interval=1 \
        --num=0 \
        --batch=100 \
        --read=$i \
        --ssd_path=${database} \
        --benchmarks=readtrace,createtracewithinterval,randomizeworkload,readcustomize,readcustomize,readcustomize,readcustomize,readcustomize \
        --worker_threads=4 \
        --tracefile=database_320M_rnd_db.trc \
        --print_tx_console=false \
        --is_readonly=true > ../results/working-set-size/data/rocksdb_5GB_one_page_${i}.data

    mv log.log ../results/working-set-size/data/rocksdb_5GB_one_page_${i}.log
done
}

# micro_rocksdb_random_read_one_page

# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches

micro_rocksdb_random_read