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

sudo rm -rf rocksdb_db
sudo ../build/frontend/rocksdb_ycsb_bench  \
--num=800000000 \
--batch=100 \
--benchmarks=readtrace,load_batch \
--tracefile=trace_rnd_48bit.trc \
--report_interval=1 \
--ssd_path=rocksdb_db \
--worker_threads=4 \
--prepare_trace=false \
--is_seq=false

mv log.log ../results/sequential-write/data/rocksdb_5GB_memory_4_thrd_load_tempfs.log