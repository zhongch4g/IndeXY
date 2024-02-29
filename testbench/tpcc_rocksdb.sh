for i in 2 4 8 16; do
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


sudo rm -rf /dev/shm/artree_*
sudo ../build/frontend/rocksdb_tpcc \
--ssd_path=/dev/shm/ssd_file_rocksdb \
--worker_threads=${i} \
--dram_gib=30 \
--tpcc_warehouse_count=100 \
--notpcc_warehouse_affinity \
--print_tx_console \
--run_for_seconds=600 > ../results/tpcc/data/tpcc_rocksdb_30GB_${i}_thrd_tmp1.data
done