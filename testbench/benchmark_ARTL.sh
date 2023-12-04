# instructions
# 1. sudo apt-get install libjemalloc-dev cmake libaio-dev libgflags-dev libtbb-dev liblz4-dev
# 2. sudo apt-get install libz-dev libbz2-dev libsnappy-dev libzstd-dev liburing-dev 

# Build the local library
cd ..
cd libs
sudo bash clean_libs.sh
sudo bash build_libs.sh


# Compile
cd ..
sudo rm -rf build
mkdir -p build
cd build
sudo cmake -DCMAKE_BUILD_TYPE=release -DWITH_LEANSTORE=ON -DWITH_ROCKSDB=OFF ..
sudo make -j20 >/dev/null 2>&1
cd ..
cd testbench

# LeanStore Evaluation
# Prepare the database for read experiments
sudo bash ycsb_leanstore_load_database.sh # prepare the leanstore database and dataset for read
sudo bash ycsb_rocksdb_load_database.sh # prepare the rocksdb database for read
sudo bash load2_dataset.sh

# Fig1.2. Random write/Sequential write
sudo bash micro_xystore_random_write.sh leanstore
sudo bash micro_xystore_sequential_write.sh leanstore

sudo bash micro_xystore_random_write_value_size.sh leanstore

# Fig3.4. Read skew/working set size
sudo bash micro_xystore_random_read.sh leanstore

# Fig5 changing workload
sudo bash micro_xystore_adapt_workloads.sh rocksdb #*

# Fig6. YCSB
sudo bash ycsb_xystore.sh leanstore

# Fig7. TPCC 60 million TXs different threads
sudo bash tpcc_xystore.sh leanstore

# Fig8. TPCC vary page size
sudo bash tpcc_xystore_page_size.sh leanstore