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
mkdir -p build
cd build
sudo cmake -DCMAKE_BUILD_TYPE=release ..
sudo make -j20 >/dev/null 2>&1
cd ..
cd testbench

# LeanStore Evaluation
# Fig 1. Random Write & Sequential Write (four threads)
sudo bash micro_leanstore_random_write.sh

sudo bash load2_dataset.sh # prepare trace for sequential write
sudo bash micro_leanstore_sequential_write.sh #*

# Fig 2. the same total amount of data written (10GB) but with different value sizes (four threads)
# Table 1. Random write throughput of ARTL and LeanStore with different page size (4/8/16 KB)
sudo bash micro_leanstore_random_write_value_size.sh #*

# ================================= Read ================================
# sudo bash ycsb_leanstore_load_database.sh # prepare the leanstore database for read

sudo bash tpcc_leanstore.sh
sudo bash tpcc_leanstore_page_size.sh #*

# Fig 3.  Read throughput under different working set sizes (uniformly distributed access to each page (4KB page), 
# 5 GB memory constraint, 4 threads 
# Fig 4. Lookup performance of ExdIndex and LeanStore in Zipfan Distribution 
# with varying skew levels (4 threads, 5 GB dataset, 5 GB memory constraint, 0.5/0.9/0.95/0.99 skew levels)
sudo bash micro_leanstore_random_read.sh # including Fig 3.4.


# Fig 5. Lookup performance of ARTL and LeanStore in changing workloads 
# (4 threads, 5 GB dataset, 5 GB buffer pool, skew 0.7/0.8)
sudo bash micro_leanstore_adapt_workloads.sh #*

# Fig 6. YCSB workloads
sudo bash ycsb_leanstore.sh


# Fig 7. Comparative Throughput Performance of LeanStore and ExdIndex under TPC-C workload with 100
# warehouses, evaluated at two/four/eight/sixteen threads under a 30GB memory constrain
sudo bash tpcc_leanstore.sh


# Fig 8. The comparative throughput performance of LeanStore and ExdIndex under a TPC-C workload with 100
# warehouses was evaluated using four, eight, and sixteen KB page sizes for LeanStore (including ARTL), all under a
# 30GB memory constraint, 8 threads
sudo bash tpcc_leanstore_page_size.sh #*