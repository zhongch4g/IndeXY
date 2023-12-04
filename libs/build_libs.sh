# for speedb/rocksdb
cd speedb 
make static_lib -j8
cd ..

# for tbb
# cd intel/tbb


# for snappy
cd snappy
rm -rf build
mkdir build
cd build
cmake ..
sudo make install
cd ../..

# for uring
cd liburing
sudo make install
cd ..

# for zstd
cd zstd
sudo make install
cd ..

# for lz4
cd lz4
sudo make install
cd ..

# Check if the system is CentOS or Ubuntu
if [ -f /etc/redhat-release ]; then
    # CentOS/RHEL
    sudo yum install -y autoconf
elif [ -f /etc/lsb-release ]; then
    # Ubuntu
    sudo apt-get install -y autoconf
fi

cd jemalloc
./autogen.sh
make -j8
sudo make install
cd ..

cd gflags
mkdir build
cd build
cmake ..
make
sudo make install
cd ..
cd ..

cd bzip2
mkdir build
cd build
cmake ..
cmake --build . --config Release
cd ..
cd ..
