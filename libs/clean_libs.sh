
# for speedb/rocksdb
cd speedb 
make clean
cd ..

# for snappy
cd snappy
rm -rf build
cd ..

# for uring
cd liburing
sudo make clean
cd ..

# for zstd
cd zstd
sudo make clean
cd ..

# for lz4
cd lz4
sudo make clean
cd ..

cd bzip2
rm -rf build
cd ..