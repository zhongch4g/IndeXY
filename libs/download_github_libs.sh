# gflags
git clone -b v2.2.0 https://github.com/gflags/gflags.git

# tabluate
git clone -b v1.5 https://github.com/p-ranav/tabulate.git

# rapidjson
git clone -b v1.1.0 https://github.com/tencent/rapidjson

mkdir intel
cd intel
# tbb b066defc0229a1e92d7a200eb3fe0f7e35945d95
git clone -b tbb44u4 https://github.com/wjakob/tbb.git
cd tbb
git checkout b066defc0229a1e92d7a200eb3fe0f7e35945d95
cd ../..

# speedb
git clone -b speedb/v2.5.0 https://github.com/speedb-io/speedb.git

git clone https://github.com/libarchive/bzip2.git

git clone https://github.com/lz4/lz4.git

git clone https://github.com/axboe/liburing.git

git clone https://github.com/google/snappy.git

git clone https://github.com/facebook/zstd.git
