ROOT_DIRECTORY="../results/"
mkdir -p "$ROOT_DIRECTORY"

SHARED_DIRECTORY="../results/shared/"
mkdir -p "$SHARED_DIRECTORY"

DIRECTORY1="../results/workload-shifting"
DIRECTORY2="../results/workload-shifting/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY1="../results/vary-skewness"
DIRECTORY2="../results/vary-skewness/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY3="../results/working-set-size"
DIRECTORY4="../results/working-set-size/data"
mkdir -p "$DIRECTORY3"
mkdir -p "$DIRECTORY4"

DIRECTORY1="../results/value-size"
DIRECTORY2="../results/value-size/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY1="../results/random-write"
DIRECTORY2="../results/random-write/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY1="../results/sequential-write"
DIRECTORY2="../results/sequential-write/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY1="../results/workload-shifting"
DIRECTORY2="../results/workload-shifting/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY1="../results/tpcc"
DIRECTORY2="../results/tpcc/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"

DIRECTORY1="../results/ycsb"
DIRECTORY2="../results/ycsb/data"
mkdir -p "$DIRECTORY1"
mkdir -p "$DIRECTORY2"


# instructions
# 1. sudo apt-get install libjemalloc-dev cmake libaio-dev libgflags-dev libtbb-dev liblz4-dev
# 2. sudo apt-get install libz-dev libbz2-dev libsnappy-dev libzstd-dev liburing-dev 

# Build the local library
cd ..
cd libs
sudo bash clean_libs.sh
sudo bash build_libs.sh