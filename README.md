# IndeXY
IndeXY implementation

This guide outlines the steps to compile and execute experiments for the IndeXY implementation.

## Compiling
Install dependencies:

`sudo apt-get install gcc g++ make cmake libaio-dev libz-dev numactl dstat`

`cd testbench`

`bash build_env.sh`

## Experiment results
To run the experiments, execute the following command under testbench folder:

### ART-LSM
`sudo bash benchmark_ARTR.sh`

### ART-B+
`sudo bash benchmark_ARTL.sh`

### B+-B+
`sudo bash benchmark_LeanStore.sh`

### RocksDB
`sudo bash benchmark_rocksdb.sh`

```
Note: Please be aware that each of these scripts might take several weeks to complete depending on the speed of the SSD.
```