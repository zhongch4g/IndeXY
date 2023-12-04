load2_dataset() {
    rm ssd_file
    touch ssd_file
    rm trace_rnd_48bit*
    rm -rf random
    numactl -N 0 sudo ../build/frontend/xystore_ycsb_bench \
        --is_seq=false \
        --prepare_trace=false \
        --num=800000000 \
        --batch=100 \
        --benchmarks=saveload2trace \
        --tracefile=trace_rnd_48bit.trc \
        --report_interval=1 \
        --read=100 \
        --x_dram_gib=4.5 \
        --dram_gib=0.5 \
        --start_level=2 \
        --public_list_len=15000000 \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --csv_path=./log \
        --cool_pct=10 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --print_tx_console=false >../results/shared/load2_trace.data

    mv log.log ../results/shared/load2_trace.log
}

leanstore_load2_dataset() {
    rm ssd_file
    touch ssd_file
    rm trace_rnd_48bit*
    rm -rf random
    numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
        --is_seq=false \
        --prepare_trace=false \
        --num=800000000 \
        --batch=100 \
        --benchmarks=saveload2trace \
        --tracefile=trace_rnd_48bit.trc \
        --report_interval=1 \
        --read=100 \
        --dram_gib=1 \
        --ssd_path=./ssd_file \
        --worker_threads=4 \
        --pp_threads=4 \
        --csv_path=./log \
        --cool_pct=10 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --print_tx_console=false >../results/shared/load2_trace.data

    mv log.log ../results/shared/load2_trace.log
}

leanstore_load2_dataset