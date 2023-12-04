ycsb_leanstore_load_databse() {
    database="database_320M_rnd_db"
    rm ${database}*
    touch ${database}
    numactl -N 0 sudo ../build/frontend/leanstore_ycsb_bench \
        --report_interval=1 \
        --num=320000000 \
        --is_seq=false \
        --batch=100 \
        --benchmarks=load,savetrace,stats \
        --worker_threads=16 \
        --pp_threads=4 \
        --ssd_path=./${database} \
        --tracefile=${database}.trc \
        --dram_gib=30 \
        --csv_path=./log \
        --cool_pct=40 \
        --free_pct=1 \
        --contention_split=false \
        --xmerge=false \
        --print_tx_console=false \
        --recover=false \
        --persist=true \
        --persist_file=${database}.json
}

ycsb_leanstore_load_databse
