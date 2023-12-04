input=artlsm_artree_rocksdb_1th_24GB_100WH_cf_compaction_iostat.log
output=artlsm_artree_rocksdb_1th_24GB_100WH_cf_compaction_iostat.png

iostat-cli --data $input --disk sda --fig-output $output plot
file $output