#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial sto-disk sto-memory sto-sketch sto-counter-lazy tictoc-disk tictoc-memory tictoc-sketch mvcc-memory mvcc-sketch mvcc-disk mvcc-counter-lazy)
WORKLOADS=(write_intensive read_intensive write_intensive_medium read_intensive_medium mixed mixed_medium long_txn)

LOG_DIR=../ycsb_logs
OUTPUT_DIR=../ycsb_results

DEV=/dev/nvme0n1

NRUNS=3

mkdir -p $LOG_DIR

function setup_for_long_txn() {
    git checkout -- ../splinterdb
    if [ "$work" == "long_txn" ]; then
        if [ "$sys" == "tictoc-sketch" ] || [ "$sys" == "tictoc-counter-lazy" ]; then
            sed -i 's/txn_splinterdb_cfg->iceberght_config.max_num_keys = 1000;/txn_splinterdb_cfg->iceberght_config.max_num_keys = 60000;/' ../splinterdb/src/transaction_impl/transaction_tictoc_sketch.h
        elif [ "$sys" == "sto-sketch" ] || [ "$sys" == "sto-counter-lazy" ]; then
            sed -i 's/txn_splinterdb_cfg->iceberght_config.max_num_keys = 1000;/txn_splinterdb_cfg->iceberght_config.max_num_keys = 60000;/' ../splinterdb/src/transaction_impl/transaction_sto.h
        elif [ "$sys" == "mvcc-sketch" ] || [ "$sys" == "mvcc-counter-lazy" ]; then
            sed -i 's/txn_splinterdb_cfg->iceberght_config.max_num_keys = 1000;/txn_splinterdb_cfg->iceberght_config.max_num_keys = 60000;/' ../splinterdb/src/transaction_impl/transaction_mvcc_sketch.h
        fi
    fi
}


for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
	setup_for_long_txn
        # for thr in 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
        for thr in 60
        do
            for run in $(seq 1 ${NRUNS})
            do
                LOG_FILE="$LOG_DIR/${sys}_${work}_${thr}_${run}.log"

                # Skip if log file already exists and contains the desired line
                if [ -f "$LOG_FILE" ] && grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                    continue
                fi

                # Retry until the output file contains the desired line
                while true
                do
                    sudo blkdiscard $DEV
                    timeout 3600 ./ycsb.py -s $sys -w $work -t $thr -c 6144 -r 240 -d $DEV | tee $LOG_FILE

                    # Check if the log file contains the required line
                    if grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                        break
                    fi
                done
            done
        done
    done
done

mkdir -p $OUTPUT_DIR
python3 parse.py $LOG_DIR $OUTPUT_DIR
