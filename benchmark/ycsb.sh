#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial sto-disk sto-memory sto-sketch sto-counter-lazy tictoc-disk tictoc-memory tictoc-sketch tictoc-counter-lazy mvcc-memory mvcc-sketch mvcc-disk mvcc-counter-lazy tictoc-disk-cache sto-disk-cache mvcc-disk-cache)
WORKLOADS=(write_intensive read_intensive write_intensive_medium read_intensive_medium mixed mixed_medium long_txn)

LOG_DIR=../ycsb_logs
OUTPUT_DIR=../ycsb_results

DEV=/dev/nvme0n1

NRUNS=3

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
        for thr in 120
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
