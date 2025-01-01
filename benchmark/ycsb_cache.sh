#!/usr/bin/bash -x

SYSTEMS=(tictoc-disk tictoc-sketch)
WORKLOADS=(read_intensive read_intensive_medium)

LOG_DIR=../ycsb_cache_logs
OUTPUT_DIR=../ycsb_cache_results

DEV=/dev/nvme0n1

NRUNS=3

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
        for thr in 60
        do
            for cache in 6 7 9 13 21 37
            do
                for run in $(seq 1 ${NRUNS})
                do
                    LOG_FILE="$LOG_DIR/${sys}_${work}_${thr}_${cache}GB_${run}.log"

                    # Skip if log file already exists and contains the desired line
                    if [ -f "$LOG_FILE" ] && grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                        continue
                    fi

                    # Retry until the output file contains the desired line
                    while true
                    do
                        timeout 3600 ./ycsb.py -g -s $sys -w $work -t $thr -c $(($cache * 1024)) -r 240 -d $DEV | tee $LOG_FILE

                        # Check if the log file contains the required line
                        if grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                            break
                        fi
                    done
                done
            done
        done
    done
done

mkdir -p $OUTPUT_DIR
python3 parse_ycsb_cache.py $LOG_DIR $OUTPUT_DIR
