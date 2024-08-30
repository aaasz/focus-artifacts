#!/usr/bin/env python3

import os
import sys
import getopt
import subprocess

def print_help():
    print("\t-h,--help: Print this help message", file=sys.stderr)
    print("\t-p,--parse [results path]: Parse the results in the given directory", file=sys.stderr)
    exit(1)

try:
    opts, args = getopt.getopt(sys.argv[1:], "hp:", ["help", "parse="])
except getopt.GetoptError as err:
    print_help()

def parse_result(results_path):
    results = []
    for file in os.listdir(results_path):
        if not file.startswith("rows_"):
            continue
        f = open(f"{results_path}/{file}", "r")
        lines = f.readlines()
        f.close()

        run_data = False

        for line in lines:
            if run_data:
                fields = line.split()
                run_threads = fields[-2]
                run_tputs = fields[-1]
                run_data = False

            if line.startswith("# Transaction throughput (KTPS)"):
                run_data = True

            if line.startswith("# Abort count:"):
                fields = line.split()
                abort_counts = fields[-1]

            if line.startswith("Abort rate:"):
                fields = line.split()
                abort_rates = fields[-1]

        filename_splits = file.split("_")
        rows = int(filename_splits[1])
        cols = int(filename_splits[3])

        size_bytes = rows * cols * (128 // 8)

        results.append((str(size_bytes), str(rows), str(cols), run_threads, run_tputs, abort_counts, abort_rates))

    results.sort(key=lambda x: (int(x[0]), int(x[1]), int(x[2])))

    output = open(f"{results_path}/results.csv", "w")
    print("size_bytes\trows\tcols\trun_threads\trun_tputs\tabort_counts\tabort_rates", file=output)
    for result in results:
        print("\t".join(result), file=output)
    output.close()

for o, a in opts:
    if o in ('-h', '--help'):
        print_help()
    if o in ('-p', '--parse'):
        parse_result(a)
        exit(0)

def run_cmd(cmd):
    subprocess.call(cmd, shell=True)

ycsb_path = os.getcwd()
splinterdb_path = os.path.abspath("../splinterdb")

def get_device_size_bytes(device: str) -> int:
    import subprocess
    output = subprocess.run(
        ["lsblk", device, "--output", "SIZE", "--bytes", "--noheadings", "--nodeps"],
        capture_output=True,
        check=True,
    )
    size = int(output.stdout.decode())
    return size


def run(system, workload, num_threads):
    results_root_dir = "../sketch_size_exp_results"
    os.makedirs(results_root_dir, exist_ok=True)
    results_path = f"{results_root_dir}/{system}-{workload}-{num_threads}"
    os.makedirs(results_path, exist_ok=True)
    os.chdir(splinterdb_path)
    run_cmd("git checkout -- .")
    if system == "tictoc":
        run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_SKETCH 1/g' src/experimental_mode.h")
    elif system == "sto":
        run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_STO_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_STO_SKETCH 1/g' src/experimental_mode.h")
    elif system == "mvcc":
        run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_MVCC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_MVCC_SKETCH 1/g' src/experimental_mode.h")

    os.environ['CC'] = "clang"
    os.environ['LD'] = "clang"

    dev_name = "/dev/nvme0n1"
    
    if system == "tictoc":
        src_file = "transaction_tictoc_sketch.h"
    elif system == "sto":
        src_file = "transaction_sto.h"
    elif system == "mvcc":
        src_file = "transaction_mvcc_sketch.h"

    max_size = 8 * 1024 * 1024
    rowsxcols = max_size // (128 // 8)
    for rows in [2]:
        cols = rowsxcols // rows
        while (rows == 1 and cols == 1) or (rows == 2 and cols >= 1):
            os.chdir(splinterdb_path)
            run_cmd(f"sed -i 's/txn_splinterdb_cfg->sktch_config.rows = [0-9]\+;/txn_splinterdb_cfg->sktch_config.rows = {rows};/' src/transaction_impl/{src_file}")
            run_cmd(f"sed -i 's/txn_splinterdb_cfg->sktch_config.cols = [0-9]\+;/txn_splinterdb_cfg->sktch_config.cols = {cols};/' src/transaction_impl/{src_file}")
            run_cmd("sudo -E make clean")
            run_cmd("sudo -E make install")
            os.chdir(ycsb_path)
            run_cmd("make clean")
            run_cmd("make")

            output_path = os.path.join(results_path, f"rows_{rows}_cols_{cols}")
            run_cmd(f"LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./ycsbc \
                    -db transactional_splinterdb \
                    -threads {num_threads} \
                    -client txn \
                    -benchmark_seconds 240 \
                    -L workloads/{workload}.spec \
                    -W workloads/{workload}.spec \
                    -p splinterdb.filename {dev_name} \
                    -p splinterdb.cache_size_mb 6144 \
                    -p splinterdb.disable_upsert 1 \
                    -p splinterdb.io_contexts_per_process 64 \
                    -p splinterdb.disk_size_gb {get_device_size_bytes(dev_name) // (1024**3)} \
                    > {output_path} 2>&1")
            
            cols = cols // 2
    
    parse_result(results_path)

            
run("tictoc", "write_intensive", 60)
run("tictoc", "read_intensive", 60)
run("tictoc", "read_intensive", 16)
run("tictoc", "read_intensive_67M", 60)

run("sto", "write_intensive", 60)
run("sto", "read_intensive", 60)
run("sto", "read_intensive", 16)
run("sto", "read_intensive_67M", 60)

run("mvcc", "write_intensive", 60)
run("mvcc", "read_intensive", 60)
run("mvcc", "read_intensive", 16)
run("mvcc", "read_intensive_67M", 60)