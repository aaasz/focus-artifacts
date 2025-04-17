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


        for line in lines:
            if line.startswith("# Number of client threads:"):
                fields = line.split()
                run_threads = fields[-1]

            if line.startswith("# Transaction throughput (KTPS)"):
                fields = line.split()
                run_tputs = fields[-1]

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

def is_done(file_path):
    try:
        with open(file_path, 'r') as logfile:
            lines = logfile.readlines()
            for line in lines:
                if '# Transaction throughput (KTPS)' in line:
                    return True
    except FileNotFoundError:
        return False
    return False

def run(system, workload, num_threads):
    results_root = os.path.abspath("../sketch_exp_results")
    os.makedirs(results_root, exist_ok=True)
    results_path = f"{results_root}/{system}-{workload}-{num_threads}"
    os.makedirs(results_path, exist_ok=True)
    os.chdir(splinterdb_path)
    run_cmd("git checkout src/experimental_mode.h")
    if system == "tictoc":
        src_file = "transaction_tictoc_sketch.h"
    elif system == "sto":
        src_file = "transaction_sto.h"
    elif system == "mvcc":
        src_file = "transaction_mvcc_sketch.h"
    run_cmd(f"git checkout src/transaction_impl/{src_file}")
    if system == "tictoc":
        run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_COUNTER 1/g' src/experimental_mode.h")
    elif system == "sto":
        run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_STO_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_STO_COUNTER 1/g' src/experimental_mode.h")
    elif system == "mvcc":
        run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_MVCC_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_MVCC_COUNTER 1/g' src/experimental_mode.h")

    def change_max_threads():
        run_cmd("git checkout src/platform_linux/platform.h")
        with open('src/platform_linux/platform.h', 'r') as f:
            lines = f.readlines()
        with open('src/platform_linux/platform.h', 'w') as f:
            for line in lines:
                if line.startswith('#define MAX_THREADS ('):
                    f.write(f'#define MAX_THREADS ({max(64, num_threads + 2)})\n')
                else:
                    f.write(line)
    change_max_threads()

    os.environ['CC'] = "clang"
    os.environ['LD'] = "clang"

    dev_name = "/dev/nvme0n1"
    
    cache_size = 614 if workload == "read_intensive_67M" else 6144

    rows = 2
    for size in [128, 512, 1024, 2*1024, 4*1024, 8*1024, 16*1024, 32*1024, 128*1024, 4*1024*1024, 8*1024*1024, 16*1024*1024, 32*1024*1024]:
        cols = (size // 16) // rows
        os.chdir(ycsb_path)
        output_path = os.path.join(results_path, f"rows_{rows}_cols_{cols}")
        if os.path.exists(output_path):
            continue
        os.chdir(splinterdb_path)
        run_cmd(f"sed -i 's/txn_splinterdb_cfg->sktch_config.rows = [0-9]\+;/txn_splinterdb_cfg->sktch_config.rows = {rows};/' src/transaction_impl/{src_file}")
        run_cmd(f"sed -i 's/txn_splinterdb_cfg->sktch_config.cols = [0-9]\+;/txn_splinterdb_cfg->sktch_config.cols = {cols};/' src/transaction_impl/{src_file}")
        run_cmd("sudo -E make clean")
        run_cmd("sudo -E make install")
        os.chdir(ycsb_path)
        run_cmd("make clean")
        run_cmd("make")
        while not is_done(output_path):
            run_cmd(f"sudo blkdiscard {dev_name}")
            run_cmd(f"LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./ycsbc \
                    -db transactional_splinterdb \
                    -threads {num_threads} \
                    -client txn \
                    -benchmark_seconds 240 \
                    -L workloads/{workload}.spec \
                    -W workloads/{workload}.spec \
                    -p splinterdb.filename {dev_name} \
                    -p splinterdb.cache_size_mb {cache_size} \
                    -p splinterdb.disable_upsert 1 \
                    -p splinterdb.io_contexts_per_process 64 \
                    -p splinterdb.disk_size_gb {get_device_size_bytes(dev_name) // (1024**3)} \
                    > {output_path} 2>&1")
    
    parse_result(results_path)

    output_path = os.path.join(results_path, "memory.log")
    if not os.path.exists(output_path):
        while not is_done(output_path):
            run_cmd(f"python3 ycsb.py -s {system}-memory -w {workload} -t {num_threads} -r 240 -d {dev_name} -c {cache_size} > {output_path} 2>&1")

            
run("tictoc", "write_intensive", 120)
run("tictoc", "write_intensive", 60)
run("tictoc", "read_intensive", 120)
run("tictoc", "read_intensive", 60)
run("tictoc", "read_intensive", 16)
run("tictoc", "read_intensive_67M", 120)
run("tictoc", "read_intensive_67M", 60)

run("sto", "write_intensive", 120)
run("sto", "write_intensive", 60)
run("sto", "read_intensive", 120)
run("sto", "read_intensive", 60)
run("sto", "read_intensive", 16)
run("sto", "read_intensive_67M", 120)
run("sto", "read_intensive_67M", 60)

run("mvcc", "write_intensive", 120)
run("mvcc", "write_intensive", 60)
run("mvcc", "read_intensive", 120)
run("mvcc", "read_intensive", 60)
run("mvcc", "read_intensive", 16)
run("mvcc", "read_intensive_67M", 120)
run("mvcc", "read_intensive_67M", 60)
