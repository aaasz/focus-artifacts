import re
import csv
import sys
import os

# Function to parse the input file
def parse_input_file(file_path):
    with open(file_path, 'r') as file:
        input_text = file.read()

    # Define regex patterns for the required information
    load_throughput_pattern = r"# Load throughput \(KTPS\)\n.*\s+(\d+\.\d+)"
    thread_count_pattern = r"# Number of client threads:\s+(\d+)"
    thread_goodput_pattern = r"# Transaction throughput \(KTPS\)\s+(\d+\.\d+)"
    abort_count_pattern = r"# Abort count:\s+(\d+)"
    abort_rate_pattern = r"Abort rate:\s+(.+)"
    commit_latency_pattern = r"# Commit Latencies \(us\)\nMin:\s+(.+)\nMax:\s+(.+)\nAvg:\s+(.+)\nP50:\s+(.+)\nP90:\s+(.+)\nP95:\s+(.+)\nP99:\s+(.+)\nP99\.9:\s+(.+)"
    abort_latency_pattern = r"# Abort Latencies \(us\)\nMin:\s+(.+)\nMax:\s+(.+)\nAvg:\s+(.+)\nP50:\s+(.+)\nP90:\s+(.+)\nP95:\s+(.+)\nP99:\s+(.+)\nP99\.9:\s+(.+)"

    # Extract the required values using regex
    load_throughput = re.search(load_throughput_pattern, input_text)
    load_throughput = load_throughput.group(1) if load_throughput else 0

    thread_count = re.search(thread_count_pattern, input_text)
    thread_count = thread_count.group(1) if thread_count else 0

    goodput = re.search(thread_goodput_pattern, input_text)
    goodput = goodput.group(1) if goodput else 0

    abort_count = re.search(abort_count_pattern, input_text)
    abort_count = abort_count.group(1) if abort_count else 0

    abort_rate = re.search(abort_rate_pattern, input_text)
    abort_rate = abort_rate.group(1) if abort_rate else 0

    commit_latency = re.search(commit_latency_pattern, input_text)
    commit_latency = commit_latency.groups() if commit_latency else [0]*8

    abort_latency = re.search(abort_latency_pattern, input_text)
    abort_latency = abort_latency.groups() if abort_latency else [0]*8

    # Return the extracted values
    return {
        "Thread Count": thread_count,
        # "Load Throughput (KTPS)": load_throughput,
        "Goodput (KTPS)": goodput,
        "Abort Count": abort_count,
        "Abort Rate": abort_rate,
        "Commit Latencies (us)": {
            "Min": commit_latency[0],
            "Max": commit_latency[1],
            "Avg": commit_latency[2],
            "P50": commit_latency[3],
            "P90": commit_latency[4],
            "P95": commit_latency[5],
            "P99": commit_latency[6],
            "P99.9": commit_latency[7]
        },
        "Abort Latencies (us)": {
            "Min": abort_latency[0],
            "Max": abort_latency[1],
            "Avg": abort_latency[2],
            "P50": abort_latency[3],
            "P90": abort_latency[4],
            "P95": abort_latency[5],
            "P99": abort_latency[6],
            "P99.9": abort_latency[7]
        }
    }

# Function to write the extracted values to a CSV file
def write_to_csv(results, output_file):
    # headers = [
    #     "Thread Count", "Load Throughput (KTPS)", "Goodput (KTPS)", "Abort Rate",
    #     "Commit Latency Min (us)", "Commit Latency Max (us)", "Commit Latency Avg (us)",
    #     "Commit Latency P50 (us)", "Commit Latency P90 (us)", "Commit Latency P95 (us)",
    #     "Commit Latency P99 (us)", "Commit Latency P99.9 (us)",
    #     "Abort Latency Min (us)", "Abort Latency Max (us)", "Abort Latency Avg (us)",
    #     "Abort Latency P50 (us)", "Abort Latency P90 (us)", "Abort Latency P95 (us)",
    #     "Abort Latency P99 (us)", "Abort Latency P99.9 (us)"
    # ]
    headers = [
        "threads", "goodput", "aborts", "abort_rate", "commit_latency_min", "commit_latency_max", "commit_latency_avg", "commit_latency_p50", "commit_latency_p90", "commit_latency_p95", "commit_latency_p99", "commit_latency_p99.9", "abort_latency_min", "abort_latency_max", "abort_latency_avg", "abort_latency_p50", "abort_latency_p90", "abort_latency_p95", "abort_latency_p99", "abort_latency_p99.9"]

    rows = []
    for result in results:
        row = [
            result["Thread Count"], result["Goodput (KTPS)"], result["Abort Count"], result["Abort Rate"]
        ]
        row.extend(result["Commit Latencies (us)"].values())
        row.extend(result["Abort Latencies (us)"].values())
        rows.append(row)

    # Write rows to CSV file
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)


def generateOutputFile(input, output=sys.stdout):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(input)
    df = df.select_dtypes(include=np.number)
    df = df.groupby(by='threads').agg('mean')
    lines = df.to_string().split('\n')
    lines[0] = df.index.name + '\t' + lines[0].lstrip()
    del lines[1]
    print('\n'.join(lines), file=output)
    
    
input_dir = sys.argv[1]
output_dir = sys.argv[2]
output_filenames = {
    '2pl-no-wait': '2PL',
    'occ-serial': 'KR-OCC',
    'sto-disk': 'STO-Disk',
    'sto-disk-cache': 'STO-Disk-Cache',
    'sto-memory': 'STO-Memory',
    'sto-counter': 'STO-Counter',
    'sto-sketch': 'STO-FPSketch',
    'sto-sketch-lazy': 'STO-FPSketch-Lazy',
    'sto-counter-lazy': 'STO-Counter-Lazy',
    'tictoc-disk': 'TicToc-Disk',
    'tictoc-memory': 'TicToc-Memory',
    'tictoc-counter': 'TicToc-Counter',
    'tictoc-sketch': 'TicToc-FPSketch',
    'tictoc-sketch-lazy': 'TicToc-FPSketch-Lazy',
    'tictoc-counter-lazy': 'TicToc-Counter-Lazy',
    'tictoc-disk-cache': 'TicToc-Disk-Cache',
    'mvcc-memory': 'MVTO-Memory',
    'mvcc-sketch': 'MVTO-FPSketch',
    'mvcc-sketch-lazy': 'MVTO-FPSketch-lazy',
    'mvcc-counter': 'MVTO-Counter',
    'mvcc-disk': 'MVTO-Disk',
    'mvcc-disk-cache': 'MVTO-Disk-Cache',
    'mvcc-counter-lazy': 'MVTO-Counter-Lazy',
}

os.makedirs(output_dir, exist_ok=True)

# Example usage
for system in output_filenames.keys():
    for workload in ['write_intensive', 'read_intensive', 'write_intensive_medium', 'read_intensive_medium',
                     'mixed', 'mixed_medium', 'long_txn',
                     'tpcc-wh4', 'tpcc-wh8', 'tpcc-wh16', 'tpcc-wh32']:
        input_file_paths = []
        for thr in [1, 2] + list(range(4, 128, 4)):
            for run in range(1, 4):
                if os.path.exists(f'{input_dir}/{system}_{workload}_{thr}_{run}.log'):
                    input_file_paths.append(f'{input_dir}/{system}_{workload}_{thr}_{run}.log')
        if not input_file_paths:
            continue

        output_file_path = f'{output_dir}/{system}-{workload}.csv'  # Replace with the path to your output CSV file

        results = [parse_input_file(file_path) for file_path in input_file_paths]
        write_to_csv(results, output_file_path)
        
        os.makedirs(f'{output_dir}/{workload}', exist_ok=True)

        with open(f'{output_dir}/{workload}/{output_filenames[system]}', 'w') as out:
            generateOutputFile(output_file_path, out)

