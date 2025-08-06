#!/usr/bin/env python3
import os
import subprocess
import sys

# --- Configuration ---
# These variables correspond to the shell script's arrays and variables.
SYSTEMS = ["tictoc-disk", "tictoc-sketch"]
WORKLOADS = ["read_intensive", "read_intensive_medium"]
THREADS = [120]
CACHE_SIZES_GB = [6, 12, 18, 24, 30, 36]

LOG_DIR = "~/ycsb_cache_logs"
OUTPUT_DIR = "~/ycsb_cache_results"

DEV = "/dev/nvme0n1"
NRUNS = 3
TIMEOUT_SECONDS = 3600
SUCCESS_MARKER = "# Transaction throughput (KTPS)"

BACKOFF_TIME = {
	'tictoc-disk': {
        6: 8000,
        12: 5000,
        18: 4000,
        24: 3500,
        30: 3500,
        36: 3500,
        },
	'tictoc-sketch': {
		6: 3000,
		12: 1500,
		18: 1200,
		24: 1200,
		30: 1200,
		36: 1200
    }
}

# --- Helper Function ---
def check_log_for_marker(filepath, marker):
    """
    Checks if a file exists and contains the specified marker string.
    This mimics 'grep -q' on an existing file.
    """
    if not os.path.exists(filepath):
        return False
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return any(marker in line for line in f)
    except (IOError, UnicodeDecodeError):
        # Return False if file is unreadable or has encoding issues
        return False

# --- Main Script Logic ---

# 1. Create log directory if it doesn't exist (equivalent to mkdir -p)
os.makedirs(LOG_DIR, exist_ok=True)

# 2. Iterate through all combinations of configurations
for work in WORKLOADS:
    for sys_name in SYSTEMS:
        for thr in THREADS:
            for cache_gb in CACHE_SIZES_GB:
                for run in range(1, NRUNS + 1):
                    # Construct the log file path
                    log_filename = f"{sys_name}_{work}_{thr}_{cache_gb}GB_{run}.log"
                    log_filepath = os.path.join(LOG_DIR, log_filename)
                    
                    print(f"--- Preparing run: {log_filename} ---")

                    # Skip if the log file already exists and indicates a successful run
                    if check_log_for_marker(log_filepath, SUCCESS_MARKER):
                        print(f"Skipping completed run: {log_filename}\n")
                        continue

                    os.system(f"sudo blkdiscard {DEV}")

                    # Retry loop to ensure a successful run
                    while True:
                        cache_mb = cache_gb * 1024
                        command = [
                            "./ycsb.py",
                            "-s", sys_name,
                            "-w", work,
                            "-t", str(thr),
                            "-c", str(cache_mb),
                            "-r", "240",
                            "-d", DEV
                        ]
						
                        if cache_gb in BACKOFF_TIME[sys_name]:
                            command.extend(["-o", f"-w mintxnabortpaneltyus {BACKOFF_TIME[sys_name][cache_gb]}"])
                        
                        print(f"Executing: {' '.join(command)}")

                        output = ""
                        try:
                            # Execute the command, capture its output, and enforce a timeout
                            result = subprocess.run(
                                command,
                                timeout=TIMEOUT_SECONDS,
                                capture_output=True,
                                text=True,
                                check=False  # Don't raise error for non-zero exit codes
                            )
                            # Combine stdout and stderr, similar to how 'tee' would handle it
                            output = result.stdout + result.stderr

                        except subprocess.TimeoutExpired as e:
                            print(f"Command timed out after {TIMEOUT_SECONDS} seconds.", file=sys.stderr)
                            # Capture any output produced before the timeout
                            output = (e.stdout or "") + (e.stderr or "")

                        # Write the captured output to the log file, overwriting previous attempts
                        with open(log_filepath, 'w', encoding='utf-8') as f:
                            f.write(output)
                        
                        # Check if the run was successful
                        if SUCCESS_MARKER in output:
                            print(f"Success! Log saved to {log_filepath}\n")
                            break  # Exit the retry loop
                        else:
                            print(output)
                            print(f"Run failed: success marker not found in output. Retrying...\n")

# 3. Create the output directory and run the final parsing script
print("--- All runs complete. Starting post-processing. ---")
os.makedirs(OUTPUT_DIR, exist_ok=True)

parse_command = ["python3", "parse_ycsb_cache.py", LOG_DIR, OUTPUT_DIR]
print(f"Executing: {' '.join(parse_command)}")

try:
    subprocess.run(parse_command, check=True)
    print("Processing finished successfully.")
except (subprocess.CalledProcessError, FileNotFoundError) as e:
    print(f"Error running parse script: {e}", file=sys.stderr)
