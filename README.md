# FOCUS Project Documentation

## Project Structure

- **splinterdb/**
  - `src/transaction_impl/`: Implementation of transaction protocols
  - `src/FPSketch/`: Implementation of FPSketch
  - All other files are related to SplinterDB

- **benchmark/**
  - Contains YCSB and TPCC benchmarks
  - All instructions below should be executed in this directory

## Testbed

[CloudLab](https://www.cloudlab.us) - Clemson r6525 machine

## Getting Started

### Environment Setup

```sh
bash setup.sh
```

**Important**: After running setup, logout and login again. Verify your setup by running `groups` in the shell - it should display "disk".

## Running Experiments

### YCSB Benchmark

```sh
./ycsb.sh
```

Results will be stored in `../ycsb_results`

### TPCC Benchmark

```sh
./tpcc.sh
```

Results will be stored in `../tpcc_results`

### Sketch Size Experiment

```sh
./run_sketch_size_exp.py
```

Results will be stored in `../sketch_size_exp_results`

### YCSB Cache Experiment

```sh
./ycsb_cache.sh
```

Results will be stored in `../ycsb_cache_results`
