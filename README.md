# Testbed

CloudLab(www.cloudlab.us) Clemson r6525 machine

# Setup

```sh
bash setup.sh
```

And then logout and login again. Type `groups` in shell and see if
it shows "disk".

# YCSB

```sh
./ycsb.sh
```

Results will be in `../ycsb_results`.

# TPCC

```sh
./tpcc.sh
```

Results will be in `../tpcc_results`.

# Sketch size experiment

```sh
./run_sketch_size_exp.py
```

Results will be in `../sketch_size_exp_results`.
