#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>
#include "core/timer.h"
#include "core/utils.h"
#include "db/db_factory.h"
#include "core/core_workload.h"
#include "core/tpcc_workload.h"

namespace ycsbc {

typedef enum progress_mode {
   no_progress,
   hash_progress,
   percent_progress,
} progress_mode;

typedef struct WorkloadProperties {
   std::string       filename;
   bool             preloaded;
   utils::Properties props;
} WorkloadProperties;

class YCSBInput {
public:
   ycsbc::DB                      *db;
   ycsbc::CoreWorkload            *wl;
   uint64_t                        total_num_clients;
   uint64_t                        num_ops;
   bool                            is_loading;
   progress_mode                   pmode;
   uint64_t                        total_ops;
   volatile uint64_t              *global_op_counter;
   volatile uint64_t              *last_printed;
   double                          benchmark_seconds;
   utils::Timer<double>            benchmark_timer;
   volatile std::atomic<uint64_t> *global_num_clients_done;

   YCSBInput()
      : db(nullptr),
        wl(nullptr),
        num_ops(0),
        is_loading(false),
        pmode(no_progress),
        total_ops(0),
        global_op_counter(nullptr),
        last_printed(nullptr),
        benchmark_seconds(0),
        global_num_clients_done(nullptr){};
};

class YCSBOutput {
public:
   uint64_t                   commit_cnt;
   uint64_t                   abort_cnt;
   uint64_t                   txn_cnt;
   std::vector<double>        commit_txn_latencies;
   std::vector<double>        abort_txn_latencies;
   std::vector<unsigned long> abort_cnt_per_txn;

   uint64_t                   long_txn_commit_cnt;
   uint64_t                   long_txn_abort_cnt;
   uint64_t                   long_txn_cnt;
   std::vector<double>        long_txn_commit_txn_latencies;
   std::vector<double>        long_txn_abort_txn_latencies;
   std::vector<unsigned long> long_txn_abort_cnt_per_txn;

   YCSBOutput()
      : commit_cnt(0),
        abort_cnt(0),
        txn_cnt(0),
        long_txn_commit_cnt(0),
        long_txn_abort_cnt(0),
        long_txn_cnt(0){};
};

class TPCCInput {
public:
   ycsbc::DB                      *db;
   tpcc::TPCCWorkload             *wl;
   uint64_t                        total_num_clients;
   double                          benchmark_seconds;
   utils::Timer<double>            benchmark_timer;
   volatile std::atomic<uint64_t> *global_num_clients_done;

   TPCCInput()
      : db(nullptr),
        wl(nullptr),
        total_num_clients(0),
        benchmark_seconds(0),
        global_num_clients_done(nullptr){};
};

struct TPCCOutput {
   uint64_t            txn_cnt;
   uint64_t            commit_cnt;
   uint64_t            abort_cnt;
   uint64_t            abort_cnt_payment;
   uint64_t            abort_cnt_new_order;
   uint64_t            attempts_payment;
   uint64_t            attempts_new_order;
   std::vector<double> commit_txn_latencies;
   std::vector<double> abort_txn_latencies;
};

} // namespace ycsbc 