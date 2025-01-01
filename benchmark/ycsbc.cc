//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "core/client.h"
#include "core/txn_client.h"
#include "core/core_workload.h"
#include "core/timer.h"
#include "core/utils.h"
#include "core/numautils.h"
#include "db/db_factory.h"
#include "core/tpcc_workload.h"
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <vector>
#include <unistd.h>

#define TPCC 1

using namespace std;

typedef struct WorkloadProperties {
   string            filename;
   bool              preloaded;
   utils::Properties props;
} WorkloadProperties;

std::map<string, string> default_props = {
   {"threadcount", "1"},
   {"dbname", "basic"},
   {"progress", "none"},

   //
   // Basicdb config defaults
   //
   {"basicdb.verbose", "0"},

   //
   // splinterdb config defaults
   //
   {"splinterdb.filename", "splinterdb.db"},
   {"splinterdb.cache_size_mb", "4096"},
   // {"splinterdb.cache_size_mb", "163840"},
   {"splinterdb.disk_size_gb", "1024"},

   {"splinterdb.max_key_size", "24"},
   {"splinterdb.use_log", "1"},

   // All these options use splinterdb's internal defaults
   {"splinterdb.page_size", "0"},
   {"splinterdb.extent_size", "0"},
   {"splinterdb.io_flags", "16450"}, // O_CREAT | O_RDWR | O_DIRECT
   {"splinterdb.io_perms", "0"},
   {"splinterdb.io_async_queue_depth", "0"},
   {"splinterdb.cache_use_stats", "0"},
   {"splinterdb.cache_logfile", "0"},
   {"splinterdb.btree_rough_count_height", "0"},
   {"splinterdb.filter_remainder_size", "0"},
   {"splinterdb.filter_index_size", "0"},
   {"splinterdb.memtable_capacity", "0"},
   {"splinterdb.fanout", "0"},
   {"splinterdb.max_branches_per_node", "0"},
   {"splinterdb.use_stats", "0"},
   {"splinterdb.reclaim_threshold", "0"},
   {"splinterdb.num_memtable_bg_threads", "0"},
   {"splinterdb.num_normal_bg_threads", "0"},
   {"splinterdb.io_contexts_per_process", "0"},

   // Transaction isolation level isn't used for now
   {"splinterdb.isolation_level", "1"},

   {"rocksdb.database_filename", "rocksdb.db"},
   //    {"rocksdb.isolation_level", "3"},
};

void
UsageMessage(const char *command);
bool
StrStartWith(const char *str, const char *pre);
void
ParseCommandLine(int                         argc,
                 const char                 *argv[],
                 utils::Properties          &props,
                 WorkloadProperties         &load_workload,
                 vector<WorkloadProperties> &run_workloads);

typedef enum progress_mode {
   no_progress,
   hash_progress,
   percent_progress,
} progress_mode;

static inline void
ReportProgress(progress_mode      pmode,
               uint64_t           total_ops,
               volatile uint64_t *global_op_counter,
               uint64_t           stepsize,
               volatile uint64_t *last_printed)
{
   uint64_t old_counter = __sync_fetch_and_add(global_op_counter, stepsize);
   uint64_t new_counter = old_counter + stepsize;
   if (100 * old_counter / total_ops != 100 * new_counter / total_ops) {
      if (pmode == hash_progress) {
         cout << "#" << flush;
      } else if (pmode == percent_progress) {
         uint64_t my_percent = 100 * new_counter / total_ops;
         while (*last_printed + 1 != my_percent) {
         }
         cout << 100 * new_counter / total_ops << "%\r" << flush;
         *last_printed = my_percent;
      }
   }
}

static inline void
ProgressUpdate(progress_mode      pmode,
               uint64_t           total_ops,
               volatile uint64_t *global_op_counter,
               uint64_t           i,
               volatile uint64_t *last_printed)
{
   uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
   if ((i % sync_interval) == 0) {
      ReportProgress(
         pmode, total_ops, global_op_counter, sync_interval, last_printed);
   }
}

static inline void
ProgressFinish(progress_mode      pmode,
               uint64_t           total_ops,
               volatile uint64_t *global_op_counter,
               uint64_t           i,
               volatile uint64_t *last_printed)
{
   uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
   ReportProgress(
      pmode, total_ops, global_op_counter, i % sync_interval, last_printed);
}

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

template<class Client = ycsbc::Client>
void
DelegateClient(int id, YCSBInput *input, YCSBOutput *output)
{
   // Hoping this atomic shared variable is not bottlenecked.
   static std::atomic<bool> run_bench;
   run_bench.store(true);
   input->db->Init();
   Client client(id, *input->db, *input->wl);

   uint64_t txn_cnt = 0;

   if (input->is_loading) {
      for (uint64_t i = 0; i < input->num_ops; ++i) {
         txn_cnt += client.DoInsert();
         ProgressUpdate(input->pmode,
                        input->total_ops,
                        input->global_op_counter,
                        i,
                        input->last_printed);
      }
   } else {
      if (input->benchmark_seconds > 0) {
         // timer-based running -- all clients make sure running at
         // least benchmark_seconds
         input->benchmark_timer.Start();
         while (input->global_num_clients_done->load()
                < input->total_num_clients) {
            txn_cnt += client.DoTransaction();
            ProgressUpdate(input->pmode,
                           input->total_ops,
                           input->global_op_counter,
                           txn_cnt,
                           input->last_printed);
            double duration = input->benchmark_timer.End();
            if (duration > input->benchmark_seconds) {
               input->global_num_clients_done->fetch_add(1);
            }
         }
         ProgressFinish(input->pmode,
                        input->total_ops,
                        input->global_op_counter,
                        txn_cnt,
                        input->last_printed);

      } else if (input->wl->max_txn_count() > 0) {
         while (txn_cnt < input->wl->max_txn_count() && run_bench.load()) {
         // while (txn_cnt < input->wl->max_txn_count()) {
            txn_cnt += client.DoTransaction();
            ProgressUpdate(input->pmode,
                           input->total_ops,
                           input->global_op_counter,
                           txn_cnt,
                           input->last_printed);
         }
         ProgressFinish(input->pmode,
                        input->total_ops,
                        input->global_op_counter,
                        txn_cnt,
                        input->last_printed);
         run_bench.store(false);
      } else {
         for (uint64_t i = 0; i < input->num_ops; ++i) {
            txn_cnt += client.DoTransaction();
            ProgressUpdate(input->pmode,
                           input->total_ops,
                           input->global_op_counter,
                           i,
                           input->last_printed);
         }
         ProgressFinish(input->pmode,
                        input->total_ops,
                        input->global_op_counter,
                        input->num_ops,
                        input->last_printed);
      }
   }
   input->db->Close();

   if (output) {
      output->txn_cnt              = txn_cnt;
      output->commit_cnt           = client.GetTxnCnt();
      output->abort_cnt            = client.GetAbortCnt();
      output->commit_txn_latencies = client.GetCommitTxnLatnecies();
      output->abort_txn_latencies  = client.GetAbortTxnLatnecies();
      output->abort_cnt_per_txn    = client.GetAbortCntPerTxn();
      output->long_txn_cnt =
         client.GetLongTxnCnt() + client.GetLongTxnAbortCnt();
      output->long_txn_commit_cnt = client.GetLongTxnCnt();
      output->long_txn_abort_cnt  = client.GetLongTxnAbortCnt();
      output->long_txn_commit_txn_latencies =
         client.GetLongTxnCommitTxnLatnecies();
      output->long_txn_abort_txn_latencies =
         client.GetLongTxnAbortTxnLatnecies();
      output->long_txn_abort_cnt_per_txn = client.GetLongTxnAbortCntPerTxn();
   }
}

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

int
DelegateTPCCClient(uint32_t thread_id, TPCCInput *input, TPCCOutput *stats)
{
   input->db->Init();

   tpcc::TPCCClient client(thread_id, input->wl);

   uint64_t txn_cnt = 0;

   if (input->benchmark_seconds > 0) {
      // timer-based running -- all clients make sure running at
      // least benchmark_seconds
      input->benchmark_timer.Start();
      while (input->global_num_clients_done->load() < input->total_num_clients)
      {
         txn_cnt += client.run_transaction();
         double duration = input->benchmark_timer.End();
         if (duration > input->benchmark_seconds) {
            input->global_num_clients_done->fetch_add(1);
         }
      }
   } else {
      txn_cnt = tpcc::g_total_num_transactions;
      client.run_transactions();
   }

   if (stats) {
      stats->txn_cnt              = txn_cnt;
      stats->commit_cnt           = client.GetCommitCnt();
      stats->abort_cnt            = client.GetAbortCnt();
      stats->abort_cnt_payment    = client.GetAbortCntPayment();
      stats->abort_cnt_new_order  = client.GetAbortCntNewOrder();
      stats->attempts_payment     = client.GetAttemptsPayment();
      stats->attempts_new_order   = client.GetAttemptsNewOrder();
      stats->commit_txn_latencies = client.GetCommitTxnLatnecies();
      stats->abort_txn_latencies  = client.GetAbortTxnLatnecies();
   }

   input->db->Close();


   return 0;
}

template<typename T>
void
PrintDistribution(std::vector<T> &data, std::ostream &out = std::cout)
{
   if (data.empty()) {
      return;
   }

   out << "Min: " << data.front() << std::endl;
   out << "Max: " << data.back() << std::endl;
   out << "Avg: "
       << std::accumulate(data.begin(), data.end(), 0.0) / data.size()
       << std::endl;
   out << "P50: " << data[data.size() / 2] << std::endl;
   out << "P90: " << data[data.size() * 9 / 10] << std::endl;
   out << "P95: " << data[data.size() * 95 / 100] << std::endl;
   out << "P99: " << data[data.size() * 99 / 100] << std::endl;
   out << "P99.9: " << data[data.size() * 999 / 1000] << std::endl;
};

struct IOStats {
   long long read_bytes  = 0;
   long long write_bytes = 0;
};

IOStats
getIOStats()
{
   IOStats ioStats;

   // Get the current process ID (PID)
   pid_t       pid        = getpid();
   std::string ioFilePath = "/proc/" + std::to_string(pid) + "/io";

   // Open the /proc/<pid>/io file
   std::ifstream ioFile(ioFilePath);
   if (!ioFile.is_open()) {
      std::cerr << "Failed to open " << ioFilePath << std::endl;
      return ioStats;
   }

   // Parse the file to extract read_bytes and write_bytes
   std::string line;
   while (std::getline(ioFile, line)) {
      if (line.find("read_bytes:") == 0) {
         ioStats.read_bytes = std::stoll(line.substr(line.find(":") + 1));
      } else if (line.find("write_bytes:") == 0) {
         ioStats.write_bytes = std::stoll(line.substr(line.find(":") + 1));
      }
   }

   ioFile.close();
   return ioStats;
}


int
main(const int argc, const char *argv[])
{
   utils::Properties          props;
   WorkloadProperties         load_workload;
   vector<WorkloadProperties> run_workloads;
   ParseCommandLine(argc, argv, props, load_workload, run_workloads);

   const unsigned int num_threads = stoi(props.GetProperty("threadcount", "1"));
   progress_mode      pmode       = no_progress;
   if (props.GetProperty("progress", "none") == "hash") {
      pmode = hash_progress;
   } else if (props.GetProperty("progress", "none") == "percent") {
      pmode = percent_progress;
   }
   vector<future<int>>  actual_ops;
   uint64_t             record_count;
   uint64_t             total_ops;
   uint64_t             total_txn_count;
   utils::Timer<double> timer;

   // 60 is the max number of threads that splinterdb supports.
   std::vector<size_t> cores = numautils::get_cores(60);

   ycsbc::DB *db;
   if (props.GetProperty("benchmark") == "tpcc") {
      db = new ycsbc::TransactionalSplinterDB(props,
                                              load_workload.preloaded,
                                              tpcc::tpcc_merge_tuple,
                                              tpcc::tpcc_merge_tuple_final);
      for (WorkloadProperties workload : run_workloads) {
         tpcc::TPCCWorkload tpcc_wl;
         tpcc_wl.init(workload.props,
                      (ycsbc::TransactionalSplinterDB *)db,
                      num_threads); // loads TPCC tables into DB

         std::vector<TPCCInput>   _tpcc_inputs(num_threads);
         std::vector<TPCCOutput>  _tpcc_output(num_threads);
         std::vector<std::thread> tpcc_threads;
         std::atomic<uint64_t>    num_clients_done(0);

         timer.Start();
         {
            for (unsigned int i = 0; i < num_threads; ++i) {
               _tpcc_inputs[i].db = db;
               _tpcc_inputs[i].wl = &tpcc_wl;

               _tpcc_inputs[i].benchmark_seconds =
                  stof(props.GetProperty("benchmark_seconds", "0"));
               _tpcc_inputs[i].global_num_clients_done = &num_clients_done;
               _tpcc_inputs[i].total_num_clients       = num_threads;
               tpcc_threads.emplace_back(std::thread(
                  DelegateTPCCClient, i, &_tpcc_inputs[i], &_tpcc_output[i]));
               numautils::bind_to_core(tpcc_threads[i], cores[i]);
            }
            for (auto &t : tpcc_threads) {
               t.join();
            }
         }
         double run_duration = timer.End();
         if (pmode != no_progress) {
            cout << "\n";
         }
         tpcc_wl.deinit();

         uint64_t total_committed_cnt         = 0;
         uint64_t total_aborted_cnt           = 0;
         uint64_t total_aborted_cnt_payment   = 0;
         uint64_t total_aborted_cnt_new_order = 0;
         uint64_t total_attempts_payment      = 0;
         uint64_t total_attempts_new_order    = 0;

         for (unsigned int i = 0; i < num_threads; ++i) {
            cout << "[Client " << i
                 << "] commit_cnt: " << _tpcc_output[i].commit_cnt
                 << ", abort_cnt: " << _tpcc_output[i].abort_cnt << endl;
            total_committed_cnt += _tpcc_output[i].commit_cnt;
            total_aborted_cnt += _tpcc_output[i].abort_cnt;
            total_aborted_cnt_payment += _tpcc_output[i].abort_cnt_payment;
            total_aborted_cnt_new_order += _tpcc_output[i].abort_cnt_new_order;
            total_attempts_payment += _tpcc_output[i].attempts_payment;
            total_attempts_new_order += _tpcc_output[i].attempts_new_order;
         }

         cout << "# Transaction throughput (KTPS)" << endl;
         cout << props["dbname"] << " TransactionalSplinterDB" << '\t'
              << num_threads << '\t';
         cout << total_committed_cnt / run_duration / 1000 << endl;
         cout << "Run duration (sec):\t" << run_duration << endl;

         cout << "# Abort count:\t" << total_aborted_cnt << '\n';
         cout << "Abort rate:\t"
              << (double)total_aborted_cnt
                    / (total_aborted_cnt + total_committed_cnt)
              << "\n";

         if (total_aborted_cnt > 0) {
            cout << "# (Payment) Abort rate(%):\t"
                 << total_aborted_cnt_payment * 100.0 / total_aborted_cnt
                 << '\n';
            cout << "# (NewOrder) Abort rate(%):\t"
                 << total_aborted_cnt_new_order * 100.0 / total_aborted_cnt
                 << '\n';
         } else {
            cout << "# (Payment) Abort rate(%):\t0\n";
            cout << "# (NewOrder) Abort rate(%):\t0\n";
         }

         cout << "# (Payment) Failure rate(%):\t"
              << total_aborted_cnt_payment * 100.0 / total_attempts_payment
              << '\n';
         cout << "# (NewOrder) Failure rate(%):\t"
              << total_aborted_cnt_new_order * 100.0 / total_attempts_new_order
              << '\n';

         cout << "# (Payment) Total attempts:\t" << total_attempts_payment
              << '\n';
         cout << "# (NewOrder) Total attempts:\t" << total_attempts_new_order
              << '\n';

         /*
          * Print Latencies
          */
         std::vector<double> total_commit_txn_latencies;
         std::vector<double> total_abort_txn_latencies;
         for (unsigned int i = 0; i < num_threads; ++i) {
            total_commit_txn_latencies.insert(
               total_commit_txn_latencies.end(),
               _tpcc_output[i].commit_txn_latencies.begin(),
               _tpcc_output[i].commit_txn_latencies.end());
            total_abort_txn_latencies.insert(
               total_abort_txn_latencies.end(),
               _tpcc_output[i].abort_txn_latencies.begin(),
               _tpcc_output[i].abort_txn_latencies.end());
         }

         std::sort(total_commit_txn_latencies.begin(),
                   total_commit_txn_latencies.end());
         std::sort(total_abort_txn_latencies.begin(),
                   total_abort_txn_latencies.end());

         std::cout << "# Commit Latencies (us)" << std::endl;
         PrintDistribution<double>(total_commit_txn_latencies);
         std::cout << "# Abort Latencies (us)" << std::endl;
         PrintDistribution<double>(total_abort_txn_latencies);

         db->PrintDBStats();
      }
   } else {
      db = ycsbc::DBFactory::CreateDB(props, load_workload.preloaded);
      if (!db) {
         cout << "Unknown database name " << props["dbname"] << endl;
         exit(0);
      }

      const uint64_t num_threads_load = 60;
      record_count =
         stol(load_workload.props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
      uint64_t batch_size = sqrt(record_count);
      if (record_count / batch_size < num_threads_load)
         batch_size = record_count / num_threads_load;
      if (batch_size < 1)
         batch_size = 1;

      ycsbc::BatchedCounterGenerator key_generator(
         load_workload.preloaded ? record_count : 0, batch_size);
      ycsbc::CoreWorkload wls[num_threads_load];

      unsigned int thr_i;
      for (thr_i = 0; thr_i < num_threads_load; ++thr_i) {
         wls[thr_i].InitLoadWorkload(
            load_workload.props, num_threads_load, thr_i, &key_generator);
      }

      // Perform the Load phase
      if (!load_workload.preloaded) {
         std::vector<std::thread> load_threads;
         YCSBInput                ycsb_inputs[num_threads_load];
         YCSBOutput               ycsb_outputs[num_threads_load];

         IOStats io_stats_load_begin = getIOStats();

         timer.Start();
         {
            cout << "# Loading records:\t" << record_count << endl;
            uint64_t load_progress = 0;
            uint64_t last_printed  = 0;
            for (thr_i = 0; thr_i < num_threads_load; ++thr_i) {
               uint64_t start_op = (record_count * thr_i) / num_threads_load;
               uint64_t end_op =
                  (record_count * (thr_i + 1)) / num_threads_load;
               ycsb_inputs[thr_i].db                = db;
               ycsb_inputs[thr_i].wl                = &wls[thr_i];
               ycsb_inputs[thr_i].num_ops           = end_op - start_op;
               ycsb_inputs[thr_i].is_loading        = true;
               ycsb_inputs[thr_i].pmode             = pmode;
               ycsb_inputs[thr_i].total_ops         = record_count;
               ycsb_inputs[thr_i].global_op_counter = &load_progress;
               ycsb_inputs[thr_i].last_printed      = &last_printed;

               load_threads.emplace_back(
                  std::thread(props.GetProperty("client") == "txn"
                                 ? DelegateClient<ycsbc::TxnClient>
                                 : DelegateClient<ycsbc::Client>,
                              thr_i,
                              &ycsb_inputs[thr_i],
                              &ycsb_outputs[thr_i]));
               numautils::bind_to_core(load_threads[thr_i], cores[thr_i]);
            }
            for (auto &t : load_threads) {
               t.join();
            }
         }
         double  load_duration     = timer.End();
         IOStats io_stats_load_end = getIOStats();

         if (pmode != no_progress) {
            cout << "\n";
         }
         total_txn_count = 0;
         for (thr_i = 0; thr_i < num_threads_load; ++thr_i) {
            total_txn_count += ycsb_outputs[thr_i].txn_cnt;
         }
         cout << "# Load throughput (KTPS)" << endl;
         cout << props["dbname"] << '\t' << load_workload.filename << '\t'
              << num_threads_load << '\t';
         cout << total_txn_count / load_duration / 1000 << endl;
         cout << "Load duration (sec):\t" << load_duration << endl;

         db->PrintDBStats();

         long long read_bytes =
            io_stats_load_end.read_bytes - io_stats_load_begin.read_bytes;
         long long write_bytes =
            io_stats_load_end.write_bytes - io_stats_load_begin.write_bytes;
         cout << "[Load phase IO stats]\t read_bytes: " << read_bytes
              << ", write_bytes: " << write_bytes << " (Throughput (B/s): "
              << (read_bytes + write_bytes) / load_duration << ")" << endl;
      }

      uint64_t ops_per_transactions = 1;
      // Perform any Run phases
      for (const auto &workload : run_workloads) {
         std::vector<std::thread> run_threads;
         for (thr_i = 0; thr_i < num_threads; ++thr_i) {
            run_threads.emplace_back(std::thread(
               [&wls = wls, &workload = workload, num_threads, thr_i]() {
                  wls[thr_i].InitRunWorkload(
                     workload.props, num_threads, thr_i);
               }));
            numautils::bind_to_core(run_threads[thr_i], cores[thr_i]);
         }
         for (auto &t : run_threads) {
            t.join();
         }
         // actual_ops.clear();
         run_threads.clear();

         ops_per_transactions   = stoi(workload.props.GetProperty(
            ycsbc::CoreWorkload::OPS_PER_TRANSACTION_PROPERTY,
            ycsbc::CoreWorkload::OPS_PER_TRANSACTION_DEFAULT));
         uint64_t max_txn_count = stoi(workload.props.GetProperty(
            ycsbc::CoreWorkload::MAX_TXN_COUNT_PROPERTY, "0"));
         total_ops =
            max_txn_count > 0
               ? max_txn_count * num_threads * ops_per_transactions
               : stoi(workload
                         .props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
         uint64_t              run_progress = 0;
         uint64_t              last_printed = 0;
         YCSBInput             ycsb_inputs[num_threads];
         YCSBOutput            ycsb_outputs[num_threads];
         std::atomic<uint64_t> num_clients_done(0);

         IOStats io_stats_run_begin = getIOStats();
         timer.Start();
         {
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               uint64_t start_op = (total_ops * thr_i) / num_threads;
               uint64_t end_op   = (total_ops * (thr_i + 1)) / num_threads;
               uint64_t num_transactions =
                  (end_op - start_op) / ops_per_transactions;
               ycsb_inputs[thr_i].db                = db;
               ycsb_inputs[thr_i].wl                = &wls[thr_i];
               ycsb_inputs[thr_i].num_ops           = num_transactions;
               ycsb_inputs[thr_i].is_loading        = false;
               ycsb_inputs[thr_i].pmode             = pmode;
               ycsb_inputs[thr_i].total_ops         = total_ops;
               ycsb_inputs[thr_i].global_op_counter = &run_progress;
               ycsb_inputs[thr_i].last_printed      = &last_printed;
               ycsb_inputs[thr_i].benchmark_seconds =
                  stof(props.GetProperty("benchmark_seconds", "0"));
               ycsb_inputs[thr_i].global_num_clients_done = &num_clients_done;
               ycsb_inputs[thr_i].total_num_clients       = num_threads;

               run_threads.emplace_back(
                  std::thread(props.GetProperty("client") == "txn"
                                 ? DelegateClient<ycsbc::TxnClient>
                                 : DelegateClient<ycsbc::Client>,
                              thr_i,
                              &ycsb_inputs[thr_i],
                              &ycsb_outputs[thr_i]));

               numautils::bind_to_core(run_threads[thr_i], cores[thr_i]);
            }
            for (auto &t : run_threads) {
               t.join();
            }
         }
         double  run_duration     = timer.End();
         IOStats io_stats_run_end = getIOStats();

         if (pmode != no_progress) {
            cout << "\n";
         }

         for (thr_i = 0; thr_i < num_threads; ++thr_i) {
            wls[thr_i].DeinitRunWorkload();
         }

         total_txn_count           = 0;
         uint64_t total_commit_cnt = 0;
         uint64_t total_abort_cnt  = 0;
         /*
          * Print stats by transactions if long txn is enabled.
          */
         bool is_long_txn_enabled =
            stof(workload.props.GetProperty(
               ycsbc::CoreWorkload::LONG_TXN_RATIO,
               ycsbc::CoreWorkload::LONG_TXN_RATIO_DEFAULT))
            > 0.0;
         if (is_long_txn_enabled) {
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               cout << "[Client " << thr_i << "] commit_cnt: "
                    << ycsb_outputs[thr_i].commit_cnt
                          + ycsb_outputs[thr_i].long_txn_commit_cnt
                    << ", abort_cnt: "
                    << ycsb_outputs[thr_i].abort_cnt
                          + ycsb_outputs[thr_i].long_txn_abort_cnt
                    << endl;
               total_txn_count += ycsb_outputs[thr_i].txn_cnt;
               total_commit_cnt += ycsb_outputs[thr_i].commit_cnt
                                   + ycsb_outputs[thr_i].long_txn_commit_cnt;
               total_abort_cnt += ycsb_outputs[thr_i].abort_cnt
                                  + ycsb_outputs[thr_i].long_txn_abort_cnt;
            }
            cout << "# Transaction count:\t" << total_txn_count << endl;
            cout << "# Committed Transaction count:\t" << total_commit_cnt
                 << endl;
            cout << "# Aborted Transaction count:\t"
                 << total_txn_count - total_commit_cnt << endl;
            cout << "# Transaction throughput (KTPS)" << endl;
            cout << props["dbname"] << '\t' << workload.filename << '\t'
                 << num_threads << '\t';
            cout << total_commit_cnt / run_duration / 1000 << endl;
            cout << "Run duration (sec):\t" << run_duration << endl;
            cout << "# Abort count:\t" << total_abort_cnt << '\n';
            cout << "Abort rate:\t"
                 << (double)total_abort_cnt
                       / (total_abort_cnt + total_commit_cnt)
                 << "\n";
            /*
             * Print Latencies
             */
            std::vector<double> total_commit_txn_latencies;
            std::vector<double> total_abort_txn_latencies;
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               total_commit_txn_latencies.insert(
                  total_commit_txn_latencies.end(),
                  ycsb_outputs[thr_i].commit_txn_latencies.begin(),
                  ycsb_outputs[thr_i].commit_txn_latencies.end());
               total_commit_txn_latencies.insert(
                  total_commit_txn_latencies.end(),
                  ycsb_outputs[thr_i].long_txn_commit_txn_latencies.begin(),
                  ycsb_outputs[thr_i].long_txn_commit_txn_latencies.end());
               total_abort_txn_latencies.insert(
                  total_abort_txn_latencies.end(),
                  ycsb_outputs[thr_i].abort_txn_latencies.begin(),
                  ycsb_outputs[thr_i].abort_txn_latencies.end());
               total_abort_txn_latencies.insert(
                  total_abort_txn_latencies.end(),
                  ycsb_outputs[thr_i].long_txn_abort_txn_latencies.begin(),
                  ycsb_outputs[thr_i].long_txn_abort_txn_latencies.end());
            }

            std::sort(total_commit_txn_latencies.begin(),
                      total_commit_txn_latencies.end());
            std::sort(total_abort_txn_latencies.begin(),
                      total_abort_txn_latencies.end());

            std::cout << "# Commit Latencies (us)" << std::endl;
            PrintDistribution<double>(total_commit_txn_latencies);
            std::cout << "# Abort Latencies (us)" << std::endl;
            PrintDistribution<double>(total_abort_txn_latencies);

            /*
             * Print abort count per transaction
             */
            std::vector<unsigned long> total_abort_cnt_per_txn;
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               total_abort_cnt_per_txn.insert(
                  total_abort_cnt_per_txn.end(),
                  ycsb_outputs[thr_i].abort_cnt_per_txn.begin(),
                  ycsb_outputs[thr_i].abort_cnt_per_txn.end());

               total_abort_cnt_per_txn.insert(
                  total_abort_cnt_per_txn.end(),
                  ycsb_outputs[thr_i].long_txn_abort_cnt_per_txn.begin(),
                  ycsb_outputs[thr_i].long_txn_abort_cnt_per_txn.end());
            }
            std::sort(total_abort_cnt_per_txn.begin(),
                      total_abort_cnt_per_txn.end());
            std::cout << "# Abort count per transaction" << std::endl;
            PrintDistribution<unsigned long>(total_abort_cnt_per_txn);

            std::cout << "# Long Transaction Stats" << std::endl;
            uint64_t total_long_txn_cnt    = 0;
            uint64_t total_long_commit_cnt = 0;
            uint64_t total_long_abort_cnt  = 0;

            std::vector<double> total_long_commit_txn_latencies;
            std::vector<double> total_long_abort_txn_latencies;
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               // cout << "[Client " << thr_i
               //      << "] long_txn_commit_cnt: " <<
               //      ycsb_outputs[thr_i].long_txn_commit_cnt
               //      << ", long_txn_abort_cnt: " <<
               //      ycsb_outputs[thr_i].long_txn_abort_cnt << endl;
               total_long_txn_cnt += ycsb_outputs[thr_i].long_txn_cnt;
               total_long_commit_cnt += ycsb_outputs[thr_i].long_txn_commit_cnt;
               total_long_abort_cnt += ycsb_outputs[thr_i].long_txn_abort_cnt;

               total_long_commit_txn_latencies.insert(
                  total_long_commit_txn_latencies.end(),
                  ycsb_outputs[thr_i].long_txn_commit_txn_latencies.begin(),
                  ycsb_outputs[thr_i].long_txn_commit_txn_latencies.end());
               total_long_abort_txn_latencies.insert(
                  total_long_abort_txn_latencies.end(),
                  ycsb_outputs[thr_i].long_txn_abort_txn_latencies.begin(),
                  ycsb_outputs[thr_i].long_txn_abort_txn_latencies.end());
            }
            std::cout << "# Long Transaction count:\t" << total_long_txn_cnt
                      << std::endl;
            std::cout << "# Committed Long Transaction count:\t"
                      << total_long_commit_cnt << std::endl;
            std::cout << "# Aborted Long Transaction count:\t"
                      << total_long_txn_cnt - total_long_commit_cnt
                      << std::endl;
            std::cout << "# Long Transaction abort rate:\t\t"
                      << (double)total_long_abort_cnt
                            / (total_long_abort_cnt + total_long_commit_cnt)
                      << std::endl;
            std::cout << "# Long Transaction abort portion (%):\t"
                      << (double)total_long_abort_cnt / total_abort_cnt * 100
                      << std::endl;

            std::sort(total_long_commit_txn_latencies.begin(),
                      total_long_commit_txn_latencies.end());
            std::sort(total_long_abort_txn_latencies.begin(),
                      total_long_abort_txn_latencies.end());

            std::cout << "# Long Transaction Commit Latencies (us)"
                      << std::endl;
            PrintDistribution<double>(total_long_commit_txn_latencies);
            std::cout << "# Long Transaction Abort Latencies (us)" << std::endl;
            PrintDistribution<double>(total_long_abort_txn_latencies);
         } else {
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               cout << "[Client " << thr_i
                    << "] commit_cnt: " << ycsb_outputs[thr_i].commit_cnt
                    << ", abort_cnt: " << ycsb_outputs[thr_i].abort_cnt << endl;
               total_txn_count += ycsb_outputs[thr_i].txn_cnt;
               total_commit_cnt += ycsb_outputs[thr_i].commit_cnt;
               total_abort_cnt += ycsb_outputs[thr_i].abort_cnt;
            }
            cout << "# Transaction count:\t" << total_txn_count << endl;
            cout << "# Committed Transaction count:\t" << total_commit_cnt
                 << endl;
            cout << "# Aborted Transaction count:\t"
                 << total_txn_count - total_commit_cnt << endl;
            cout << "# Transaction throughput (KTPS)" << endl;
            cout << props["dbname"] << '\t' << workload.filename << '\t'
                 << num_threads << '\t';
            cout << total_commit_cnt / run_duration / 1000 << endl;
            cout << "Run duration (sec):\t" << run_duration << endl;
            cout << "# Abort count:\t" << total_abort_cnt << '\n';
            cout << "Abort rate:\t"
                 << (double)total_abort_cnt
                       / (total_abort_cnt + total_commit_cnt)
                 << "\n";
            /*
             * Print Latencies
             */
            std::vector<double> total_commit_txn_latencies;
            std::vector<double> total_abort_txn_latencies;
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               total_commit_txn_latencies.insert(
                  total_commit_txn_latencies.end(),
                  ycsb_outputs[thr_i].commit_txn_latencies.begin(),
                  ycsb_outputs[thr_i].commit_txn_latencies.end());
               total_abort_txn_latencies.insert(
                  total_abort_txn_latencies.end(),
                  ycsb_outputs[thr_i].abort_txn_latencies.begin(),
                  ycsb_outputs[thr_i].abort_txn_latencies.end());
            }

            std::sort(total_commit_txn_latencies.begin(),
                      total_commit_txn_latencies.end());
            std::sort(total_abort_txn_latencies.begin(),
                      total_abort_txn_latencies.end());

            std::cout << "# Commit Latencies (us)" << std::endl;
            PrintDistribution<double>(total_commit_txn_latencies);
            std::cout << "# Abort Latencies (us)" << std::endl;
            PrintDistribution<double>(total_abort_txn_latencies);

            /*
             * Print abort count per transaction
             */
            std::vector<unsigned long> total_abort_cnt_per_txn;
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               total_abort_cnt_per_txn.insert(
                  total_abort_cnt_per_txn.end(),
                  ycsb_outputs[thr_i].abort_cnt_per_txn.begin(),
                  ycsb_outputs[thr_i].abort_cnt_per_txn.end());
            }
            std::sort(total_abort_cnt_per_txn.begin(),
                      total_abort_cnt_per_txn.end());
            std::cout << "# Abort count per transaction" << std::endl;
            PrintDistribution<unsigned long>(total_abort_cnt_per_txn);
         }
         db->PrintDBStats();

         long long read_bytes =
            io_stats_run_end.read_bytes - io_stats_run_begin.read_bytes;
         long long write_bytes =
            io_stats_run_end.write_bytes - io_stats_run_begin.write_bytes;
         cout << "[Run phase IO stats]\t read_bytes: " << read_bytes
              << ", write_bytes: " << write_bytes << " (Throughput (B/s): "
              << (read_bytes + write_bytes) / run_duration << ")" << endl;
      }
   }

   delete db;
}

void
ParseCommandLine(int                         argc,
                 const char                 *argv[],
                 utils::Properties          &props,
                 WorkloadProperties         &load_workload,
                 vector<WorkloadProperties> &run_workloads)
{
   bool                saw_load_workload = false;
   WorkloadProperties *last_workload     = NULL;
   int                 argindex          = 1;

   load_workload.filename  = "";
   load_workload.preloaded = false;

   props.SetProperty("benchmark", "ycsb");

   for (auto const &[key, val] : default_props) {
      props.SetProperty(key, val);
   }

   while (argindex < argc && StrStartWith(argv[argindex], "-")) {
      if (strcmp(argv[argindex], "-threads") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("threadcount", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-db") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("dbname", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-progress") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("progress", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-host") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("host", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-port") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("port", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-slaves") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("slaves", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-benchmark") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("benchmark", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-client") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("client", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-benchmark_seconds") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("benchmark_seconds", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-W") == 0
                 || strcmp(argv[argindex], "-P") == 0
                 || strcmp(argv[argindex], "-L") == 0)
      {
         WorkloadProperties workload;
         workload.preloaded = strcmp(argv[argindex], "-P") == 0;
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         workload.filename.assign(argv[argindex]);
         ifstream input(argv[argindex]);
         try {
            workload.props.Load(input);
         } catch (const string &message) {
            cout << message << endl;
            exit(0);
         }
         input.close();
         argindex++;
         if (strcmp(argv[argindex - 2], "-W") == 0) {
            run_workloads.push_back(workload);
            last_workload = &run_workloads[run_workloads.size() - 1];
         } else if (saw_load_workload) {
            UsageMessage(argv[0]);
            exit(0);
         } else {
            saw_load_workload = true;
            load_workload     = workload;
            last_workload     = &load_workload;
         }
      } else if (strcmp(argv[argindex], "-p") == 0
                 || strcmp(argv[argindex], "-w") == 0)
      {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         std::string propkey = argv[argindex];
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         std::string propval = argv[argindex];
         if (strcmp(argv[argindex - 2], "-w") == 0) {
            if (last_workload) {
               last_workload->props.SetProperty(propkey, propval);
            } else {
               UsageMessage(argv[0]);
               exit(0);
            }
         } else {
            props.SetProperty(propkey, propval);
         }
         argindex++;
      } else {
         cout << "Unknown option '" << argv[argindex] << "'" << endl;
         exit(0);
      }
   }

   if (argindex == 1 || argindex != argc
       || (props.GetProperty("benchmark") == "ycsb" && !saw_load_workload))
   {
      UsageMessage(argv[0]);
      exit(0);
   }
}

void
UsageMessage(const char *command)
{
   cout << "Usage: " << command << " [options]"
        << "-L <load-workload.spec> [-W run-workload.spec] ..." << endl;
   cout << "       Perform the given Load workload, then each Run workload"
        << endl;
   cout << "Usage: " << command << " [options]"
        << "-P <load-workload.spec> [-W run-workload.spec] ... " << endl;
   cout << "       Perform each given Run workload on a database that has been "
           "preloaded with the given Load workload"
        << endl;
   cout << "Options:" << endl;
   cout << "  -threads <n>: execute using <n> threads (default: "
        << default_props["threadcount"] << ")" << endl;
   cout << "  -db <dbname>: specify the name of the DB to use (default: "
        << default_props["dbname"] << ")" << endl;
   cout
      << "  -L <file>: Initialize the database with the specified Load workload"
      << endl;
   cout << "  -P <file>: Indicates that the database has been preloaded with "
           "the specified Load workload"
        << endl;
   cout << "  -W <file>: Perform the Run workload specified in <file>" << endl;
   cout << "  -p <prop> <val>: set property <prop> to value <val>" << endl;
   cout << "  -w <prop> <val>: set a property in the previously specified "
           "workload"
        << endl;
   cout << "Exactly one Load workload is allowed, but multiple Run workloads "
           "may be given.."
        << endl;
   cout << "Run workloads will be executed in the order given on the command "
           "line."
        << endl;
}

inline bool
StrStartWith(const char *str, const char *pre)
{
   return strncmp(str, pre, strlen(pre)) == 0;
}
