//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "core/benchmark_structs.h"
#include "core/client.h"
#include "core/cmd_parser.h"
#include "core/core_workload.h"
#include "core/numautils.h"
#include "core/progress_reporter.h"
#include "core/stats_reporter.h"
#include "core/timer.h"
#include "core/tpcc_workload.h"
#include "core/txn_client.h"
#include "core/utils.h"
#include "db/db_factory.h"

using namespace std;
using namespace ycsbc;

namespace ycsbc {
// Default properties for the benchmark
std::map<string, string> default_props = {
   {"threadcount", "1"},
   {"dbname", "basic"},
   {"progress", "none"},
   {"basicdb.verbose", "0"},
   {"splinterdb.filename", "splinterdb.db"},
   {"splinterdb.cache_size_mb", "4096"},
   {"splinterdb.disk_size_gb", "1024"},
   {"splinterdb.max_key_size", "24"},
   {"splinterdb.use_log", "1"},
   {"splinterdb.page_size", "0"},
   {"splinterdb.extent_size", "0"},
   {"splinterdb.io_flags", "16450"},
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
   {"splinterdb.isolation_level", "1"},
   {"rocksdb.database_filename", "rocksdb.db"},
};
}

template<class Client = ycsbc::Client>
void DelegateClient(int id, YCSBInput *input, YCSBOutput *output) {
   static std::atomic<bool> run_bench;
   run_bench.store(true);
   input->db->Init();
   Client client(id, *input->db, *input->wl);

   uint64_t txn_cnt = 0;

   if (input->is_loading) {
      for (uint64_t i = 0; i < input->num_ops; ++i) {
         txn_cnt += client.DoInsert();
         ProgressUpdate(input->pmode, input->total_ops, input->global_op_counter, i, input->last_printed);
      }
   } else {
      if (input->benchmark_seconds > 0) {
         input->benchmark_timer.Start();
         while (input->global_num_clients_done->load() < input->total_num_clients) {
            txn_cnt += client.DoTransaction();
            ProgressUpdate(input->pmode, input->total_ops, input->global_op_counter, txn_cnt, input->last_printed);
            double duration = input->benchmark_timer.End();
            if (duration > input->benchmark_seconds) {
               input->global_num_clients_done->fetch_add(1);
            }
         }
         ProgressFinish(input->pmode, input->total_ops, input->global_op_counter, txn_cnt, input->last_printed);
      } else if (input->wl->max_txn_count() > 0) {
         while (txn_cnt < input->wl->max_txn_count() && run_bench.load()) {
            txn_cnt += client.DoTransaction();
            ProgressUpdate(input->pmode, input->total_ops, input->global_op_counter, txn_cnt, input->last_printed);
         }
         ProgressFinish(input->pmode, input->total_ops, input->global_op_counter, txn_cnt, input->last_printed);
         run_bench.store(false);
      } else {
         for (uint64_t i = 0; i < input->num_ops; ++i) {
            txn_cnt += client.DoTransaction();
            ProgressUpdate(input->pmode, input->total_ops, input->global_op_counter, i, input->last_printed);
         }
         ProgressFinish(input->pmode, input->total_ops, input->global_op_counter, input->num_ops, input->last_printed);
      }
   }
   input->db->Close();

   if (output) {
      output->txn_cnt = txn_cnt;
      output->commit_cnt = client.GetTxnCnt();
      output->abort_cnt = client.GetAbortCnt();
      output->commit_txn_latencies = client.GetCommitTxnLatnecies();
      output->abort_txn_latencies = client.GetAbortTxnLatnecies();
      output->abort_cnt_per_txn = client.GetAbortCntPerTxn();
      output->long_txn_cnt = client.GetLongTxnCnt() + client.GetLongTxnAbortCnt();
      output->long_txn_commit_cnt = client.GetLongTxnCnt();
      output->long_txn_abort_cnt = client.GetLongTxnAbortCnt();
      output->long_txn_commit_txn_latencies = client.GetLongTxnCommitTxnLatnecies();
      output->long_txn_abort_txn_latencies = client.GetLongTxnAbortTxnLatnecies();
      output->long_txn_abort_cnt_per_txn = client.GetLongTxnAbortCntPerTxn();
   }
}

int DelegateTPCCClient(uint32_t thread_id, TPCCInput *input, TPCCOutput *stats) {
   input->db->Init();
   tpcc::TPCCClient client(thread_id, input->wl);
   uint64_t txn_cnt = 0;

   if (input->benchmark_seconds > 0) {
      input->benchmark_timer.Start();
      while (input->global_num_clients_done->load() < input->total_num_clients) {
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
      stats->txn_cnt = txn_cnt;
      stats->commit_cnt = client.GetCommitCnt();
      stats->abort_cnt = client.GetAbortCnt();
      stats->abort_cnt_payment = client.GetAbortCntPayment();
      stats->abort_cnt_new_order = client.GetAbortCntNewOrder();
      stats->attempts_payment = client.GetAttemptsPayment();
      stats->attempts_new_order = client.GetAttemptsNewOrder();
      stats->commit_txn_latencies = client.GetCommitTxnLatnecies();
      stats->abort_txn_latencies = client.GetAbortTxnLatnecies();
   }

   input->db->Close();
   return 0;
}

int main(const int argc, const char *argv[]) {
   utils::Properties props;
   WorkloadProperties load_workload;
   vector<WorkloadProperties> run_workloads;
   ParseCommandLine(argc, argv, props, load_workload, run_workloads);

   const unsigned int num_threads = stoi(props.GetProperty("threadcount", "1"));
   progress_mode pmode = no_progress;
   if (props.GetProperty("progress", "none") == "hash") {
      pmode = hash_progress;
   } else if (props.GetProperty("progress", "none") == "percent") {
      pmode = percent_progress;
   }

   const auto cpu_topology = numautils::get_cpu_topology();
   const uint64_t max_cores = cpu_topology.total_threads;
   assert(num_threads <= max_cores && "Number of threads cannot exceed available hardware threads");

   cout << "CPU Topology:" << endl
        << "  Physical Cores: " << cpu_topology.physical_cores << endl
        << "  Threads per Core: " << cpu_topology.threads_per_core << endl
        << "  Total Hardware Threads: " << cpu_topology.total_threads << endl;

   std::vector<size_t> cores = numautils::get_cores(max_cores);

   ycsbc::DB *db;
   utils::Timer<double> timer;
   uint64_t record_count;
   uint64_t total_ops;
   uint64_t total_txn_count;

   if (props.GetProperty("benchmark") == "tpcc") {
      db = new ycsbc::TransactionalSplinterDB(props, load_workload.preloaded,
                                             tpcc::tpcc_merge_tuple,
                                             tpcc::tpcc_merge_tuple_final);

      for (WorkloadProperties workload : run_workloads) {
         tpcc::TPCCWorkload tpcc_wl;
         tpcc_wl.init(workload.props, (ycsbc::TransactionalSplinterDB *)db, num_threads);

         std::vector<TPCCInput> _tpcc_inputs(num_threads);
         std::vector<TPCCOutput> _tpcc_output(num_threads);
         std::vector<std::thread> tpcc_threads;
         std::atomic<uint64_t> num_clients_done(0);

         timer.Start();
         for (unsigned int i = 0; i < num_threads; ++i) {
            _tpcc_inputs[i].db = db;
            _tpcc_inputs[i].wl = &tpcc_wl;
            _tpcc_inputs[i].benchmark_seconds = stof(props.GetProperty("benchmark_seconds", "0"));
            _tpcc_inputs[i].global_num_clients_done = &num_clients_done;
            _tpcc_inputs[i].total_num_clients = num_threads;
            tpcc_threads.emplace_back(std::thread(DelegateTPCCClient, i, &_tpcc_inputs[i], &_tpcc_output[i]));
            numautils::bind_to_core(tpcc_threads[i], cores[i]);
         }

         for (auto &t : tpcc_threads) {
            t.join();
         }

         double run_duration = timer.End();
         if (pmode != no_progress) {
            cout << "\n";
         }
         tpcc_wl.deinit();

         PrintTPCCStats(_tpcc_output, run_duration);
         db->PrintDBStats();
      }
   } else {
      db = ycsbc::DBFactory::CreateDB(props, load_workload.preloaded);
      if (!db) {
         cout << "Unknown database name " << props["dbname"] << endl;
         exit(0);
      }

      const uint64_t num_threads_load = num_threads;
      record_count = stol(load_workload.props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
      uint64_t batch_size = sqrt(record_count);
      if (record_count / batch_size < num_threads_load)
         batch_size = record_count / num_threads_load;
      if (batch_size < 1)
         batch_size = 1;

      ycsbc::BatchedCounterGenerator key_generator(load_workload.preloaded ? record_count : 0, batch_size);
      ycsbc::CoreWorkload wls[num_threads_load];

      for (unsigned int i = 0; i < num_threads_load; ++i) {
         wls[i].InitLoadWorkload(load_workload.props, num_threads_load, i, &key_generator);
      }

      if (!load_workload.preloaded) {
         std::vector<std::thread> load_threads;
         YCSBInput ycsb_inputs[num_threads_load];
         YCSBOutput ycsb_outputs[num_threads_load];

         IOStats io_stats_load_begin = getIOStats();
         timer.Start();

         cout << "# Loading records:\t" << record_count << endl;
         uint64_t load_progress = 0;
         uint64_t last_printed = 0;
         for (unsigned int i = 0; i < num_threads_load; ++i) {
            uint64_t start_op = (record_count * i) / num_threads_load;
            uint64_t end_op = (record_count * (i + 1)) / num_threads_load;
            ycsb_inputs[i].db = db;
            ycsb_inputs[i].wl = &wls[i];
            ycsb_inputs[i].num_ops = end_op - start_op;
            ycsb_inputs[i].is_loading = true;
            ycsb_inputs[i].pmode = pmode;
            ycsb_inputs[i].total_ops = record_count;
            ycsb_inputs[i].global_op_counter = &load_progress;
            ycsb_inputs[i].last_printed = &last_printed;

            load_threads.emplace_back(std::thread(props.GetProperty("client") == "txn"
                                                   ? DelegateClient<ycsbc::TxnClient>
                                                   : DelegateClient<ycsbc::Client>,
                                                i, &ycsb_inputs[i], &ycsb_outputs[i]));
            numautils::bind_to_core(load_threads[i], cores[i]);
         }

         for (auto &t : load_threads) {
            t.join();
         }

         double load_duration = timer.End();
         IOStats io_stats_load_end = getIOStats();

         if (pmode != no_progress) {
            cout << "\n";
         }

         total_txn_count = 0;
         for (unsigned int i = 0; i < num_threads_load; ++i) {
            total_txn_count += ycsb_outputs[i].txn_cnt;
         }

         cout << "# Load throughput (KTPS)" << endl;
         cout << props["dbname"] << '\t' << load_workload.filename << '\t'
              << num_threads_load << '\t';
         cout << total_txn_count / load_duration / 1000 << endl;
         cout << "Load duration (sec):\t" << load_duration << endl;

         // Convert array to vector for PrintYCSBStats
         // std::vector<YCSBOutput> ycsb_outputs_vec(ycsb_outputs, ycsb_outputs + num_threads_load);
         // PrintYCSBStats(ycsb_outputs_vec, total_txn_count, load_duration);
         // db->PrintDBStats();
         PrintIOStats(io_stats_load_begin, io_stats_load_end, load_duration, "Load");
      }

      for (const auto &workload : run_workloads) {
         std::vector<std::thread> run_threads;
         for (unsigned int i = 0; i < num_threads; ++i) {
            run_threads.emplace_back(std::thread(
               [&wls = wls, &workload = workload, num_threads, i]() {
                  wls[i].InitRunWorkload(workload.props, num_threads, i);
               }));
            numautils::bind_to_core(run_threads[i], cores[i]);
         }

         for (auto &t : run_threads) {
            t.join();
         }
         run_threads.clear();

         uint64_t ops_per_transactions = stoi(workload.props.GetProperty(
            ycsbc::CoreWorkload::OPS_PER_TRANSACTION_PROPERTY,
            ycsbc::CoreWorkload::OPS_PER_TRANSACTION_DEFAULT));
         uint64_t max_txn_count = stoi(workload.props.GetProperty(
            ycsbc::CoreWorkload::MAX_TXN_COUNT_PROPERTY, "0"));
         total_ops = max_txn_count > 0
                        ? max_txn_count * num_threads * ops_per_transactions
                        : stoi(workload.props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

         uint64_t run_progress = 0;
         uint64_t last_printed = 0;
         YCSBInput ycsb_inputs[num_threads];
         YCSBOutput ycsb_outputs[num_threads];
         std::atomic<uint64_t> num_clients_done(0);

         IOStats io_stats_run_begin = getIOStats();
         timer.Start();

         for (unsigned int i = 0; i < num_threads; ++i) {
            uint64_t start_op = (total_ops * i) / num_threads;
            uint64_t end_op = (total_ops * (i + 1)) / num_threads;
            uint64_t num_transactions = (end_op - start_op) / ops_per_transactions;
            ycsb_inputs[i].db = db;
            ycsb_inputs[i].wl = &wls[i];
            ycsb_inputs[i].num_ops = num_transactions;
            ycsb_inputs[i].is_loading = false;
            ycsb_inputs[i].pmode = pmode;
            ycsb_inputs[i].total_ops = total_ops;
            ycsb_inputs[i].global_op_counter = &run_progress;
            ycsb_inputs[i].last_printed = &last_printed;
            ycsb_inputs[i].benchmark_seconds = stof(props.GetProperty("benchmark_seconds", "0"));
            ycsb_inputs[i].global_num_clients_done = &num_clients_done;
            ycsb_inputs[i].total_num_clients = num_threads;

            run_threads.emplace_back(std::thread(props.GetProperty("client") == "txn"
                                                  ? DelegateClient<ycsbc::TxnClient>
                                                  : DelegateClient<ycsbc::Client>,
                                               i, &ycsb_inputs[i], &ycsb_outputs[i]));
            numautils::bind_to_core(run_threads[i], cores[i]);
         }

         for (auto &t : run_threads) {
            t.join();
         }

         double run_duration = timer.End();
         IOStats io_stats_run_end = getIOStats();

         if (pmode != no_progress) {
            cout << "\n";
         }

         for (unsigned int i = 0; i < num_threads; ++i) {
            wls[i].DeinitRunWorkload();
         }

         total_txn_count = 0;
         for (unsigned int i = 0; i < num_threads; ++i) {
            total_txn_count += ycsb_outputs[i].txn_cnt;
         }

         bool is_long_txn_enabled = stof(workload.props.GetProperty(
            ycsbc::CoreWorkload::LONG_TXN_RATIO,
            ycsbc::CoreWorkload::LONG_TXN_RATIO_DEFAULT)) > 0.0;

         std::vector<YCSBOutput> ycsb_outputs_vec(ycsb_outputs, ycsb_outputs + num_threads);
         PrintYCSBStats(ycsb_outputs_vec, total_txn_count, run_duration, is_long_txn_enabled);
         db->PrintDBStats();
         PrintIOStats(io_stats_run_begin, io_stats_run_end, run_duration, "Run");
      }
   }

   delete db;
   return 0;
}
