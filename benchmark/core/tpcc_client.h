//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef TPCC_CLIENT_H_
#define TPCC_CLIENT_H_

#include "tpcc_workload.h"
#include "db.h"
#include "utils.h"
#include "timer.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <string>
#include <thread>

namespace tpcc {

class TPCCClient {
public:
   TPCCClient(int thread_id, TPCCWorkload *wl) : _thread_id(thread_id), _wl(wl)
   {
      abort_cnt  = 0;
      commit_cnt = 0;

      abort_cnt_payment   = 0;
      abort_cnt_new_order = 0;

      attempts_payment   = 0;
      attempts_new_order = 0;
   }

   virtual bool
   run_transaction();
   virtual bool
   run_transactions();

   virtual ~TPCCClient()
   {
      // TPCCClient::total_abort_cnt.fetch_add(abort_cnt);
   }

   // getters for commit_cnt and abort_cnt
   int
   GetCommitCnt() const
   {
      return commit_cnt;
   }
   int
   GetAbortCnt() const
   {
      return abort_cnt;
   }

   int
   GetAbortCntPayment() const
   {
      return abort_cnt_payment;
   }

   int
   GetAbortCntNewOrder() const
   {
      return abort_cnt_new_order;
   }

   int
   GetAttemptsPayment() const
   {
      return attempts_payment;
   }

   int
   GetAttemptsNewOrder() const
   {
      return attempts_new_order;
   }

   const std::vector<double> &
   GetCommitTxnLatnecies() const
   {
      return commit_txn_latencies;
   }
   const std::vector<double> &
   GetAbortTxnLatnecies() const
   {
      return abort_txn_latencies;
   }

   // static std::atomic<unsigned long> total_abort_cnt;

protected:
   int           _thread_id;
   TPCCWorkload *_wl;
   unsigned long abort_cnt;
   unsigned long commit_cnt;

   unsigned long abort_cnt_payment;
   unsigned long abort_cnt_new_order;

   unsigned long attempts_payment;
   unsigned long attempts_new_order;

   std::vector<double>  commit_txn_latencies;
   std::vector<double>  abort_txn_latencies;
   utils::Timer<double> timer;
};

inline bool
TPCCClient::run_transaction()
{
   TPCCTransaction txn;
   txn.init(_thread_id);

   tpcc::tpcc_txn_type current_txn_type = txn.type;

   bool     need_retry = false;
   uint32_t retry      = 0;

   timer.Start();
   do {
      switch (current_txn_type) {
         case TPCC_PAYMENT:
            ++attempts_payment;
            break;
         case TPCC_NEW_ORDER:
            ++attempts_new_order;
            break;
         default:
            assert(false);
      }
      need_retry = (_wl->run_transaction(&txn) == ycsbc::DB::kErrorConflict);
      if (need_retry) {
         switch (current_txn_type) {
            case TPCC_PAYMENT:
               ++abort_cnt_payment;
               break;
            case TPCC_NEW_ORDER:
               ++abort_cnt_new_order;
               break;
            default:
               assert(false);
         }
         ++abort_cnt;

         const int sleep_for = std::pow(2.0, retry) * g_abort_penalty_us;
         if (sleep_for > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_for));
         }
         ++retry;
      } else {
         ++commit_cnt;
      }
   } while (need_retry && retry <= g_max_txn_retry);

   double latency_sec = timer.End();
   double latency_us  = latency_sec * 1000000;
   if (need_retry) {
      abort_txn_latencies.push_back(latency_us);
   } else {
      commit_txn_latencies.push_back(latency_us);
   }

   return true;
}

inline bool
TPCCClient::run_transactions()
{
   for (uint32_t i = 0; i < g_total_num_transactions; i++) {
      run_transaction();
   }
   return true;
}

} // namespace tpcc

#endif // TPCC_CLIENT_H_
