//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "db.h"
#include "utils.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_set>
#include "timer.h"

namespace ycsbc {

struct ClientOperation {
   enum Operation           op;
   std::string              table;
   std::string              key;
   size_t                   len;
   std::vector<std::string> read_fields;
   std::vector<DB::KVPair>  values;

   inline bool
   operator==(const ClientOperation &other) const
   {
      return table == other.table && key == other.key; // && op == other.op;
   }
};

} // namespace ycsbc

namespace std {
template<>
struct hash<ycsbc::ClientOperation> {
   size_t
   operator()(const ycsbc::ClientOperation &op) const
   {
      size_t hash_table = std::hash<std::string>{}(op.table);
      size_t hash_key   = std::hash<std::string>{}(op.key);
      // size_t hash_op    = std::hash<int>{}(op.op);
      return hash_table ^ hash_key; // ^ (hash_op << 1);
   }
};
} // namespace std

namespace ycsbc {

class Client {
public:
   Client(int id, DB &db, CoreWorkload &wl) : id_(id), db_(db), workload_(wl)
   {
      workload_.InitKeyBuffer(key);
      workload_.InitPairs(pairs);

      abort_cnt          = 0;
      txn_cnt            = 0;
      long_txn_abort_cnt = 0;
      long_txn_cnt       = 0;

      srand48_r(id, &drand_buffer);
   }

   virtual bool
   DoInsert();
   virtual bool
   DoTransaction();

   virtual ~Client() {}

   // getters for txn_cnt and abort_cnt
   int
   GetTxnCnt() const
   {
      return txn_cnt;
   }
   int
   GetLongTxnCnt() const
   {
      return long_txn_cnt;
   }
   int
   GetAbortCnt() const
   {
      return abort_cnt;
   }
   int
   GetLongTxnAbortCnt() const
   {
      return long_txn_abort_cnt;
   }
   const std::vector<double> &
   GetCommitTxnLatnecies() const
   {
      return commit_txn_latencies;
   }
   const std::vector<double> &
   GetLongTxnCommitTxnLatnecies() const
   {
      return long_txn_commit_txn_latencies;
   }
   const std::vector<double> &
   GetAbortTxnLatnecies() const
   {
      return abort_txn_latencies;
   }
   const std::vector<double> &
   GetLongTxnAbortTxnLatnecies() const
   {
      return long_txn_abort_txn_latencies;
   }
   const std::vector<unsigned long> &
   GetAbortCntPerTxn() const
   {
      return abort_cnt_per_txn;
   }
   const std::vector<unsigned long> &
   GetLongTxnAbortCntPerTxn() const
   {
      return long_txn_abort_cnt_per_txn;
   }

protected:
   virtual bool
   DoOperation();
   virtual void
   GenerateClientTransactionalOperations();
   virtual bool
   DoTransactionalOperations();

   virtual int
   TransactionRead(Transaction *txn, ClientOperation &client_op);
   virtual int
   TransactionReadModifyWrite(Transaction *txn, ClientOperation &client_op);
   virtual int
   TransactionScan(Transaction *txn, ClientOperation &client_op);
   virtual int
   TransactionUpdate(Transaction *txn, ClientOperation &client_op);
   virtual int
   TransactionInsert(Transaction *txn, ClientOperation &client_op);

   virtual void
   GenerateClientOperationRead(ClientOperation &client_op);
   virtual void
   GenerateClientOperationReadModifyWrite(ClientOperation &client_op);
   virtual void
   GenerateClientOperationScan(ClientOperation &client_op);
   virtual void
   GenerateClientOperationUpdate(ClientOperation &client_op);
   virtual void
   GenerateClientOperationInsert(ClientOperation &client_op);

   int id_;

   DB                     &db_;
   CoreWorkload           &workload_;
   std::string             key;
   std::vector<DB::KVPair> pairs;

   // std::vector<ClientOperation> operations_in_transaction;
   std::unordered_set<ClientOperation> operations_in_transaction;
   unsigned long                       abort_cnt;
   unsigned long                       long_txn_abort_cnt;
   unsigned long                       txn_cnt;
   unsigned long                       long_txn_cnt;
   drand48_data                        drand_buffer;

   utils::Timer<double>       timer;
   std::vector<double>        commit_txn_latencies;
   std::vector<double>        long_txn_commit_txn_latencies;
   std::vector<double>        abort_txn_latencies;
   std::vector<double>        long_txn_abort_txn_latencies;
   std::vector<unsigned long> abort_cnt_per_txn;
   std::vector<unsigned long> long_txn_abort_cnt_per_txn;
};

inline bool
Client::DoInsert()
{
   workload_.NextSequenceKey(key);
   workload_.UpdateValues(pairs);
   return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);

   // int status = -1;
   // if (db_.IsTransactionSupported()) {
   //    Transaction *txn = NULL;
   //    db_.Begin(&txn);
   //    status = db_.Insert(txn, workload_.NextTable(), key, pairs);
   //    db_.Commit(&txn);
   // } else {
   //    status = db_.Insert(NULL, workload_.NextTable(), key, pairs);
   // }

   // return (status == DB::kOK);
}

inline bool
Client::DoTransaction()
{
   //   if (db_.IsTransactionSupported()) {
   return DoTransactionalOperations();
   //   }

   return DoOperation();
}

inline bool
Client::DoOperation()
{
   int status = -1;

   ClientOperation client_op;

   switch (workload_.NextOperation()) {
      case READ:
         GenerateClientOperationRead(client_op);
         status = TransactionRead(NULL, client_op);
         break;
      case UPDATE:
         GenerateClientOperationUpdate(client_op);
         status = TransactionUpdate(NULL, client_op);
         break;
      case INSERT:
         GenerateClientOperationInsert(client_op);
         status = TransactionInsert(NULL, client_op);
         break;
      case SCAN:
         GenerateClientOperationScan(client_op);
         status = TransactionScan(NULL, client_op);
         break;
      case READMODIFYWRITE:
         GenerateClientOperationReadModifyWrite(client_op);
         status = TransactionReadModifyWrite(NULL, client_op);
         break;
      default:
         throw utils::Exception("Operation request is not recognized!");
   }
   assert(status >= 0);

   ++txn_cnt;
   return true;
}

void
Client::GenerateClientTransactionalOperations()
{
   int num_ops = workload_.ops_per_transaction();

   // for (int i = 0; i < num_ops; ++i)
   while (operations_in_transaction.size() < (size_t)num_ops) {
      Operation op = workload_.NextOperation();

      ClientOperation client_op;

      switch (op) {
         case READ:
            GenerateClientOperationRead(client_op);
            break;
         case UPDATE:
            GenerateClientOperationUpdate(client_op);
            break;
         case INSERT:
            GenerateClientOperationInsert(client_op);
            break;
         case SCAN:
            GenerateClientOperationScan(client_op);
            break;
         case READMODIFYWRITE:
            GenerateClientOperationReadModifyWrite(client_op);
            break;
         default:
            throw utils::Exception("Operation request is not recognized!");
      }
      operations_in_transaction.emplace(client_op);
   }

   // for (auto &op : operations_in_transaction) {
   //    std::cout << op.op << " " << op.key << std::endl;
   // }
   // std::cout << "---" << std::endl;
}

inline bool
Client::DoTransactionalOperations()
{
   timer.Start();
   GenerateClientTransactionalOperations();

   bool is_abort = false;
   int  retry    = 0;
   // A long txn should have max_row_per_txn
   bool is_long_txn = operations_in_transaction.size()
                      > (size_t)workload_.ops_per_transaction();
   do {
      is_abort            = false;
      int          status = -1;
      Transaction *txn    = NULL;
      db_.Begin(&txn);

      for (ClientOperation client_op : operations_in_transaction) {
         switch (client_op.op) {
            case READ:
               status = TransactionRead(txn, client_op);
               break;
            case UPDATE:
               status = TransactionUpdate(txn, client_op);
               break;
            case INSERT:
               status = TransactionInsert(txn, client_op);
               break;
            case SCAN:
               status = TransactionScan(txn, client_op);
               break;
            case READMODIFYWRITE:
               status = TransactionReadModifyWrite(txn, client_op);
               break;
            default:
               throw utils::Exception("Operation request is not recognized!");
         }
         assert(status >= 0);
         if (status != 0) {
            is_abort = true;
            break;
         }
      }

      if (!is_abort) {
         is_abort = (db_.Commit(&txn) == DB::kErrorConflict);
      }

      if (is_abort) {
         if (is_long_txn) {
            ++long_txn_abort_cnt;
         } else {
            ++abort_cnt;
         }
         const int sleep_for =
            std::pow(2.0, retry) * workload_.min_txn_abort_panelty_us();
         if (sleep_for > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_for));
         }
         ++retry;
      }
   } while (is_abort && retry <= workload_.max_txn_retry());

   double latency_sec = timer.End();
   double latency_us  = latency_sec * 1000000;
   if (is_abort) {
      if (is_long_txn) {
         long_txn_abort_txn_latencies.push_back(latency_us);
      } else {
         abort_txn_latencies.push_back(latency_us);
      }
   } else {
      if (is_long_txn) {
         long_txn_commit_txn_latencies.push_back(latency_us);
      } else {
         commit_txn_latencies.push_back(latency_us);
      }
   }

   if (is_long_txn) {
      long_txn_abort_cnt_per_txn.push_back(retry);
      long_txn_cnt += !is_abort;
   } else {
      abort_cnt_per_txn.push_back(retry);
      txn_cnt += !is_abort;
   }

   operations_in_transaction.clear();

   return true;
}

inline int
Client::TransactionRead(Transaction *txn, ClientOperation &client_op)
{
   if (!workload_.read_all_fields()) {
      std::vector<std::string> fields;
      fields.push_back("field" + workload_.NextFieldName());
      return db_.Read(txn, client_op.table, client_op.key, &fields, pairs);
   } else {
      return db_.Read(txn, client_op.table, client_op.key, NULL, pairs);
   }
}

inline int
Client::TransactionReadModifyWrite(Transaction *txn, ClientOperation &client_op)
{
   int ret;
   if (!workload_.read_all_fields()) {
      ret = db_.Read(
         txn, client_op.table, client_op.key, &client_op.read_fields, pairs);
   } else {
      ret = db_.Read(txn, client_op.table, client_op.key, NULL, pairs);
   }

   if (ret != 0)
      return ret;

   return db_.Insert(txn, client_op.table, client_op.key, client_op.values);
}

inline int
Client::TransactionScan(Transaction *txn, ClientOperation &client_op)
{
   std::vector<std::vector<DB::KVPair>> result;
   if (!workload_.read_all_fields()) {
      return db_.Scan(txn,
                      client_op.table,
                      client_op.key,
                      client_op.len,
                      &client_op.read_fields,
                      result);
   } else {
      return db_.Scan(
         txn, client_op.table, client_op.key, client_op.len, NULL, result);
   }
}

inline int
Client::TransactionUpdate(Transaction *txn, ClientOperation &client_op)
{
   return db_.Update(txn, client_op.table, client_op.key, client_op.values);
}

inline int
Client::TransactionInsert(Transaction *txn, ClientOperation &client_op)
{
   return db_.Insert(txn, client_op.table, client_op.key, client_op.values);
}

inline void
Client::GenerateClientOperationRead(ClientOperation &client_op)
{
   client_op.op    = READ;
   client_op.table = workload_.NextTable();
   client_op.key   = workload_.NextTransactionKey();
}

inline void
Client::GenerateClientOperationReadModifyWrite(ClientOperation &client_op)
{
   client_op.op    = READMODIFYWRITE;
   client_op.table = workload_.NextTable();
   client_op.key   = workload_.NextTransactionKey();

   if (!workload_.read_all_fields()) {
      client_op.read_fields.push_back("field" + workload_.NextFieldName());
   }

   if (workload_.write_all_fields()) {
      workload_.BuildValues(client_op.values);
   } else {
      workload_.BuildUpdate(client_op.values);
   }
}

inline void
Client::GenerateClientOperationScan(ClientOperation &client_op)
{
   client_op.op    = SCAN;
   client_op.table = workload_.NextTable();
   client_op.key   = workload_.NextTransactionKey();
   client_op.len   = workload_.NextScanLength();

   if (!workload_.read_all_fields()) {
      client_op.read_fields.push_back("field" + workload_.NextFieldName());
   }
}

inline void
Client::GenerateClientOperationUpdate(ClientOperation &client_op)
{
   client_op.op    = UPDATE;
   client_op.table = workload_.NextTable();
   client_op.key   = workload_.NextTransactionKey();

   if (workload_.write_all_fields()) {
      workload_.BuildValues(client_op.values);
   } else {
      workload_.BuildUpdate(client_op.values);
   }
}

inline void
Client::GenerateClientOperationInsert(ClientOperation &client_op)
{
   workload_.NextSequenceKey(key);
   client_op.op    = INSERT;
   client_op.table = workload_.NextTable();
   client_op.key   = key;
   workload_.BuildValues(client_op.values);
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
