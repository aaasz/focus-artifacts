#pragma once

#include "client.h"

namespace ycsbc {

class TxnClient : public Client {
public:
   TxnClient(int id, DB &db, CoreWorkload &wl) : Client(id, db, wl) {}

protected:
   void
   GenerateClientTransactionalShortOperations()
   {
      ClientOperation client_op;
      size_t          num_ops = workload_.ops_per_transaction();

      if (workload_.read_proportion() > 0) {
         size_t read_ops = num_ops * workload_.read_proportion() + 0.5;
         read_ops += (read_ops == 0);
         size_t start_num_ops = operations_in_transaction.size();
         while (operations_in_transaction.size() - start_num_ops < read_ops) {
            GenerateClientOperationRead(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.insert_proportion() > 0) {
         size_t insert_ops = num_ops * workload_.insert_proportion() + 0.5;
         insert_ops += (insert_ops == 0);
         size_t start_num_ops = operations_in_transaction.size();
         while (operations_in_transaction.size() - start_num_ops < insert_ops) {
            GenerateClientOperationInsert(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.update_proportion() > 0) {
         size_t update_ops = num_ops * workload_.update_proportion() + 0.5;
         update_ops += (update_ops == 0);
         size_t start_num_ops = operations_in_transaction.size();
         while (operations_in_transaction.size() - start_num_ops < update_ops) {
            GenerateClientOperationUpdate(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.scan_proportion() > 0) {
         size_t scan_ops = num_ops * workload_.scan_proportion() + 0.5;
         scan_ops += (scan_ops == 0);
         size_t start_num_ops = operations_in_transaction.size();
         while (operations_in_transaction.size() - start_num_ops < scan_ops) {
            GenerateClientOperationScan(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.readmodifywrite_proportion() > 0) {
         size_t readmodifywrite_ops =
            num_ops * workload_.readmodifywrite_proportion() + 0.5;
         readmodifywrite_ops += (readmodifywrite_ops == 0);
         size_t start_num_ops = operations_in_transaction.size();
         while (operations_in_transaction.size() - start_num_ops
                < readmodifywrite_ops) {
            GenerateClientOperationReadModifyWrite(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      assert(num_ops == operations_in_transaction.size());

      // for (auto &op : operations_in_transaction) {
      //    std::cout << op.op << " " << op.key << std::endl;
      // }
      // std::cout << "---" << std::endl;
   }

   void
   GenerateClientTransactionalLongOperations()
   {
      ClientOperation client_op;
      size_t          num_ops = workload_.max_row_per_txn();

      if (workload_.long_txn_read_ratio() > 0) {
         size_t read_ops = num_ops * workload_.long_txn_read_ratio() + 0.5;
         read_ops += (read_ops == 0);
         size_t start_num_ops = operations_in_transaction.size();
         while (operations_in_transaction.size() - start_num_ops < read_ops) {
            GenerateClientOperationRead(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      size_t update_ops    = num_ops - operations_in_transaction.size();
      size_t start_num_ops = operations_in_transaction.size();
      while (operations_in_transaction.size() - start_num_ops < update_ops) {
         GenerateClientOperationUpdate(client_op);
         operations_in_transaction.emplace(client_op);
      }
   }

   void
   GenerateClientTransactionalOperations()
   {
      double x;
      drand48_r(&drand_buffer, &x);
      x = (x * 100) / 100;
      if (x < workload_.long_txn_ratio()) {
         GenerateClientTransactionalLongOperations();
      } else {
         GenerateClientTransactionalShortOperations();
      }
   }
};

} // namespace ycsbc