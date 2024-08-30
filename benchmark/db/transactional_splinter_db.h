//
//  splinter_db.h
//  YCSB-C
//

#ifndef YCSB_C_TRANSACTIONAL_SPLINTER_DB_H_
#define YCSB_C_TRANSACTIONAL_SPLINTER_DB_H_

#include <iostream>
#include <string>
#include <thread>

#include "core/db.h"
#include "core/properties.h"

extern "C" {
#include "splinterdb/transaction.h"
}

using std::cout;
using std::endl;

namespace ycsbc {

class TransactionalSplinterDB : public DB {
public:
   TransactionalSplinterDB(utils::Properties &props, bool preloaded);
   TransactionalSplinterDB(utils::Properties   &props,
                           bool                 preloaded,
                           merge_tuple_fn       merge,
                           merge_tuple_final_fn merge_final);

   ~TransactionalSplinterDB();

   void
   Init();
   void
   Close();

   int
   Read(const std::string              &table,
        const std::string              &key,
        const std::vector<std::string> *fields,
        std::vector<KVPair>            &result);

   int
   Scan(const std::string                &table,
        const std::string                &key,
        int                               len,
        const std::vector<std::string>   *fields,
        std::vector<std::vector<KVPair>> &result);

   int
   Update(const std::string   &table,
          const std::string   &key,
          std::vector<KVPair> &values);

   int
   Insert(const std::string   &table,
          const std::string   &key,
          std::vector<KVPair> &values);

   int
   Delete(const std::string &table, const std::string &key);

   void
   Begin(Transaction **txn);

   int
   Commit(Transaction **txn);

   int
   Read(Transaction                    *txn,
        const std::string              &table,
        const std::string              &key,
        const std::vector<std::string> *fields,
        std::vector<KVPair>            &result);

   int
   Scan(Transaction                      *txn,
        const std::string                &table,
        const std::string                &key,
        int                               len,
        const std::vector<std::string>   *fields,
        std::vector<std::vector<KVPair>> &result);

   int
   Update(Transaction         *txn,
          const std::string   &table,
          const std::string   &key,
          std::vector<KVPair> &values);

   int
   Insert(Transaction         *txn,
          const std::string   &table,
          const std::string   &key,
          std::vector<KVPair> &values);

   int
   Delete(Transaction *txn, const std::string &table, const std::string &key);

   int
   Store(void *key, uint32_t key_size, void *value, uint32_t value_size);
   int
   Read(Transaction *txn,
        void        *key,
        uint32_t     key_size,
        void        *value,
        uint32_t     value_size);
   int
   Update(Transaction *txn,
          void        *key,
          uint32_t     key_size,
          void        *value,
          uint32_t     value_size);
   int
   Insert(Transaction *txn,
          void        *key,
          uint32_t     key_size,
          void        *value,
          uint32_t     value_size);

   virtual void
   PrintDBStats() const;

private:
   splinterdb_config         splinterdb_cfg;
   data_config               data_cfg;
   transactional_splinterdb *spl;
};

class SplinterDBTransaction : public Transaction {
public:
   SplinterDBTransaction() : Transaction()
   {
      handle = transaction_create();
   };

   ~SplinterDBTransaction()
   {
      transaction_destroy(handle);
   };

private:
   transaction *handle;

   friend TransactionalSplinterDB;
};

} // namespace ycsbc

#endif // YCSB_C_TRANSACTIONAL_SPLINTER_DB_H_
