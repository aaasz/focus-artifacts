//
//  rocks_db.h
//  YCSB-C
//

#ifndef YCSB_C_ROCKS_DB_H_
#define YCSB_C_ROCKS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "core/properties.h"
#include "rocksdb/db.h"

using std::cout;
using std::endl;

namespace ycsbc {

class RocksDB : public DB {
public:
   RocksDB(utils::Properties &props, bool preloaded);
   ~RocksDB();

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

private:
   void
   InitializeOptions(utils::Properties &props);

   rocksdb::DB          *db;
   rocksdb::Options      options;
   rocksdb::ReadOptions  roptions;
   rocksdb::WriteOptions woptions;
};

} // namespace ycsbc

#endif // YCSB_C_ROCKS_DB_H_
