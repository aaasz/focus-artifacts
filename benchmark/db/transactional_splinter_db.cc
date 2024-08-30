//
//  splinter_db.cc
//  YCSB-C
//
//  Created by Rob Johnson on 3/20/2022.
//  Copyright (c) 2022 VMware.
//

#include "db/transactional_splinter_db.h"
extern "C" {
#include "splinterdb/default_data_config.h"
}

#include <string>
#include <vector>
#include <cstring>

using std::string;
using std::vector;

namespace ycsbc {

TransactionalSplinterDB::TransactionalSplinterDB(
   utils::Properties   &props,
   bool                 preloaded,
   merge_tuple_fn       merge,
   merge_tuple_final_fn merge_final)
{
   cout << "This is TransacionalSplinterDB\n";
   isTransactionSupported = true;

   uint64_t max_key_size = props.GetIntProperty("splinterdb.max_key_size");

   default_data_config_init(max_key_size, &data_cfg);
   if (merge)
      data_cfg.merge_tuples = merge;
   if (merge_final)
      data_cfg.merge_tuples_final = merge_final;

   memset(&splinterdb_cfg, 0, sizeof(splinterdb_cfg));
   splinterdb_cfg.filename = props.GetProperty("splinterdb.filename").c_str();
   splinterdb_cfg.cache_size =
      props.GetIntProperty("splinterdb.cache_size_mb") * 1024 * 1024;
   splinterdb_cfg.disk_size =
      props.GetIntProperty("splinterdb.disk_size_gb") * 1024 * 1024 * 1024;
   splinterdb_cfg.data_cfg    = &data_cfg;
   splinterdb_cfg.heap_handle = NULL;
   splinterdb_cfg.heap_id     = NULL;
   splinterdb_cfg.page_size   = props.GetIntProperty("splinterdb.page_size");
   splinterdb_cfg.extent_size = props.GetIntProperty("splinterdb.extent_size");
   splinterdb_cfg.io_flags    = props.GetIntProperty("splinterdb.io_flags");
   splinterdb_cfg.io_perms    = props.GetIntProperty("splinterdb.io_perms");
   splinterdb_cfg.io_async_queue_depth =
      props.GetIntProperty("splinterdb.io_async_queue_depth");
   splinterdb_cfg.cache_use_stats =
      props.GetIntProperty("splinterdb.cache_use_stats");
   splinterdb_cfg.cache_logfile =
      props.GetProperty("splinterdb.cache_logfile").c_str();
   splinterdb_cfg.btree_rough_count_height =
      props.GetIntProperty("splinterdb.btree_rough_count_height");
   splinterdb_cfg.filter_remainder_size =
      props.GetIntProperty("splinterdb.filter_remainder_size");
   splinterdb_cfg.filter_index_size =
      props.GetIntProperty("splinterdb.filter_index_size");
   splinterdb_cfg.use_log = props.GetIntProperty("splinterdb.use_log");
   splinterdb_cfg.memtable_capacity =
      props.GetIntProperty("splinterdb.memtable_capacity");
   splinterdb_cfg.fanout = props.GetIntProperty("splinterdb.fanout");
   splinterdb_cfg.max_branches_per_node =
      props.GetIntProperty("splinterdb.max_branches_per_node");
   splinterdb_cfg.use_stats = props.GetIntProperty("splinterdb.use_stats");
   splinterdb_cfg.reclaim_threshold =
      props.GetIntProperty("splinterdb.reclaim_threshold");
   splinterdb_cfg.num_memtable_bg_threads =
      props.GetIntProperty("splinterdb.num_memtable_bg_threads");
   splinterdb_cfg.num_normal_bg_threads =
      props.GetIntProperty("splinterdb.num_normal_bg_threads");
   splinterdb_cfg.io_contexts_per_process =
      props.GetIntProperty("splinterdb.io_contexts_per_process");

   if (preloaded) {
      assert(!transactional_splinterdb_open(&splinterdb_cfg, &spl));
   } else {
      assert(!transactional_splinterdb_create(&splinterdb_cfg, &spl));
   }

   transactional_splinterdb_set_isolation_level(
      spl,
      (transaction_isolation_level)props.GetIntProperty(
         "splinterdb.isolation_level"));
   if (stoi(props.GetProperty("splinterdb.disable_upsert", "0")) == 1) {
      transactional_splinterdb_disable_upsert(spl);
   }
}

TransactionalSplinterDB::TransactionalSplinterDB(utils::Properties &props,
                                                 bool               preloaded)
   : TransactionalSplinterDB(props, preloaded, NULL, NULL)
{}

TransactionalSplinterDB::~TransactionalSplinterDB()
{
   transactional_splinterdb_close(&spl);
}

void
TransactionalSplinterDB::Init()
{
   transactional_splinterdb_register_thread(spl);
}

void
TransactionalSplinterDB::Close()
{
   transactional_splinterdb_deregister_thread(spl);
}

int
TransactionalSplinterDB::Read(Transaction          *txn,
                              const string         &table,
                              const string         &key,
                              const vector<string> *fields,
                              vector<KVPair>       &result)
{
   assert(txn != NULL);

   splinterdb_lookup_result lookup_result;
   transactional_splinterdb_lookup_result_init(
      spl,
      &lookup_result,
      result[0].second.size(),
      (char *)result[0].second.c_str());
   slice key_slice = slice_create(key.size(), key.c_str());
   // cout << "lookup " << key << endl;

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   if (transactional_splinterdb_lookup(
          spl, txn_handle, key_slice, &lookup_result)
       != 0)
      return DB::kErrorConflict;
   // if (!splinterdb_lookup_found(&lookup_result)) {
   //    cout << "FAILED lookup " << key << endl;
   //    assert(0);
   // }
   // cout << "done lookup " << key << endl;

   // slice value;
   // splinterdb_lookup_result_value(&lookup_result, // IN
   //                                &value);
   // result.emplace_back(make_pair(key, (char *)slice_data(value)));

   // TODO: do we need to call this even if read didn't happen?
   splinterdb_lookup_result_deinit(&lookup_result);
   return DB::kOK;
}

int
TransactionalSplinterDB::Scan(Transaction            *txn,
                              const string           &table,
                              const string           &key,
                              int                     len,
                              const vector<string>   *fields,
                              vector<vector<KVPair>> &result)
{
   assert(txn != NULL);
   assert(fields == NULL);

   return DB::kErrorNotSupport;
}

int
TransactionalSplinterDB::Update(Transaction    *txn,
                                const string   &table,
                                const string   &key,
                                vector<KVPair> &values)
{
   assert(txn != NULL);
   assert(values.size() == 1);

   std::string val       = values[0].second;
   slice       key_slice = slice_create(key.size(), key.c_str());
   slice       val_slice = slice_create(val.size(), val.c_str());
   // cout << "update " << key << endl;

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   if (transactional_splinterdb_update(spl, txn_handle, key_slice, val_slice)
       != 0)
      return DB::kErrorConflict;
   // cout << "done update " << key << endl;

   return DB::kOK;
}

int
TransactionalSplinterDB::Insert(Transaction    *txn,
                                const string   &table,
                                const string   &key,
                                vector<KVPair> &values)
{
   assert(txn != NULL);
   assert(values.size() == 1);

   std::string val       = values[0].second;
   slice       key_slice = slice_create(key.size(), key.c_str());
   slice       val_slice = slice_create(val.size(), val.c_str());
   // cout << "insert " << key << endl;

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   if (transactional_splinterdb_insert(spl, txn_handle, key_slice, val_slice)
       != 0)
      return DB::kErrorConflict;
   // cout << "done insert " << key << endl;

   return DB::kOK;
}

int
TransactionalSplinterDB::Delete(Transaction  *txn,
                                const string &table,
                                const string &key)
{
   slice key_slice = slice_create(key.size(), key.c_str());

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   assert(!transactional_splinterdb_delete(spl, txn_handle, key_slice));

   return DB::kOK;
}

void
TransactionalSplinterDB::Begin(Transaction **txn)
{
   assert(*txn == NULL);
   *txn                    = new SplinterDBTransaction();
   transaction *txn_handle = ((SplinterDBTransaction *)*txn)->handle;
   transactional_splinterdb_begin(spl, txn_handle);
}

int
TransactionalSplinterDB::Commit(Transaction **txn)
{
   int          ret        = DB::kOK;
   transaction *txn_handle = ((SplinterDBTransaction *)*txn)->handle;
   if (transactional_splinterdb_commit(spl, txn_handle) < 0) {
      ret = DB::kErrorConflict;
   }

   delete *txn;
   *txn = NULL;
   return ret;
}

int
TransactionalSplinterDB::Read(const std::string              &table,
                              const std::string              &key,
                              const std::vector<std::string> *fields,
                              std::vector<KVPair>            &result)
{
   return Read(NULL, table, key, fields, result);
}
///
/// Performs a range scan for a set of records in the database.
/// Field/value pairs from the result are stored in a vector.
///
/// @param table The name of the table.
/// @param key The key of the first record to read.
/// @param record_count The number of records to read.
/// @param fields The list of fields to read, or NULL for all of them.
/// @param result A vector of vector, where each vector contains field/value
///        pairs for one record
/// @return Zero on success, or a non-zero error code on error.
///
int
TransactionalSplinterDB::Scan(const std::string                &table,
                              const std::string                &key,
                              int                               record_count,
                              const std::vector<std::string>   *fields,
                              std::vector<std::vector<KVPair>> &result)
{
   return Scan(NULL, table, key, record_count, fields, result);
}

///
/// Updates a record in the database.
/// Field/value pairs in the specified vector are written to the record,
/// overwriting any existing values with the same field names.
///
/// @param table The name of the table.
/// @param key The key of the record to write.
/// @param values A vector of field/value pairs to update in the record.
/// @return Zero on success, a non-zero error code on error.
///
int
TransactionalSplinterDB::Update(const std::string   &table,
                                const std::string   &key,
                                std::vector<KVPair> &values)
{
   return Update(NULL, table, key, values);
}
///
/// Inserts a record into the database.
/// Field/value pairs in the specified vector are written into the record.
///
/// @param table The name of the table.
/// @param key The key of the record to insert.
/// @param values A vector of field/value pairs to insert in the record.
/// @return Zero on success, a non-zero error code on error.
///
int
TransactionalSplinterDB::Insert(const std::string   &table,
                                const std::string   &key,
                                std::vector<KVPair> &values)
{

   assert(values.size() == 1);

   std::string val       = values[0].second;
   slice       key_slice = slice_create(key.size(), key.c_str());
   slice       val_slice = slice_create(val.size(), val.c_str());
   // cout << "insert " << key << endl;
   transactional_splinterdb_insert(spl, NULL, key_slice, val_slice);
   // const splinterdb *non_txn_spl = transactional_splinterdb_get_db(spl);
   // splinterdb_insert(non_txn_spl, key_slice, val_slice);
   // cout << "done insert " << key << endl;

   return DB::kOK;


   // return Insert(NULL, table, key, values);
}
///
/// Deletes a record from the database.
///
/// @param table The name of the table.
/// @param key The key of the record to delete.
/// @return Zero on success, a non-zero error code on error.
///
int
TransactionalSplinterDB::Delete(const std::string &table,
                                const std::string &key)
{
   return Delete(NULL, table, key);
}


int
TransactionalSplinterDB::Store(void    *key,
                               uint32_t key_size,
                               void    *value,
                               uint32_t value_size)
{
   slice key_slice = slice_create(key_size, key);
   slice val_slice = slice_create(value_size, value);
   // printf("Store key (%lu, %lu)\n", *(uint64_t*)key, *((uint64_t*)key+1));

   transactional_splinterdb_insert(spl, NULL, key_slice, val_slice);
   // const splinterdb *non_txn_spl = transactional_splinterdb_get_db(spl);
   // splinterdb_insert(non_txn_spl, key_slice, val_slice);

   return DB::kOK;
}

///
/// Print splinterdb stats.
/// It requires to set the config "use_stats" to 1.
///
void
TransactionalSplinterDB::PrintDBStats() const
{
   splinterdb *db = (splinterdb *)transactional_splinterdb_get_db(spl);
   splinterdb_stats_print_insertion(db);
   splinterdb_stats_print_lookup(db);
   splinterdb_stats_reset(db);
}

int
TransactionalSplinterDB::Read(Transaction *txn,
                              void        *key,
                              uint32_t     key_size,
                              void        *value,
                              uint32_t     value_size)
{
   assert(txn != NULL);

   splinterdb_lookup_result lookup_result;
   transactional_splinterdb_lookup_result_init(
      spl, &lookup_result, value_size, (char *)value);
   slice key_slice = slice_create(key_size, key);
   // cout << "lookup " << key << endl;

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   if (transactional_splinterdb_lookup(
          spl, txn_handle, key_slice, &lookup_result)
       != 0)
      return DB::kErrorConflict;

   // assert(!transactional_splinterdb_lookup(
   //    spl, txn_handle, key_slice, &lookup_result));
   //  if (!splinterdb_lookup_found(&lookup_result)) {
   //     cout << "FAILED lookup " << key << endl;
   //     assert(0);
   //  }
   //  cout << "done lookup " << key << endl;

   // TODO: transactional_splinterdb_lookup_result_deinit();
   splinterdb_lookup_result_deinit(&lookup_result);
   return DB::kOK;
}

int
TransactionalSplinterDB::Update(Transaction *txn,
                                void        *key,
                                uint32_t     key_size,
                                void        *value,
                                uint32_t     value_size)
{
   assert(txn != NULL);

   slice key_slice = slice_create(key_size, key);
   slice val_slice = slice_create(value_size, value);
   // cout << "update " << key << endl;

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   if (transactional_splinterdb_update(spl, txn_handle, key_slice, val_slice)
       != 0)
      return DB::kErrorConflict;
   // cout << "done update " << key << endl;

   return DB::kOK;
}

int
TransactionalSplinterDB::Insert(Transaction *txn,
                                void        *key,
                                uint32_t     key_size,
                                void        *value,
                                uint32_t     value_size)
{
   assert(txn != NULL);

   slice key_slice = slice_create(key_size, key);
   slice val_slice = slice_create(value_size, value);
   // cout << "insert " << key << endl;

   transaction *txn_handle = ((SplinterDBTransaction *)txn)->handle;
   if (transactional_splinterdb_insert(spl, txn_handle, key_slice, val_slice)
       != 0)
      return DB::kErrorConflict;
   // cout << "done insert " << key << endl;

   return DB::kOK;
}

} // namespace ycsbc
