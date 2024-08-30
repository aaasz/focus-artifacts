//
//  core_workload.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"

#include <string>

using std::string;
using ycsbc::CoreWorkload;

std::ostream &
operator<<(std::ostream &out, const ycsbc::Operation value)
{
   static std::map<ycsbc::Operation, std::string> strings;
   if (strings.size() == 0) {
#define INSERT_ELEMENT(p) strings[p] = #p
      INSERT_ELEMENT(ycsbc::INSERT);
      INSERT_ELEMENT(ycsbc::READ);
      INSERT_ELEMENT(ycsbc::UPDATE);
      INSERT_ELEMENT(ycsbc::SCAN);
      INSERT_ELEMENT(ycsbc::READMODIFYWRITE);
#undef INSERT_ELEMENT
   }

   return out << strings[value];
}

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT  = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT  = "1";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
   "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT  = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT  = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT  = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT  = "0.9";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT  = "0.1";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT  = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT  = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
   "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
   "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "zipfian";

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT  = "20";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT  = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
   "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT  = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT  = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY    = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const string CoreWorkload::OPS_PER_TRANSACTION_PROPERTY = "opspertransaction";
const string CoreWorkload::OPS_PER_TRANSACTION_DEFAULT  = "1";

const string CoreWorkload::THETA_PROPERTY = "theta";
const string CoreWorkload::THETA_DEFAULT  = "0.99";

const string CoreWorkload::MIN_TXN_ABORT_PANELTY_US_PROPERTY =
   "mintxnabortpaneltyus";
const string CoreWorkload::MIN_TXN_ABORT_PANELTY_US_DEFAULT = "1000";

const string CoreWorkload::MAX_TXN_RETRY_PROPERTY = "maxtxnretry";
const string CoreWorkload::MAX_TXN_RETRY_DEFAULT  = "10";

const std::string CoreWorkload::MAX_TXN_COUNT_PROPERTY = "maxtxncount";
const std::string CoreWorkload::MAX_TXN_COUNT_DEFAULT  = "0";

void
CoreWorkload::InitLoadWorkload(const utils::Properties &p,
                               unsigned int             nthreads,
                               unsigned int             this_thread,
                               BatchedCounterGenerator *key_generator)
{
   table_name_ = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);

   field_count_ =
      std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
   field_len_generator_ = GetFieldLenGenerator(p);

   record_count_ = std::stol(p.GetProperty(RECORD_COUNT_PROPERTY));

   zero_padding_ =
      std::stoi(p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT));

   if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
      ordered_inserts_ = false;
   } else {
      ordered_inserts_ = true;
   }

   generator_.seed(this_thread * 3423452437 + 8349344563457);

   insert_key_sequence_.Set(record_count_);

   key_generator_   = key_generator;
   key_batch_start_ = key_generator_->Next();
   key_generator_batch_.Set(key_batch_start_);
   batch_remaining_ = key_generator_->BatchSize();
}

void
CoreWorkload::DeinitLoadWorkload()
{
   if (field_len_generator_) {
      delete field_len_generator_;
      field_len_generator_ = nullptr;
   }
}


void
CoreWorkload::InitRunWorkload(const utils::Properties &p,
                              unsigned int             nthreads,
                              unsigned int             this_thread)
{
   generator_.seed(this_thread * 3423452437 + 8349344563457);

   read_proportion_ = std::stod(
      p.GetProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_DEFAULT));
   update_proportion_ = std::stod(
      p.GetProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_DEFAULT));
   insert_proportion_ = std::stod(
      p.GetProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_DEFAULT));
   scan_proportion_ = std::stod(
      p.GetProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_DEFAULT));
   readmodifywrite_proportion_ = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));

   std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                            REQUEST_DISTRIBUTION_DEFAULT);
   int         max_scan_len = std::stoi(
      p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
   std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                             SCAN_LENGTH_DISTRIBUTION_DEFAULT);

   read_all_fields_ = utils::StrToBool(
      p.GetProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_DEFAULT));
   write_all_fields_ = utils::StrToBool(
      p.GetProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_DEFAULT));

   ops_per_transaction_ = std::stoi(
      p.GetProperty(OPS_PER_TRANSACTION_PROPERTY, OPS_PER_TRANSACTION_DEFAULT));

   min_txn_abort_panelty_us_ = std::stoi(p.GetProperty(
      MIN_TXN_ABORT_PANELTY_US_PROPERTY, MIN_TXN_ABORT_PANELTY_US_DEFAULT));
   max_txn_retry_ =
      std::stoi(p.GetProperty(MAX_TXN_RETRY_PROPERTY, MAX_TXN_RETRY_DEFAULT));
   max_txn_count_ =
      std::stoul(p.GetProperty(MAX_TXN_COUNT_PROPERTY, MAX_TXN_COUNT_DEFAULT));

   if (read_proportion_ > 0) {
      op_chooser_.AddValue(READ, read_proportion_);
   }
   if (update_proportion_ > 0) {
      op_chooser_.AddValue(UPDATE, update_proportion_);
   }
   if (insert_proportion_ > 0) {
      op_chooser_.AddValue(INSERT, insert_proportion_);
   }
   if (scan_proportion_ > 0) {
      op_chooser_.AddValue(SCAN, scan_proportion_);
   }
   if (readmodifywrite_proportion_ > 0) {
      op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion_);
   }

   if (request_dist == "uniform") {
      key_chooser_ = new UniformGenerator(generator_, 0, record_count_ - 1);
   } else if (request_dist == "zipfian") {
      // If the number of keys changes, we don't want to change popular keys.
      // So we construct the scrambled zipfian generator with a keyspace
      // that is larger than what exists at the beginning of the test.
      // If the generator picks a key that is not inserted yet, we just ignore
      // it and pick another key.
      int op_count;
      if (max_txn_count_ > 0) {
         // This is max op_count
         op_count = max_txn_count_ * nthreads;
      } else {
         op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
      }
      int new_keys = (int)(op_count * insert_proportion_ * 2); // a fudge factor
      // key_chooser_ = new ScrambledZipfianGenerator(generator_, record_count_
      // + new_keys);
      uint64_t num_items = record_count_ + new_keys;
      double   theta = std::stof(p.GetProperty(THETA_PROPERTY, THETA_DEFAULT));
      key_chooser_ =
         new ScrambledZipfianGenerator(generator_, 0, num_items - 1, theta);
   } else if (request_dist == "latest") {
      key_chooser_ = new SkewedLatestGenerator(generator_, *key_generator_);

   } else {
      throw utils::Exception("Unknown request distribution: " + request_dist);
   }

   field_chooser_ = new UniformGenerator(generator_, 0, field_count_ - 1);

   if (scan_len_dist == "uniform") {
      scan_len_chooser_ = new UniformGenerator(generator_, 1, max_scan_len);
   } else if (scan_len_dist == "zipfian") {
      scan_len_chooser_ = new ZipfianGenerator(generator_, 1, max_scan_len);
   } else {
      throw utils::Exception("Distribution not allowed for scan length: "
                             + scan_len_dist);
   }

   // batch_size_ = 1;
}

void
CoreWorkload::DeinitRunWorkload()
{
   op_chooser_.Clear();
   if (key_chooser_) {
      delete key_chooser_;
      key_chooser_ = nullptr;
   }
   if (field_chooser_) {
      delete field_chooser_;
      field_chooser_ = nullptr;
   }
   if (scan_len_chooser_) {
      delete scan_len_chooser_;
      scan_len_chooser_ = nullptr;
   }
}

ycsbc::Generator<uint64_t> *
CoreWorkload::GetFieldLenGenerator(const utils::Properties &p)
{
   string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                         FIELD_LENGTH_DISTRIBUTION_DEFAULT);
   int    field_len =
      std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
   if (field_len_dist == "constant") {
      return new ConstGenerator(field_len);
   } else if (field_len_dist == "uniform") {
      return new UniformGenerator(generator_, 1, field_len);
   } else if (field_len_dist == "zipfian") {
      return new ZipfianGenerator(generator_, 1, field_len);
   } else {
      throw utils::Exception("Unknown field length distribution: "
                             + field_len_dist);
   }
}

void
CoreWorkload::BuildValues(std::vector<ycsbc::DB::KVPair> &values)
{
   values.clear();
   for (int i = 0; i < field_count_; ++i) {
      ycsbc::DB::KVPair pair;
      pair.first.append("field").append(std::to_string(i));
      pair.second.append(field_len_generator_->Next(),
                         uniform_letter_dist_(generator_));
      values.push_back(pair);
   }
}

void
CoreWorkload::InitPairs(std::vector<ycsbc::DB::KVPair> &values)
{
   for (int i = 0; i < field_count_; ++i) {
      ycsbc::DB::KVPair pair;
      pair.first.append("field").append(std::to_string(i));
      pair.second.append(field_len_generator_->Next(), '_');
      values.push_back(pair);
   }
}

void
CoreWorkload::UpdateValues(std::vector<ycsbc::DB::KVPair> &values)
{
   assert(values.size() == (unsigned int)field_count_);
   for (int i = 0; i < field_count_; ++i) {
      values[i].second[0] = uniform_letter_dist_(generator_);
   }
}

void
CoreWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update)
{
   update.clear();
   ycsbc::DB::KVPair pair;
   pair.first.append(NextFieldName());
   pair.second.append(field_len_generator_->Next(),
                      uniform_letter_dist_(generator_));
   update.push_back(pair);
}
