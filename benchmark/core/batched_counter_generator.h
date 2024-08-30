//
//  batched_counter_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BATCHED_COUNTER_GENERATOR_H_
#define YCSB_C_BATCHED_COUNTER_GENERATOR_H_

#include "generator.h"

#include <cstdint>
#include <atomic>
#include <mutex>
#include <set>

namespace ycsbc {

class BatchedCounterGenerator : public Generator<uint64_t> {
public:
   BatchedCounterGenerator(uint64_t start, uint64_t batch_size)
      : start_(start),
        counter_(0),
        batch_size_(batch_size),
        mutex_(),
        num_completed_batches_(0),
        outstanding_()
   {}

   uint64_t
   Next()
   {
      std::lock_guard<std::mutex> lock(mutex_);
      uint64_t                    result_batch = counter_++;
      assert(outstanding_.count(result_batch) == 0);
      outstanding_.insert(result_batch);
      return start_ + result_batch * batch_size_;
   }
   uint64_t
   BatchSize()
   {
      return batch_size_;
   }
   uint64_t
   Last()
   {
      return start_ + num_completed_batches_ * batch_size_;
   }
   uint64_t
   Set(uint64_t start)
   {
      assert(false);
   }

   void
   MarkCompleted(uint64_t batch_start)
   {
      std::lock_guard<std::mutex> lock(mutex_);
      uint64_t batch_num = (batch_start - start_) / batch_size_;
      assert(outstanding_.count(batch_num) == 1);
      outstanding_.erase(batch_num);
      while (num_completed_batches_ < counter_
             && outstanding_.count(num_completed_batches_) == 0)
      {
         num_completed_batches_++;
      }
   }

private:
   uint64_t              start_;
   uint64_t              counter_;
   uint64_t              batch_size_;
   std::mutex            mutex_;
   std::atomic<uint64_t> num_completed_batches_;
   std::set<uint64_t>    outstanding_;
};

} // namespace ycsbc

#endif // YCSB_C_COUNTER_GENERATOR_H_
