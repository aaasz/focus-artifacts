//
//  discrete_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/6/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DISCRETE_GENERATOR_H_
#define YCSB_C_DISCRETE_GENERATOR_H_

#include "generator.h"

#include "utils.h"
#include <cassert>
#include <random>
#include <vector>

namespace ycsbc {

template<typename Value>
class DiscreteGenerator : public Generator<Value> {
public:
   DiscreteGenerator(std::default_random_engine &gen)
      : generator_(gen), dist_(0.0, 1.0), sum_(0)
   {}
   void
   AddValue(Value value, double weight);
   void
   Clear();
   Value
   Next();
   Value
   Last()
   {
      return last_;
   }

private:
   std::default_random_engine           &generator_;
   std::uniform_real_distribution<float> dist_;
   std::vector<std::pair<Value, double>> values_;
   double                                sum_;
   Value                                 last_;
};

template<typename Value>
inline void
DiscreteGenerator<Value>::AddValue(Value value, double weight)
{
   if (values_.empty()) {
      last_ = value;
   }
   values_.push_back(std::make_pair(value, weight));
   sum_ += weight;
}

template<typename Value>
inline void
DiscreteGenerator<Value>::Clear()
{
   values_.clear();
   sum_ = 0;
}

template<typename Value>
inline Value
DiscreteGenerator<Value>::Next()
{
   double chooser = dist_(generator_);

   for (auto p = values_.cbegin(); p != values_.cend(); ++p) {
      if (chooser < p->second / sum_) {
         return last_ = p->first;
      }
      chooser -= p->second / sum_;
   }

   assert(false);
   return last_;
}

// XXX: This function is modified for the experimental purpose.
// Don't merge to the main branch.
// This returns a value in a round robin manner regardless of weight
// template <typename Value> inline Value DiscreteGenerator<Value>::Next() {
//   static unsigned long i = 0;
//   last_ = values_[i].first;
//   i = (i + 1) % values_.size();
//   return last_;
// }

} // namespace ycsbc

#endif // YCSB_C_DISCRETE_GENERATOR_H_
