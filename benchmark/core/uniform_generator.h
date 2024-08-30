//
//  uniform_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/6/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UNIFORM_GENERATOR_H_
#define YCSB_C_UNIFORM_GENERATOR_H_

#include "generator.h"

#include <random>

namespace ycsbc {

class UniformGenerator : public Generator<uint64_t> {
public:
   // Both min and max are inclusive
   UniformGenerator(std::default_random_engine &generator,
                    uint64_t                    min,
                    uint64_t                    max)
      : generator_(generator), dist_(min, max)
   {
      Next();
   }

   uint64_t
   Next();
   uint64_t
   Last();

private:
   std::default_random_engine             &generator_;
   std::uniform_int_distribution<uint64_t> dist_;
   uint64_t                                last_int_;
};

inline uint64_t
UniformGenerator::Next()
{
   return last_int_ = dist_(generator_);
}

inline uint64_t
UniformGenerator::Last()
{
   return last_int_;
}

} // namespace ycsbc

#endif // YCSB_C_UNIFORM_GENERATOR_H_
