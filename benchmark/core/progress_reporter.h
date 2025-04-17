#pragma once

#include <cstdint>
#include <iostream>
#include "benchmark_structs.h"

namespace ycsbc {

static inline void ReportProgress(progress_mode      pmode,
                                uint64_t           total_ops,
                                volatile uint64_t *global_op_counter,
                                uint64_t           stepsize,
                                volatile uint64_t *last_printed)
{
   uint64_t old_counter = __sync_fetch_and_add(global_op_counter, stepsize);
   uint64_t new_counter = old_counter + stepsize;
   if (100 * old_counter / total_ops != 100 * new_counter / total_ops) {
      if (pmode == hash_progress) {
         std::cout << "#" << std::flush;
      } else if (pmode == percent_progress) {
         uint64_t my_percent = 100 * new_counter / total_ops;
         while (*last_printed + 1 != my_percent) {
         }
         std::cout << 100 * new_counter / total_ops << "%\r" << std::flush;
         *last_printed = my_percent;
      }
   }
}

static inline void ProgressUpdate(progress_mode      pmode,
                                uint64_t           total_ops,
                                volatile uint64_t *global_op_counter,
                                uint64_t           i,
                                volatile uint64_t *last_printed)
{
   uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
   if ((i % sync_interval) == 0) {
      ReportProgress(pmode, total_ops, global_op_counter, sync_interval, last_printed);
   }
}

static inline void ProgressFinish(progress_mode      pmode,
                                uint64_t           total_ops,
                                volatile uint64_t *global_op_counter,
                                uint64_t           i,
                                volatile uint64_t *last_printed)
{
   uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
   ReportProgress(pmode, total_ops, global_op_counter, i % sync_interval, last_printed);
}

} // namespace ycsbc 