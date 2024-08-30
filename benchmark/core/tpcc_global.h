#ifndef _TPCC_GLOBAL_H_
#define _TPCC_GLOBAL_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <cassert>
#include "tpcc_config.h"

namespace tpcc {

// random generator per warehouse
extern drand48_data *tpcc_buffer;

#define m_assert(cond, ...)                                                    \
   if (!(cond)) {                                                              \
      printf("ASSERTION FAILURE [%s : %d] ", __FILE__, __LINE__);              \
      printf(__VA_ARGS__);                                                     \
      assert(false);                                                           \
   }

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Parameters
/******************************************/
extern uint32_t g_abort_penalty_us;
extern uint32_t g_num_wh;
extern double   g_perc_payment;
extern bool     g_wh_update;
extern uint32_t g_max_items;
extern uint32_t g_cust_per_dist;
extern uint32_t g_max_txn_retry;
extern bool     g_use_upserts;
extern uint32_t g_total_num_transactions;

uint64_t
Rand(uint64_t max, uint64_t thd_id);
uint64_t
NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id);
uint64_t
URand(uint64_t x, uint64_t y, uint64_t thd_id);
uint64_t
Lastname(uint64_t num, char *name);
uint64_t
MakeAlphaString(int min, int max, char *str, uint64_t thd_id);
uint64_t
MakeNumberString(int min, int max, char *str, uint64_t thd_id);

typedef struct {
   uint64_t table;
   uint64_t key1;
   uint64_t key2; // we need exactly 24 bytes keys to work with Iceberg
                  // and Splinter's compare-key function
} TPCCKey;

// TPCC tables
enum {
   WAREHOUSE,
   ITEM,
   CUSTOMER,
   STOCK,
   NEW_ORDER,
   ORDER,
   ORDER_LINE,
   HISTORY,
   DISTRICT
};

TPCCKey
wKey(uint64_t w_id);
TPCCKey
iKey(uint64_t i_id);
TPCCKey
dKey(uint64_t d_id, uint64_t d_w_id);
TPCCKey
cKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);
void
cKeyToParams(const TPCCKey &k,
             uint64_t      *c_id,
             uint64_t      *c_d_id,
             uint64_t      *c_w_id);
TPCCKey
olKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t ol_number);
TPCCKey
oKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
TPCCKey
noKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
TPCCKey
sKey(uint64_t s_i_id, uint64_t s_w_id);
TPCCKey
hKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);


} // namespace tpcc

#endif // _TPCC_GLOBAL_H_
