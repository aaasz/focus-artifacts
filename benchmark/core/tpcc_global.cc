#include "tpcc_global.h"

namespace tpcc {

drand48_data *tpcc_buffer;

uint32_t g_abort_penalty_us       = ABORT_PENALTY_US;
uint32_t g_num_wh                 = NUM_WH;
double   g_perc_payment           = PERC_PAYMENT;
bool     g_wh_update              = WH_UPDATE;
uint32_t g_max_items              = MAX_ITEMS;
uint32_t g_cust_per_dist          = MAX_CUST_PER_DIST;
uint32_t g_max_txn_retry          = MAX_TXN_RETRY;
bool     g_use_upserts            = false;
uint32_t g_total_num_transactions = TOTAL_NUM_TRANSACTIONS;

/**********************************************/
// helper functions
/**********************************************/

TPCCKey
wKey(uint64_t w_id)
{
   return TPCCKey{.table = WAREHOUSE, .key1 = w_id, .key2 = 0};
}

TPCCKey
iKey(uint64_t i_id)
{
   return TPCCKey{.table = ITEM, .key1 = i_id, .key2 = 0};
}

TPCCKey
dKey(uint64_t d_id, uint64_t d_w_id)
{
   return TPCCKey{
      .table = DISTRICT, .key1 = d_w_id * DIST_PER_WARE + d_id, .key2 = 0};
}

TPCCKey
cKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id)
{
   return TPCCKey{.table = CUSTOMER,
                  .key1 =
                     (c_w_id * DIST_PER_WARE + c_d_id) * g_cust_per_dist + c_id,
                  .key2 = 0};
}

void
cKeyToParams(const TPCCKey &k,
             uint64_t      *c_id,
             uint64_t      *c_d_id,
             uint64_t      *c_w_id)
{
   uint64_t key = k.key1;
   *c_id        = key % g_cust_per_dist;
   key /= g_cust_per_dist;
   *c_d_id = key % DIST_PER_WARE;
   *c_w_id = key / DIST_PER_WARE;
}


TPCCKey
oKey(uint64_t w_id, uint64_t d_id, uint64_t o_id)
{
   return TPCCKey{
      .table = ORDER, .key1 = w_id * DIST_PER_WARE + d_id, .key2 = o_id};
}

TPCCKey
olKey(uint64_t w_id, uint64_t d_id, uint64_t o_id, uint64_t ol_number)
{
   return TPCCKey{.table = ORDER_LINE,
                  .key1  = w_id * DIST_PER_WARE + d_id,
                  .key2  = o_id * MAX_OL_PER_ORDER + ol_number};
}

TPCCKey
noKey(uint64_t w_id, uint64_t d_id, uint64_t o_id)
{
   return TPCCKey{
      .table = NEW_ORDER, .key1 = w_id * DIST_PER_WARE + d_id, .key2 = o_id};
}

TPCCKey
sKey(uint64_t s_i_id, uint64_t s_w_id)
{
   return TPCCKey{
      .table = STOCK, .key1 = s_w_id * g_max_items + s_i_id, .key2 = 0};
}

// HISTORY table does not really have a primary (unique) key, but we will use
// the same one as for the customer
TPCCKey
hKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id)
{
   return TPCCKey{.table = HISTORY,
                  .key1 =
                     (c_w_id * DIST_PER_WARE + c_d_id) * g_cust_per_dist + c_id,
                  .key2 = 0};
}

uint64_t
Lastname(uint64_t num, char *name)
{
   static const char *n[] = {"BAR",
                             "OUGHT",
                             "ABLE",
                             "PRI",
                             "PRES",
                             "ESE",
                             "ANTI",
                             "CALLY",
                             "ATION",
                             "EING"};
   strcpy(name, n[num / 100]);
   strcat(name, n[(num / 10) % 10]);
   strcat(name, n[num % 10]);
   return strlen(name);
}

uint64_t
Rand(uint64_t max, uint64_t thd_id)
{
   int64_t rint64 = 0;
   lrand48_r(&tpcc_buffer[thd_id], &rint64);
   return rint64 % max;
}

uint64_t
URand(uint64_t x, uint64_t y, uint64_t thd_id)
{
   return x + Rand(y - x + 1, thd_id);
}

uint64_t
NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id)
{
   static bool     C_255_init  = false;
   static bool     C_1023_init = false;
   static bool     C_8191_init = false;
   static uint64_t C_255, C_1023, C_8191;
   int             C = 0;
   switch (A) {
      case 255:
         if (!C_255_init) {
            C_255      = (uint64_t)URand(0, 255, thd_id);
            C_255_init = true;
         }
         C = C_255;
         break;
      case 1023:
         if (!C_1023_init) {
            C_1023      = (uint64_t)URand(0, 1023, thd_id);
            C_1023_init = true;
         }
         C = C_1023;
         break;
      case 8191:
         if (!C_8191_init) {
            C_8191      = (uint64_t)URand(0, 8191, thd_id);
            C_8191_init = true;
         }
         C = C_8191;
         break;
      default:
         m_assert(false, "Error! NURand\n");
         exit(-1);
   }
   return (((URand(0, A, thd_id) | URand(x, y, thd_id)) + C) % (y - x + 1)) + x;
}

uint64_t
MakeAlphaString(int min, int max, char *str, uint64_t thd_id)
{
   char char_list[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
                       'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                       'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                       'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
                       'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                       'U', 'V', 'W', 'X', 'Y', 'Z'};
   uint64_t cnt     = URand(min, max, thd_id);
   for (uint32_t i = 0; i < cnt; i++)
      str[i] = char_list[URand(0L, 60L, thd_id)];
   for (int i = cnt; i < max; i++)
      str[i] = '\0';

   return cnt;
}

uint64_t
MakeNumberString(int min, int max, char *str, uint64_t thd_id)
{
   uint64_t cnt = URand(min, max, thd_id);
   for (uint32_t i = 0; i < cnt; i++) {
      uint64_t r = URand(0L, 9L, thd_id);
      str[i]     = '0' + r;
   }
   return cnt;
}

} // namespace tpcc
