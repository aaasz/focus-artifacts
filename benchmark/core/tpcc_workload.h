//
//  tpcc_workload.h
//

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <pthread.h>
#include "tpcc_config.h"
#include "tpcc_global.h"
#include "tpcc_client.h"
#include "db.h"
#include "db/transactional_splinter_db.h"
#include "db/splinter_db.h"


#ifndef _TPCC_WORKLOAD_H_
#   define _TPCC_WORKLOAD_H_

namespace tpcc {

enum tpcc_txn_type { TPCC_PAYMENT, TPCC_NEW_ORDER };

typedef struct tpcc_warehouse_row {
   int64_t w_id;
   char    w_name[10];
   char    w_street_1[20];
   char    w_street_2[20];
   char    w_city[20];
   char    w_state[2];
   char    w_zip[9];
   double  w_tax;
   double  w_ytd;
} tpcc_warehouse_row_t;

typedef struct tpcc_district_row {
   int64_t d_id;
   int64_t d_w_id;
   char    d_name[10];
   char    d_street_1[20];
   char    d_street_2[20];
   char    d_city[20];
   char    d_state[2];
   char    d_zip[9];
   double  d_tax;
   double  d_ytd;
   int64_t d_next_o_id;
} tpcc_district_row_t;

typedef struct tpcc_customer_row {
   int64_t  c_id;
   int64_t  c_d_id;
   int64_t  c_w_id;
   char     c_first[16];
   char     c_middle[2];
   char     c_last[16];
   char     c_street_1[20];
   char     c_street_2[20];
   char     c_city[20];
   char     c_state[2];
   char     c_zip[9];
   char     c_phone[16];
   int64_t  c_since;
   char     c_credit[2];
   int64_t  c_credit_lim;
   int64_t  c_discount;
   double   c_balance;
   double   c_ytd_payment;
   uint64_t c_payment_cnt;
   uint64_t c_delivery_cnt;
   char     c_data[500];
} tpcc_customer_row_t;

typedef struct tpcc_history_row {
   int64_t h_c_id;
   int64_t h_c_d_id;
   int64_t h_c_w_id;
   int64_t h_d_id;
   int64_t h_w_id;
   int64_t h_date;
   double  h_amount;
   char    h_data[24];
} tpcc_history_row_t;

typedef struct tpcc_new_order_row {
   int64_t no_o_id;
   int64_t no_d_id;
   int64_t no_w_id;
} tpcc_new_order_row_t;

typedef struct tpcc_order_row {
   int64_t o_id;
   int64_t o_c_id;
   int64_t o_d_id;
   int64_t o_w_id;
   int64_t o_entry_d;
   int64_t o_carrier_id;
   int64_t o_ol_cnt;
   int64_t o_all_local;
} tpcc_order_row_t;

typedef struct tpcc_order_line_row {
   uint64_t ol_o_id;
   uint64_t ol_d_id;
   uint64_t ol_w_id;
   uint64_t ol_number;
   uint64_t ol_i_id;
   uint64_t ol_supply_w_id;
   uint64_t ol_delivery_d;
   uint64_t ol_quantity;
   double   ol_amount;
   char     ol_dist_info[24];
} tpcc_order_line_row_t;

typedef struct tpcc_item_row {
   int64_t i_id;
   int64_t i_im_id;
   char    i_name[24];
   int64_t i_price;
   char    i_data[50];
} tpcc_item_row_t;

typedef struct tpcc_stock_row {
   uint64_t s_i_id;
   uint64_t s_w_id;
   uint64_t s_quantity;
   char     s_dist_01[24];
   char     s_dist_02[24];
   char     s_dist_03[24];
   char     s_dist_04[24];
   char     s_dist_05[24];
   char     s_dist_06[24];
   char     s_dist_07[24];
   char     s_dist_08[24];
   char     s_dist_09[24];
   char     s_dist_10[24];
   int64_t  s_ytd;
   int64_t  s_order_cnt;
   int64_t  s_remote_cnt;
   char     s_data[50];
} tpcc_stock_row_t;

int
tpcc_merge_tuple(data_config const *cfg,
                 slice              key,          // IN
                 message            old_message,  // IN
                 merge_accumulator *new_message); // IN/OUT

int
tpcc_merge_tuple_final(data_config const *cfg,
                       slice              key,
                       merge_accumulator *oldest_message);

/**********************************************/
// generate input for TPCC transactions
/**********************************************/
class TPCCTransaction {
public:
   tpcc_txn_type type;
   void
   init(uint64_t thd_id);

   /**********************************************/
   // common txn input for both payment & new-order
   /**********************************************/
   uint64_t w_id;
   uint64_t d_id;
   uint64_t c_id;
   /**********************************************/
   // txn input for payment
   /**********************************************/
   uint64_t d_w_id;
   uint64_t c_w_id;
   uint64_t c_d_id;
   double   h_amount;
   char     c_last[LASTNAME_LEN];
   bool     by_last_name;

   /**********************************************/
   // txn input for new-order
   /**********************************************/
   // items of new order transaction
   struct item_no {
      uint64_t ol_i_id;
      uint64_t ol_supply_w_id;
      uint64_t ol_quantity;
   };
   item_no  items[MAX_OL_PER_ORDER];
   bool     rbk;
   bool     remote;
   uint64_t ol_cnt;
   uint64_t o_entry_d;
   // Input for delivery
   uint64_t o_carrier_id;
   uint64_t ol_delivery_d;
   // for order-status

private:
   // warehouse id to partition id mapping
   //	uint64_t wh_to_part(uint64_t wid);
   void
   gen_payment(uint64_t thd_id);
   void
   gen_new_order(uint64_t thd_id);
   void
   gen_order_status(uint64_t thd_id);
};

/**********************************************/
// populates TPCC tables and runs TPCC transactions
/**********************************************/
class TPCCWorkload {
public:
   void
   init(utils::Properties              &props,
        ycsbc::TransactionalSplinterDB *db,
        uint64_t                        num_client_threads);
   void
   deinit();

   int
   run_transaction(TPCCTransaction *txn);

private:
   struct thread_args {
      TPCCWorkload *wl;
      uint32_t      tid;
   };
   ycsbc::TransactionalSplinterDB *_db;

   void
   configure_parameters(utils::Properties &props);

   static void *
   thread_init_tables(void *args);
   void
   init_warehouse_table(uint64_t wid);
   void
   init_item_table();
   void
   init_district_table(uint64_t wid);
   void
   init_customer_table(uint64_t did, uint64_t wid);
   void
   init_order_table(uint64_t did, uint64_t wid);
   void
   init_stock_table(uint64_t wid);
   void
   init_permutation(uint64_t *perm_c_id, uint64_t wid);

   int
   run_payment(TPCCTransaction *txn);
   int
   run_new_order(TPCCTransaction *txn);
};

} // namespace tpcc

#endif // _TPCC_WORKLOAD_H_
