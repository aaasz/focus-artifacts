#ifndef _TPCC_CONFIG_H_
#define _TPCC_CONFIG_H_

#define ABORT_PENALTY_US 0
#define MAX_TXN_RETRY    0

// # of transactions to run for warmup
#define PERC_PAYMENT            0.5
#define FIRSTNAME_MINLEN        8
#define FIRSTNAME_LEN           16
#define LASTNAME_LEN            16
#define DIST_PER_WARE           10
#define MAX_ORDERS_PER_DISTRICT 2000000
#define MAX_OL_PER_ORDER        15
#define MAX_ITEMS               100000
#define MAX_CUST_PER_DIST       3000
#define WH_UPDATE               true
#define NUM_WH                  32
#define TOTAL_NUM_TRANSACTIONS  10000

#endif // _TPCC_CONFIG_H
