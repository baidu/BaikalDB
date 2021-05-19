// 
#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "expr_value.h"
#include "fn_manager.h"
#include "proto/expr.pb.h"
#include "parser.h"
#include "qos.h"
#include <vector>
DEFINE_int64(qos_rate,          100, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_burst,          100000, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_count,          100000, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_bthread_count,          10, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_committed_rate,          100, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_extended_rate,          100, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_globle_rate,          100, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_sum,          60, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_get_value,          60, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(qos_sleep_us,          1000, "max_tokens_per_second, defalut: 10w");
DEFINE_int64(peer_thread_us,        1000*1000, "max_tokens_per_second, defalut: 10w");


namespace baikaldb {

void test_func() {
    bvar::Adder<int64_t> test_sum;
    bvar::PerSecond<bvar::Adder<int64_t> > test_sum_per_second(&test_sum, FLAGS_qos_sum);
    int i = 1;
    int count = 0;
    int count1 = 0;
    TimeCost cost;
    for (;;) {
        test_sum << i;
        count += i;
        if (cost.get_time() > 1000*1000) {
            cost.reset();
            i=i*2;
            
            DB_WARNING("adder:%d, qps:%ld", count-count1, test_sum_per_second.get_value(FLAGS_qos_get_value));
            count1 = count;

        }
        bthread_usleep(100*1000);
    }
}
    


} // namespace baikaldb

using namespace baikaldb;
int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    // baikaldb::TokenBucket token_bucket;
    // token_bucket.reset_rate(FLAGS_qos_rate, FLAGS_qos_burst);
    // baikaldb::TimeCost cost;
    // for (int i = 0; i < FLAGS_qos_count; i++) {
    //     int64_t time;
    //     while( token_bucket.consume(1,time) <= 0){
    //         // DB_WARNING("%d, %ld", i, time);
    //     }
    // }
    baikaldb::StoreQos* store_qos = baikaldb::StoreQos::get_instance();
    int ret = store_qos->init();
    if (ret < 0) {
        DB_FATAL("store qos init fail");
        return -1;
    } 

    baikaldb::TimeCost cost;
    baikaldb::BthreadCond cond;
    for (int i = 0; i < FLAGS_qos_bthread_count; i++) {

        auto calc = [i, &cond]() {
                uint64_t sign = 123;
                if (i % 2 == 0) {
                    sign= 124;
                }

                StoreQos::get_instance()->create_bthread_local(baikaldb::QOS_SELECT,sign);
                
                baikaldb::QosBthreadLocal* local = StoreQos::get_instance()->get_bthread_local();
                DB_WARNING("local:%p", local);
                baikaldb::TimeCost local_time;
                for (;;) {

                    // 限流
                    if (local) {
                        local->scan_rate_limiting();
                    }

                    bthread_usleep(FLAGS_qos_sleep_us);
                    if (local_time.get_time() > FLAGS_peer_thread_us) {
                        break;
                    }
                }
                
       
                DB_WARNING("bthread:%d, sign:%lu. time:%ld", i, sign, local_time.get_time());
                cond.decrease_signal();
            };
            
            cond.increase();
            baikaldb::Bthread bth(&BTHREAD_ATTR_SMALL);
            bth.run(calc);
    }

    cond.wait();
    DB_WARNING("time:%ld", cost.get_time());
    store_qos->close();
    DB_WARNING("store qos close success");

    bthread_usleep(10000000);

    return 0;
}
