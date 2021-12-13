////////////////////////////////////////////////////////////////////////////////
// tdigest
//
// Copyright (c) 2018 Andrew Werner, All rights reserved.
//
// tdigest is an implementation of Ted Dunning's streaming quantile estimation
// data structure. 
// This implementation is intended to be like the new MergingHistogram.
// It focuses on being in portable C that should be easy to integrate into other
// languages. In particular it provides mechanisms to preallocate all memory 
// at construction time.
//
// The implementation is a direct descendent of 
//  https://github.com/tdunning/t-digest/
//
// TODO: add a Ted Dunning Copyright notice.
//
////////////////////////////////////////////////////////////////////////////////
#pragma once
#include <stdlib.h>
#include <stdint.h>
#include <string>

namespace baikaldb {
namespace tdigest {

// tdigest精度，越大越精确
static const int COMPRESSION = 100;

typedef struct td_histogram td_histogram_t;
// init tdigest.
td_histogram_t* td_init(double compression, uint8_t *buff, size_t size);
// td_add adds val to h with the specified count.
void td_add(td_histogram_t *h, double val, double count);
// td_merge merges the data from from into into.
void td_merge(td_histogram_t *into, td_histogram_t *from);
// td_reset resets a histogram.
void td_reset(td_histogram_t *h);
// td_value_at queries h for the value at q.
// If q is not in [0, 1], NAN will be returned.
double td_value_at(td_histogram_t *h, double q);
// td_value_at queries h for the quantile of val.
// The returned value will be in [0, 1].
double td_quantile_of(td_histogram_t *h, double val);
// td_trimmed_mean returns the mean of data from the lo quantile to the
// hi quantile.
double td_trimmed_mean(td_histogram_t *h, double lo, double hi);
// td_total_count returns the total count contained in h.
double td_total_count(td_histogram_t *h);
// td_total_sum returns the sum of all the data added to h.
double td_total_sum(td_histogram_t *h);
// td_decay multiplies all countes by factor.
void td_decay(td_histogram_t *h, double factor);
size_t td_required_buf_size(double compression);
size_t td_actual_size(td_histogram_t *h);
void td_set_target_quantile(td_histogram_t *t, double target_quantile);
double td_get_target_quantile(td_histogram_t *t);
bool is_td_object(const std::string& td);
//td序列化，执行merge后只保存需要的nodes(merged_nodes + unmerged_nodes)
void td_serialize(std::string& td);
//td转为内存buf
void td_normallize(std::string& td);

} // namespace tdigest
} // namespace baikaldb
