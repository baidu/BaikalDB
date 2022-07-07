// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <vector>
#include "expr_value.h"

namespace baikaldb {
//number functions
ExprValue round(const std::vector<ExprValue>& input);
ExprValue floor(const std::vector<ExprValue>& input);
ExprValue ceil(const std::vector<ExprValue>& input);
ExprValue abs(const std::vector<ExprValue>& input);
ExprValue sqrt(const std::vector<ExprValue>& input);
ExprValue mod(const std::vector<ExprValue>& input);
ExprValue rand(const std::vector<ExprValue>& input);
ExprValue sign(const std::vector<ExprValue>& input);
ExprValue sin(const std::vector<ExprValue>& input);
ExprValue asin(const std::vector<ExprValue>& input);
ExprValue cos(const std::vector<ExprValue>& input);
ExprValue acos(const std::vector<ExprValue>& input);
ExprValue tan(const std::vector<ExprValue>& input);
ExprValue cot(const std::vector<ExprValue>& input);
ExprValue atan(const std::vector<ExprValue>& input);
ExprValue ln(const std::vector<ExprValue>& input);
ExprValue log(const std::vector<ExprValue>& input);
ExprValue pi(const std::vector<ExprValue>& input);
ExprValue greatest(const std::vector<ExprValue>& input);
ExprValue least(const std::vector<ExprValue>& input);
ExprValue pow(const std::vector<ExprValue>& input);
//string functions
ExprValue length(const std::vector<ExprValue>& input);
ExprValue bit_length(const std::vector<ExprValue>& input);
ExprValue lower(const std::vector<ExprValue>& input);
ExprValue lower_gbk(const std::vector<ExprValue>& input);
ExprValue upper(const std::vector<ExprValue>& input);
ExprValue concat(const std::vector<ExprValue>& input);
ExprValue substr(const std::vector<ExprValue>& input);
ExprValue left(const std::vector<ExprValue>& input);
ExprValue right(const std::vector<ExprValue>& input);
ExprValue trim(const std::vector<ExprValue>& input);
ExprValue ltrim(const std::vector<ExprValue>& input);
ExprValue rtrim(const std::vector<ExprValue>& input);
ExprValue concat_ws(const std::vector<ExprValue>& input);
ExprValue ascii(const std::vector<ExprValue>& input);
ExprValue strcmp(const std::vector<ExprValue>& input);
ExprValue insert(const std::vector<ExprValue>& input);
ExprValue replace(const std::vector<ExprValue>& input);
ExprValue repeat(const std::vector<ExprValue>& input);
ExprValue reverse(const std::vector<ExprValue>& input);
ExprValue locate(const std::vector<ExprValue>& input);
ExprValue substring_index(const std::vector<ExprValue>& input);
ExprValue lpad(const std::vector<ExprValue>& input);
ExprValue rpad(const std::vector<ExprValue>& input);
ExprValue instr(const std::vector<ExprValue>& input);

// datetime functions
ExprValue unix_timestamp(const std::vector<ExprValue>& input);
ExprValue from_unixtime(const std::vector<ExprValue>& input);
ExprValue now(const std::vector<ExprValue>& input);
ExprValue utc_timestamp(const std::vector<ExprValue>& input);
ExprValue date_format(const std::vector<ExprValue>& input);
ExprValue str_to_date(const std::vector<ExprValue>& input);
ExprValue time_format(const std::vector<ExprValue>& input);
ExprValue convert_tz(const std::vector<ExprValue>& input);
ExprValue timediff(const std::vector<ExprValue>& input);
ExprValue timestampdiff(const std::vector<ExprValue>& input);
ExprValue curdate(const std::vector<ExprValue>& input);
ExprValue current_date(const std::vector<ExprValue>& input);
ExprValue curtime(const std::vector<ExprValue>& input);
ExprValue current_time(const std::vector<ExprValue>& input);
ExprValue current_timestamp(const std::vector<ExprValue>& input);
ExprValue timestamp(const std::vector<ExprValue>& input);
ExprValue day(const std::vector<ExprValue>& input);
ExprValue dayname(const std::vector<ExprValue>& input);
ExprValue dayofweek(const std::vector<ExprValue>& input);
ExprValue dayofmonth(const std::vector<ExprValue>& input);
ExprValue dayofyear(const std::vector<ExprValue>& input);
ExprValue month(const std::vector<ExprValue>& input);
ExprValue monthname(const std::vector<ExprValue>& input);
ExprValue year(const std::vector<ExprValue>& input);
ExprValue week(const std::vector<ExprValue>& input);
ExprValue time_to_sec(const std::vector<ExprValue>& input);
ExprValue sec_to_time(const std::vector<ExprValue>& input);
ExprValue datediff(const std::vector<ExprValue>& input);
ExprValue date_add(const std::vector<ExprValue>& input);
ExprValue date_sub(const std::vector<ExprValue>& input);
ExprValue weekday(const std::vector<ExprValue>& input);
ExprValue extract(const std::vector<ExprValue>& input);
ExprValue tso_to_timestamp(const std::vector<ExprValue>& input);
ExprValue timestamp_to_tso(const std::vector<ExprValue>& input);
// hll functions
ExprValue hll_add(const std::vector<ExprValue>& input);
ExprValue hll_merge(const std::vector<ExprValue>& input);
ExprValue hll_estimate(const std::vector<ExprValue>& input);
ExprValue hll_init(const std::vector<ExprValue>& input);
// case when functions
ExprValue case_when(const std::vector<ExprValue>& input);
ExprValue case_expr_when(const std::vector<ExprValue>& input);
ExprValue if_(const std::vector<ExprValue>& input);
ExprValue ifnull(const std::vector<ExprValue>& input);
ExprValue isnull(const std::vector<ExprValue>& input);
ExprValue nullif(const std::vector<ExprValue>& input);
// MurmurHash sign
ExprValue murmur_hash(const std::vector<ExprValue>& input);
//  Encryption and Compression Functions
ExprValue md5(const std::vector<ExprValue>& input);
ExprValue sha1(const std::vector<ExprValue>& input);
ExprValue sha(const std::vector<ExprValue>& input);
//  Roaring bitmap functions
ExprValue rb_build(const std::vector<ExprValue>& input);
ExprValue rb_and(const std::vector<ExprValue>& input);
//ExprValue rb_and_cardinality(const std::vector<ExprValue>& input);
ExprValue rb_or(const std::vector<ExprValue>& input);
//ExprValue rb_or_cardinality(const std::vector<ExprValue>& input);
ExprValue rb_xor(const std::vector<ExprValue>& input);
//ExprValue rb_xor_cardinality(const std::vector<ExprValue>& input);
ExprValue rb_andnot(const std::vector<ExprValue>& input);
//ExprValue rb_andnot_cardinality(const std::vector<ExprValue>& input);
ExprValue rb_cardinality(const std::vector<ExprValue>& input);
ExprValue rb_empty(const std::vector<ExprValue>& input);
ExprValue rb_equals(const std::vector<ExprValue>& input);
//ExprValue rb_not_equals(const std::vector<ExprValue>& input);
ExprValue rb_intersect(const std::vector<ExprValue>& input);
ExprValue rb_contains(const std::vector<ExprValue>& input);
ExprValue rb_contains_range(const std::vector<ExprValue>& input);
ExprValue rb_add(const std::vector<ExprValue>& input);
ExprValue rb_add_range(const std::vector<ExprValue>& input);
ExprValue rb_remove(const std::vector<ExprValue>& input);
ExprValue rb_remove_range(const std::vector<ExprValue>& input);
ExprValue rb_flip(const std::vector<ExprValue>& input);
ExprValue rb_flip_range(const std::vector<ExprValue>& input);
ExprValue rb_minimum(const std::vector<ExprValue>& input);
ExprValue rb_maximum(const std::vector<ExprValue>& input);
ExprValue rb_rank(const std::vector<ExprValue>& input);
ExprValue rb_jaccard_index(const std::vector<ExprValue>& input);
ExprValue tdigest_build(const std::vector<ExprValue>& input);
ExprValue tdigest_add(const std::vector<ExprValue>& input);
ExprValue tdigest_merge(const std::vector<ExprValue>& input);
ExprValue tdigest_total_sum(const std::vector<ExprValue>& input);
ExprValue tdigest_total_count(const std::vector<ExprValue>& input);
ExprValue tdigest_percentile(const std::vector<ExprValue>& input);
ExprValue tdigest_location(const std::vector<ExprValue>& input);
// other
ExprValue version(const std::vector<ExprValue>& input);
ExprValue last_insert_id(const std::vector<ExprValue>& input);
//transfer (latitude A, longitude A), (latitude B, longitude B) to distance of A to B (m)
ExprValue point_distance(const std::vector<ExprValue>& input);
ExprValue cast_to_date(const std::vector<ExprValue>& inpt);
ExprValue cast_to_time(const std::vector<ExprValue>& inpt);
ExprValue cast_to_datetime(const std::vector<ExprValue>& inpt);
ExprValue cast_to_signed(const std::vector<ExprValue>& inpt);
ExprValue cast_to_unsigned(const std::vector<ExprValue>& inpt);
ExprValue cast_to_string(const std::vector<ExprValue>& inpt);
ExprValue cast_to_double(const std::vector<ExprValue>& inpt);
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
