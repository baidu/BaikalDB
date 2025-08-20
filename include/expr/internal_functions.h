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
ExprValue bit_count(const std::vector<ExprValue>& input);
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
ExprValue json_extract(const std::vector<ExprValue>& input);
ExprValue json_extract1(const std::vector<ExprValue>& input);
ExprValue json_type(const std::vector<ExprValue>& input);
ExprValue json_array(const std::vector<ExprValue>& input);
ExprValue json_object(const std::vector<ExprValue>& input);
ExprValue json_valid(const std::vector<ExprValue>& input);
ExprValue regexp_replace(const std::vector<ExprValue>& input);
ExprValue export_set(const std::vector<ExprValue>& input);
ExprValue to_base64(const std::vector<ExprValue>& input);
ExprValue from_base64(const std::vector<ExprValue>& input);
ExprValue make_set(const std::vector<ExprValue>& input);
ExprValue oct(const std::vector<ExprValue>& input);
ExprValue hex(const std::vector<ExprValue>& input);
ExprValue unhex(const std::vector<ExprValue>& input);
ExprValue bin(const std::vector<ExprValue>& input);
ExprValue space(const std::vector<ExprValue>& input);
ExprValue elt(const std::vector<ExprValue>& input);
ExprValue char_length(const std::vector<ExprValue>& input);
ExprValue format(const std::vector<ExprValue>& input);
ExprValue field(const std::vector<ExprValue>& input);
ExprValue quote(const std::vector<ExprValue>& input);
ExprValue func_char(const std::vector<ExprValue>& input);
ExprValue soundex(const std::vector<ExprValue>& input);
ExprValue split_part(const std::vector<ExprValue>& input);

// datetime functions
ExprValue unix_timestamp(const std::vector<ExprValue>& input);
ExprValue from_unixtime(const std::vector<ExprValue>& input);
ExprValue now(const std::vector<ExprValue>& input);
ExprValue utc_timestamp(const std::vector<ExprValue>& input);
ExprValue utc_date(const std::vector<ExprValue>& input);
ExprValue utc_time(const std::vector<ExprValue>& input);
ExprValue minute(const std::vector<ExprValue>& input);
ExprValue second(const std::vector<ExprValue>& input);
ExprValue microsecond(const std::vector<ExprValue>& input);
ExprValue func_time(const std::vector<ExprValue>& input);
ExprValue func_quarter(const std::vector<ExprValue>& input);
ExprValue period_diff(const std::vector<ExprValue>& input);
ExprValue period_add(const std::vector<ExprValue>& input);
ExprValue date_format(const std::vector<ExprValue>& input);
ExprValue str_to_date(const std::vector<ExprValue>& input);
ExprValue time_format(const std::vector<ExprValue>& input);
ExprValue convert_tz(const std::vector<ExprValue>& input);
ExprValue timediff(const std::vector<ExprValue>& input);
ExprValue timestampdiff(const std::vector<ExprValue>& input);
ExprValue timestampadd(const std::vector<ExprValue>& input);
ExprValue curdate(const std::vector<ExprValue>& input);
ExprValue current_date(const std::vector<ExprValue>& input);
ExprValue curtime(const std::vector<ExprValue>& input);
ExprValue current_time(const std::vector<ExprValue>& input);
ExprValue current_timestamp(const std::vector<ExprValue>& input);
ExprValue timestamp(const std::vector<ExprValue>& input);
ExprValue date(const std::vector<ExprValue>& input);
ExprValue hour(const std::vector<ExprValue>& input);
ExprValue day(const std::vector<ExprValue>& input);
ExprValue dayname(const std::vector<ExprValue>& input);
ExprValue dayofweek(const std::vector<ExprValue>& input);
ExprValue dayofmonth(const std::vector<ExprValue>& input);
ExprValue dayofyear(const std::vector<ExprValue>& input);
ExprValue month(const std::vector<ExprValue>& input);
ExprValue monthname(const std::vector<ExprValue>& input);
ExprValue year(const std::vector<ExprValue>& input);
ExprValue yearweek(const std::vector<ExprValue>& input);
ExprValue week(const std::vector<ExprValue>& input);
ExprValue weekofyear(const std::vector<ExprValue>& input);
ExprValue time_to_sec(const std::vector<ExprValue>& input);
ExprValue sec_to_time(const std::vector<ExprValue>& input);
ExprValue datediff(const std::vector<ExprValue>& input);
ExprValue date_add(const std::vector<ExprValue>& input);
ExprValue date_sub(const std::vector<ExprValue>& input);
ExprValue weekday(const std::vector<ExprValue>& input);
ExprValue extract(const std::vector<ExprValue>& input);
ExprValue tso_to_timestamp(const std::vector<ExprValue>& input);
ExprValue timestamp_to_tso(const std::vector<ExprValue>& input);
ExprValue to_days(const std::vector<ExprValue>& input);
ExprValue to_seconds(const std::vector<ExprValue>& input);
ExprValue addtime(const std::vector<ExprValue>& input);
ExprValue subtime(const std::vector<ExprValue>& input);

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
// Encryption and Compression Functions
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
ExprValue find_in_set(const std::vector<ExprValue>& input);
//transfer (latitude A, longitude A), (latitude B, longitude B) to distance of A to B (m)
ExprValue point_distance(const std::vector<ExprValue>& input);
ExprValue cast_to_date(const std::vector<ExprValue>& inpt);
ExprValue cast_to_time(const std::vector<ExprValue>& inpt);
ExprValue cast_to_datetime(const std::vector<ExprValue>& inpt);
ExprValue cast_to_signed(const std::vector<ExprValue>& inpt);
ExprValue cast_to_unsigned(const std::vector<ExprValue>& inpt);
ExprValue cast_to_string(const std::vector<ExprValue>& inpt);
ExprValue cast_to_double(const std::vector<ExprValue>& inpt);
int calc_week(const uint64_t dt, int32_t mode, bool is_yearweek, int& year, int& weeks);

// to_sql
// ~ - !
std::string bit_not(const std::vector<std::string>& input);
std::string uminus(const std::vector<std::string>& input);
// + - * /
std::string add(const std::vector<std::string>& input);
std::string minus(const std::vector<std::string>& input);
std::string multiplies(const std::vector<std::string>& input);
std::string divides(const std::vector<std::string>& input);
// % << >> & | ^ 
std::string mod(const std::vector<std::string>& input);
std::string left_shift(const std::vector<std::string>& input);
std::string right_shift(const std::vector<std::string>& input);
std::string bit_and(const std::vector<std::string>& input);
std::string bit_or(const std::vector<std::string>& input);
std::string bit_xor(const std::vector<std::string>& input);
// == != > >= < <=
std::string eq(const std::vector<std::string>& input);
std::string ne(const std::vector<std::string>& input);
std::string gt(const std::vector<std::string>& input);
std::string ge(const std::vector<std::string>& input);
std::string lt(const std::vector<std::string>& input);
std::string le(const std::vector<std::string>& input);
//number functions
std::string round(const std::vector<std::string>& input);
std::string floor(const std::vector<std::string>& input);
std::string ceil(const std::vector<std::string>& input);
std::string abs(const std::vector<std::string>& input);
std::string sqrt(const std::vector<std::string>& input);
std::string mod(const std::vector<std::string>& input);
std::string rand(const std::vector<std::string>& input);
std::string sign(const std::vector<std::string>& input);
std::string sin(const std::vector<std::string>& input);
std::string asin(const std::vector<std::string>& input);
std::string cos(const std::vector<std::string>& input);
std::string acos(const std::vector<std::string>& input);
std::string tan(const std::vector<std::string>& input);
std::string cot(const std::vector<std::string>& input);
std::string atan(const std::vector<std::string>& input);
std::string ln(const std::vector<std::string>& input);
std::string log(const std::vector<std::string>& input);
std::string pi(const std::vector<std::string>& input);
std::string greatest(const std::vector<std::string>& input);
std::string least(const std::vector<std::string>& input);
std::string pow(const std::vector<std::string>& input);
//string functions
std::string length(const std::vector<std::string>& input);
std::string bit_length(const std::vector<std::string>& input);
std::string lower(const std::vector<std::string>& input);
std::string upper(const std::vector<std::string>& input);
std::string concat(const std::vector<std::string>& input);
std::string substr(const std::vector<std::string>& input);
std::string left(const std::vector<std::string>& input);
std::string right(const std::vector<std::string>& input);
std::string trim(const std::vector<std::string>& input);
std::string ltrim(const std::vector<std::string>& input);
std::string rtrim(const std::vector<std::string>& input);
std::string concat_ws(const std::vector<std::string>& input);
std::string ascii(const std::vector<std::string>& input);
std::string strcmp(const std::vector<std::string>& input);
std::string insert(const std::vector<std::string>& input);
std::string replace(const std::vector<std::string>& input);
std::string repeat(const std::vector<std::string>& input);
std::string reverse(const std::vector<std::string>& input);
std::string locate(const std::vector<std::string>& input);
std::string substring_index(const std::vector<std::string>& input);
std::string lpad(const std::vector<std::string>& input);
std::string rpad(const std::vector<std::string>& input);
std::string instr(const std::vector<std::string>& input);
std::string json_extract(const std::vector<std::string>& input);
std::string regexp_replace(const std::vector<std::string>& input);
std::string export_set(const std::vector<std::string>& input);
std::string make_set(const std::vector<std::string>& input);
std::string oct(const std::vector<std::string>& input);
std::string hex(const std::vector<std::string>& input);
std::string unhex(const std::vector<std::string>& input);
std::string bin(const std::vector<std::string>& input);
std::string space(const std::vector<std::string>& input);
std::string elt(const std::vector<std::string>& input);
std::string char_length(const std::vector<std::string>& input);
std::string format(const std::vector<std::string>& input);
std::string field(const std::vector<std::string>& input);
std::string quote(const std::vector<std::string>& input);
std::string func_char(const std::vector<std::string>& input);
std::string soundex(const std::vector<std::string>& input);
// datetime functions
std::string unix_timestamp(const std::vector<std::string>& input);
std::string from_unixtime(const std::vector<std::string>& input);
std::string now(const std::vector<std::string>& input);
std::string utc_timestamp(const std::vector<std::string>& input);
std::string utc_date(const std::vector<std::string>& input);
std::string utc_time(const std::vector<std::string>& input);
std::string minute(const std::vector<std::string>& input);
std::string second(const std::vector<std::string>& input);
std::string microsecond(const std::vector<std::string>& input);
std::string func_time(const std::vector<std::string>& input);
std::string func_quarter(const std::vector<std::string>& input);
std::string period_diff(const std::vector<std::string>& input);
std::string period_add(const std::vector<std::string>& input);
std::string date_format(const std::vector<std::string>& input);
std::string str_to_date(const std::vector<std::string>& input);
std::string time_format(const std::vector<std::string>& input);
std::string convert_tz(const std::vector<std::string>& input);
std::string timediff(const std::vector<std::string>& input);
std::string timestampdiff(const std::vector<std::string>& input);
std::string timestampadd(const std::vector<std::string>& input);
std::string curdate(const std::vector<std::string>& input);
std::string current_date(const std::vector<std::string>& input);
std::string curtime(const std::vector<std::string>& input);
std::string current_time(const std::vector<std::string>& input);
std::string current_timestamp(const std::vector<std::string>& input);
std::string timestamp(const std::vector<std::string>& input);
std::string date(const std::vector<std::string>& input);
std::string hour(const std::vector<std::string>& input);
std::string day(const std::vector<std::string>& input);
std::string dayname(const std::vector<std::string>& input);
std::string dayofweek(const std::vector<std::string>& input);
std::string dayofmonth(const std::vector<std::string>& input);
std::string dayofyear(const std::vector<std::string>& input);
std::string month(const std::vector<std::string>& input);
std::string monthname(const std::vector<std::string>& input);
std::string year(const std::vector<std::string>& input);
std::string yearweek(const std::vector<std::string>& input);
std::string week(const std::vector<std::string>& input);
std::string weekofyear(const std::vector<std::string>& input);
std::string time_to_sec(const std::vector<std::string>& input);
std::string sec_to_time(const std::vector<std::string>& input);
std::string datediff(const std::vector<std::string>& input);
std::string date_add(const std::vector<std::string>& input);
std::string date_sub(const std::vector<std::string>& input);
std::string weekday(const std::vector<std::string>& input);
std::string extract(const std::vector<std::string>& input);
std::string to_days(const std::vector<std::string>& input);
std::string to_seconds(const std::vector<std::string>& input);
std::string addtime(const std::vector<std::string>& input);
std::string subtime(const std::vector<std::string>& input);
// case when functions
std::string case_when(const std::vector<std::string>& input);
std::string case_expr_when(const std::vector<std::string>& input);
std::string if_(const std::vector<std::string>& input);
std::string ifnull(const std::vector<std::string>& input);
std::string isnull(const std::vector<std::string>& input);
std::string nullif(const std::vector<std::string>& input);
// Encryption and Compression Functions
std::string md5(const std::vector<std::string>& input);
std::string sha1(const std::vector<std::string>& input);
std::string sha(const std::vector<std::string>& input);
std::string to_base64(const std::vector<std::string>& input);
std::string from_base64(const std::vector<std::string>& input);
// other
std::string version(const std::vector<std::string>& input);
// std::string last_insert_id(const std::vector<std::string>& input);
std::string find_in_set(const std::vector<std::string>& input);
// cast函数
std::string cast_to_date(const std::vector<std::string>& input);
std::string cast_to_time(const std::vector<std::string>& input);
std::string cast_to_datetime(const std::vector<std::string>& input);
std::string cast_to_signed(const std::vector<std::string>& input);
std::string cast_to_unsigned(const std::vector<std::string>& input);
std::string cast_to_string(const std::vector<std::string>& input);
std::string cast_to_double(const std::vector<std::string>& input);
// 特殊函数
std::string match_against(const std::vector<std::string>& input);

// BaikalDB支持，Mysql不支持的函数
// std::string lower_gbk(const std::vector<std::string>& input);
// std::string split_part(const std::vector<std::string>& input);
// std::string tso_to_timestamp(const std::vector<std::string>& input);
// std::string timestamp_to_tso(const std::vector<std::string>& input);
// std::string murmur_hash(const std::vector<std::string>& input);
// // hll functions
// std::string hll_add(const std::vector<std::string>& input);
// std::string hll_merge(const std::vector<std::string>& input);
// std::string hll_estimate(const std::vector<std::string>& input);
// std::string hll_init(const std::vector<std::string>& input);
// // Roaring bitmap functions
// std::string rb_build(const std::vector<std::string>& input);
// std::string rb_and(const std::vector<std::string>& input);
// //std::string rb_and_cardinality(const std::vector<std::string>& input);
// std::string rb_or(const std::vector<std::string>& input);
// //std::string rb_or_cardinality(const std::vector<std::string>& input);
// std::string rb_xor(const std::vector<std::string>& input);
// //std::string rb_xor_cardinality(const std::vector<std::string>& input);
// std::string rb_andnot(const std::vector<std::string>& input);
// //std::string rb_andnot_cardinality(const std::vector<std::string>& input);
// std::string rb_cardinality(const std::vector<std::string>& input);
// std::string rb_empty(const std::vector<std::string>& input);
// std::string rb_equals(const std::vector<std::string>& input);
// //std::string rb_not_equals(const std::vector<std::string>& input);
// std::string rb_intersect(const std::vector<std::string>& input);
// std::string rb_contains(const std::vector<std::string>& input);
// std::string rb_contains_range(const std::vector<std::string>& input);
// std::string rb_add(const std::vector<std::string>& input);
// std::string rb_add_range(const std::vector<std::string>& input);
// std::string rb_remove(const std::vector<std::string>& input);
// std::string rb_remove_range(const std::vector<std::string>& input);
// std::string rb_flip(const std::vector<std::string>& input);
// std::string rb_flip_range(const std::vector<std::string>& input);
// std::string rb_minimum(const std::vector<std::string>& input);
// std::string rb_maximum(const std::vector<std::string>& input);
// std::string rb_rank(const std::vector<std::string>& input);
// std::string rb_jaccard_index(const std::vector<std::string>& input);
// std::string tdigest_build(const std::vector<std::string>& input);
// std::string tdigest_add(const std::vector<std::string>& input);
// std::string tdigest_merge(const std::vector<std::string>& input);
// std::string tdigest_total_sum(const std::vector<std::string>& input);
// std::string tdigest_total_count(const std::vector<std::string>& input);
// std::string tdigest_percentile(const std::vector<std::string>& input);
// std::string tdigest_location(const std::vector<std::string>& input);
// // transfer (latitude A, longitude A), (latitude B, longitude B) to distance of A to B (m)
// std::string point_distance(const std::vector<std::string>& input);

}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
