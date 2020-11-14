
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

#include <gflags/gflags.h>

namespace baikaldb{
DEFINE_int64(store_heart_beat_interval_us, 30 * 1000 * 1000, "store heart interval (30 s)");
DEFINE_int32(balance_periodicity, 60, "times of store heart beat"); 
DEFINE_int32(region_faulty_interval_times, 3, "region faulty interval times of heart beat interval");
DEFINE_int32(store_faulty_interval_times, 3, "store faulty interval times of heart beat");
DEFINE_int32(store_dead_interval_times, 60, "store dead interval times of heart beat");
DEFINE_int32(healthy_check_interval_times, 1, "meta state machine healthy check interval times of heart beat");
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
