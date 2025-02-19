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
#include "rocksdb_merge_operator.h"

namespace baikaldb {
bool OLAPMergeOperator::FullMergeV2(const rocksdb::MergeOperator::MergeOperationInput& merge_in,
                   rocksdb::MergeOperator::MergeOperationOutput* merge_out) const {
    // 扫表或compaction时同一个线程大量处理同一个table的merge，可以thread local将table信息缓存 OLAPTODO
    auto factory = SchemaFactory::get_instance();
    TableKey table_key(merge_in.key);
    int64_t region_id = table_key.extract_i64(0);
    int64_t table_id = table_key.extract_i64(sizeof(int64_t));
    ScopeGuard auto_decrease([this, table_id, region_id, &merge_in, merge_out]() {
        merge_out->existing_operand = merge_in.existing_value == nullptr ? merge_in.operand_list[0] : *merge_in.existing_value;
    });

    // TODO table_info会在thread_local短暂cache，如果加列有概率会造成新列短暂无法识别
    static thread_local SmartTable table_info = nullptr;
    static thread_local TimeCost table_cache_time; 
    static TimeCost print_log_time;
    static bool first_print_log = true; //确保启动时第一次能打印日志
    if (table_info == nullptr || table_info->id != table_id || table_cache_time.get_time() > 60 * 1000 * 1000LL) {
        auto info = factory->get_table_info_ptr(table_id);
        if (info == nullptr) {
            info = factory->get_table_info_ptr_by_index(table_id);
            if (info == nullptr) {
                if (print_log_time.get_time() > 10 * 1000 * 1000LL || first_print_log) {
                    print_log_time.reset();
                    first_print_log = false;
                    DB_FATAL("table_id: %ld, region_id: %ld, get table failed", table_id, region_id);
                }
                return true;
            }
        }
        table_cache_time.reset();
        table_info = info;
    }
    bool is_rollup_key = false;
    if (table_info->id != table_id) {
        is_rollup_key = true;
    }

    int begin_idx = 0;
    SmartRecord base_record = factory->new_record(*table_info); 
    if (merge_in.existing_value != nullptr) {
        base_record->decode(merge_in.existing_value->data(), merge_in.existing_value->size());
        // DB_WARNING("table_id: %ld, region_id: %ld, existing_value, record: %s", table_id, region_id, 
        //     base_record->debug_string().c_str());
    } else {
        base_record->decode(merge_in.operand_list[0].data(), merge_in.operand_list[0].size());
        // DB_WARNING("table_id: %ld, region_id: %ld, operand_list existing_value, record: %s", table_id, region_id, 
        //     base_record->debug_string().c_str());
        begin_idx = 1;
    }

    ExprValue base_version;
    if (table_info->has_version && !is_rollup_key) {
        auto base_version_desc = base_record->get_field_by_idx(table_info->version_field.pb_idx);
        if (base_version_desc == nullptr) {
            DB_FATAL("table_id: %ld, region_id: %ld, get version desc failed", table_id, region_id);
            return true;
        }

        base_version = base_record->get_value(base_version_desc);
        if (base_version.is_null()) {
            DB_FATAL("table_id: %ld, region_id: %ld, version is null", table_id, region_id);
            return true;
        }
    }

    for (int i = begin_idx; i < merge_in.operand_list.size(); i++) {
        SmartRecord record = factory->new_record(*table_info);
        record->decode(merge_in.operand_list[i].data(), merge_in.operand_list[i].size());
    
        if (table_info->has_version && !is_rollup_key) {
            auto field = record->get_field_by_idx(table_info->version_field.pb_idx);
            if (field == nullptr) {
                DB_FATAL("table_id: %ld, region_id: %ld, get version failed", table_id, region_id);
                return true;
            }
            ExprValue version = record->get_value(field);
            if (version.is_null()) {
                DB_FATAL("table_id: %ld, region_id: %ld, version is null", table_id, region_id);
                return true;
            }

            // version和base_version都为0，watt_stats新幂等机制，需要sum
            // rollup导入时version不会为0
            bool all_zero = version.get_numberic<uint64_t>() == 0 && base_version.get_numberic<uint64_t>() == 0;
            if (base_version.compare(version) >= 0 && !all_zero) {
                DB_DEBUG("table_id: %ld, region_id: %ld, base_record: %s, record: %s", table_id, region_id, base_record->debug_string().c_str(), record->debug_string().c_str());
                continue;
            }

            auto base_version_desc = base_record->get_field_by_idx(table_info->version_field.pb_idx);
            if (base_version_desc == nullptr) {
                DB_FATAL("table_id: %ld, region_id: %ld, get version field failed", table_id, region_id);
                return true;
            }
            int ret = base_record->set_value(base_version_desc, version);
            if (ret < 0) {
                DB_FATAL("table_id: %ld, region_id: %ld, set version failed", table_id, region_id);
                return true;
            }

            base_version = version;
        }

        for (const FieldInfo& f : table_info->fields_need_sum) {
            auto field = record->get_field_by_idx(f.pb_idx);
            if (field == nullptr) {
                if (print_log_time.get_time() > 10 * 1000 * 1000LL) {
                    print_log_time.reset();
                    DB_WARNING("table_id: %ld, region_id: %ld, get field: %d failed", table_id, region_id, f.id);
                }
                continue;
            }

            ExprValue value = record->get_value(field);
            if (value.is_null()) {
                if (print_log_time.get_time() > 10 * 1000 * 1000LL) {
                    print_log_time.reset();
                    DB_WARNING("table_id: %ld, region_id: %ld, field: %d is null", table_id, region_id, f.id);
                }
                continue;
            }

            auto base_field = base_record->get_field_by_idx(f.pb_idx);
            if (base_field == nullptr) {
                if (print_log_time.get_time() > 10 * 1000 * 1000LL) {
                    print_log_time.reset();
                    DB_WARNING("table_id: %ld, region_id: %ld, get field: %d failed", table_id, region_id, f.id);
                }
                continue;
            }
            base_record->add_value(base_field, value);
        }
    }
    
    auto_decrease.release();
    base_record->encode(merge_out->new_value);
    return true;
}

// bool OLAPMergeOperator::PartialMerge(const rocksdb::Slice& key, const rocksdb::Slice& left_operand,
//                     const rocksdb::Slice& right_operand, std::string* new_value,
//                     rocksdb::Logger* /*logger*/) const {
//     TableKey table_key(key);
//     int64_t region_id = table_key.extract_i64(0);
//     int64_t table_id = table_key.extract_i64(sizeof(int64_t));
//     SmartRecord left_record = SchemaFactory::get_instance()->new_record(table_id);
//     SmartRecord right_record = SchemaFactory::get_instance()->new_record(table_id);
//     left_record->decode(left_operand.data(), left_operand.size());
//     right_record->decode(right_operand.data(), right_operand.size());
//     DB_WARNING("table_id: %ld, region_id: %ld, left_record: %s, right_operand: %s", table_id, region_id, 
//         left_record->debug_string().c_str(), right_record->debug_string().c_str());
//     return false;
// }

}  // namespace baikaldb