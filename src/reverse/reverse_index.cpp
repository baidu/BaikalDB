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

#include "reverse_index.h"

namespace baikaldb {
int MutilReverseIndex::search(
                       myrocksdb::Transaction* txn,
                       SmartIndex& index_info,
                       SmartTable& table_info,
                       const std::vector<ReverseIndexBase*>& reverse_indexes,
                       const std::vector<std::string>& search_datas,
                       const std::vector<pb::MatchMode>& modes,
                       bool is_fast, bool bool_or) {
    uint32_t son_size = reverse_indexes.size();
    if (son_size == 0) {
        _exe = nullptr;
        return 0;
    }
    _reverse_indexes = reverse_indexes;
    _index_info = index_info;
    _table_info = table_info;
    _weight_field = get_field_info_by_name(_table_info->fields, "__weight");
    _query_words_field = get_field_info_by_name(_table_info->fields, "__querywords");
    bool_executor_type type = NODE_COPY;
    _son_exe_vec.resize(son_size);
    bool type_init = false;
    for (uint32_t i = 0; i < son_size; ++i) {
        reverse_indexes[i]->create_executor(txn, index_info, table_info, search_datas[i], modes[i],
            std::vector<ExprNode*>(), is_fast);
        _query_words += reverse_indexes[i]->get_query_words();
        _query_words += ";";
        _son_exe_vec[i] = reverse_indexes[i]->get_executor();
        if (!type_init && _son_exe_vec[i] != nullptr) {
            type = ((BooleanExecutor*)_son_exe_vec[i])->get_type();
            type_init = true;
        }
        reverse_indexes[i]->print_reverse_statistic_log();
    }
    if (bool_or) {
        _exe = new OrBooleanExecutor(type, nullptr);
        _exe->set_merge_func(merge_or);
        for (uint32_t i = 0; i < son_size; ++i) {
            if (_son_exe_vec[i] != nullptr) {
                _exe->add((BooleanExecutor*)_son_exe_vec[i]);
            }
        }
    } else {
        _exe = new AndBooleanExecutor(type, nullptr);
        _exe->set_merge_func(merge_or);
        for (uint32_t i = 0; i < son_size; ++i) {
            if (_son_exe_vec[i] != nullptr) {
                _exe->add((BooleanExecutor*)_son_exe_vec[i]);
            }
        }
    }
    if (_query_words.size() > 0 && _query_words.back() == ';') {
        _query_words.pop_back();
    }
    return 0;
}

int MutilReverseIndex::search(
    myrocksdb::Transaction* txn,
    SmartIndex& index_info,
    SmartTable& table_info,
    std::map<int64_t, ReverseIndexBase*>& reverse_index_map,
    bool is_fast, const pb::FulltextIndex& fulltext_index_info) {

    _index_info = index_info;
    _table_info = table_info;
    _txn = txn;
    _is_fast = is_fast;
    _weight_field = get_field_info_by_name(_table_info->fields, "__weight");
    _query_words_field = get_field_info_by_name(_table_info->fields, "__querywords");
    _reverse_index_map = reverse_index_map;
    _reverse_indexes.reserve(5);
    init_operator_executor(fulltext_index_info, _exe);
    if (_query_words.size() > 0 && _query_words.back() == ';') {
        _query_words.pop_back();
    }
    return 0;
}

int MutilReverseIndex::init_operator_executor(
    const pb::FulltextIndex& fulltext_index_info, OperatorBooleanExecutor*& exe) {

    if (fulltext_index_info.fulltext_node_type() == pb::FNT_AND) {
        exe = new AndBooleanExecutor(_type, nullptr);
        exe->set_merge_func(merge_or);
        for (const auto& child : fulltext_index_info.nested_fulltext_indexes()) {
            if (child.fulltext_node_type() == pb::FNT_AND || child.fulltext_node_type() == pb::FNT_OR) {
                OperatorBooleanExecutor* child_exe = nullptr;
                if (init_operator_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            } else {
                BooleanExecutor* child_exe = nullptr;
                if (init_term_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            }
        }
    } else if (fulltext_index_info.fulltext_node_type() == pb::FNT_OR) {
        exe = new OrBooleanExecutor(_type, nullptr);
        exe->set_merge_func(merge_or);
        for (const auto& child : fulltext_index_info.nested_fulltext_indexes()) {

            if (child.fulltext_node_type() == pb::FNT_AND || child.fulltext_node_type() == pb::FNT_OR) {
                OperatorBooleanExecutor* child_exe = nullptr;
                if (init_operator_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            } else {
                BooleanExecutor* child_exe = nullptr;
                if (init_term_executor(child, child_exe) == 0 && child_exe != nullptr) {
                    exe->add(child_exe);
                }
            }
        }
    } else {
        DB_WARNING("unknown node type[%s].", fulltext_index_info.ShortDebugString().c_str());
    }
    return 0;
}

int MutilReverseIndex::init_term_executor(
    const pb::FulltextIndex& fulltext_index_info, BooleanExecutor*& exe) {

    auto index_id = fulltext_index_info.possible_index().index_id();
    auto reverse_iter = static_cast<ReverseIndexBase*>(_reverse_index_map[index_id]);
    //析构用，可以重复。
    _reverse_indexes.emplace_back(reverse_iter);
    std::string word;
    auto& range = fulltext_index_info.possible_index().ranges(0);
    if (range.has_left_key()) {
        word = range.left_key();
    } else {
        SmartRecord record = SchemaFactory::get_instance()->new_record(_table_info->id);
        record->decode(range.left_pb_record());
        auto index_info = SchemaFactory::get_instance()->get_index_info_ptr(index_id);
        if (index_info == nullptr || index_info->id == -1) {
            DB_WARNING("no index_info found for index id: %ld", index_id);
            return -1;
        }
        int ret = record->get_reverse_word(*index_info, word);
        if (ret < 0) {
            DB_WARNING("index_info to word fail for index_id: %ld", index_id);
            return ret;
        }
    }
    reverse_iter->create_executor(_txn, _index_info, _table_info, word,
        fulltext_index_info.possible_index().ranges(0).match_mode(),
        std::vector<ExprNode*>(), _is_fast);

    _query_words += reverse_iter->get_query_words();
    _query_words += ";";
    exe = static_cast<BooleanExecutor*>(reverse_iter->get_executor());
    reverse_iter->print_reverse_statistic_log();
    return 0;
}
}

