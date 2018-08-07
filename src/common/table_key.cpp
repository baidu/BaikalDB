// Copyright (c) 2018 Baidu, Inc. All Rights Reserved.
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

#include "table_key.h"
#include "mut_table_key.h"
#include "table_record.h"

namespace baikaldb {

TableKey::TableKey(const MutTableKey& key) : 
        _full(key.get_full()),
        _data(key.data()) {}

int TableKey::extract_index(IndexInfo& index, TableRecord* record, int& pos) {
    return record->decode_key(index, *this, pos);
}

} // end of namespace baikaldb
