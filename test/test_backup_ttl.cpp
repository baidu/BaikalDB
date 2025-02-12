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

#include <gtest/gtest.h>
#include <climits>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include "backup_tool.h"

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace baikaldb {
void check_file_delete(std::vector<RegionFile>& region_file, const std::unordered_set<std::string>& deleted_files) {
    for (auto& f : region_file) {
        if (deleted_files.count(f.filename) == 0) {
            EXPECT_EQ(f.deleted, false);
        } else {
            EXPECT_EQ(f.deleted, true);
        }
    }
}
void check_active_file_size(std::vector<RegionFile>& region_file, const int fsize) {
    int actual_size = 0;
    for (auto& f : region_file) {
        if (!f.deleted) {
            ++actual_size;
        }
    }
    EXPECT_EQ(actual_size, fsize);
}
void generate_sst_info(std::vector<RegionFile>& region_file_vec, const std::unordered_set<std::string>& sst_files) {
    DB_NOTICE("---------------------------------------");
    for (auto sst : sst_files) {
        region_file_vec.emplace_back(RegionFile{"100", sst, 0, sst});
    }
    std::sort(region_file_vec.begin(), region_file_vec.end(), [](const RegionFile& l, const RegionFile& r){
        if (l.date != r.date) {
            return l.date > r.date;
        } else {
            return l.log_index > r.log_index;
        }
    });
}

TEST(backup_ttl, one_level_backup_ttl) {
    baikaldb::BackUp bp("127.0.0.1:8110", "e0", 1, 3, 0, 0);
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230102"});
        bp.ttl_backup_ssts("20230204",region_files);
        check_file_delete(region_files, {});
        check_active_file_size(region_files, 2);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230102", "20230103"});
        bp.ttl_backup_ssts("20230104", region_files);
        check_file_delete(region_files, {"20230101"});
        check_active_file_size(region_files, 2);
    }
}

TEST(backup_ttl, two_level_backup_ttl_weekly) {
    baikaldb::BackUp bp("127.0.0.1:8110", "e0", 1, 3, 7, 4);
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230102"});
        bp.ttl_backup_ssts("20230104", region_files);
        check_file_delete(region_files, {});
        check_active_file_size(region_files, 2);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230102", "20230103"});
        bp.ttl_backup_ssts("20230104", region_files);
        check_file_delete(region_files, {});
        check_active_file_size(region_files, 3);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230102", "20230103", "20230104"});
        bp.ttl_backup_ssts("20230105", region_files);
        check_file_delete(region_files, {"20230102"});
        check_active_file_size(region_files, 3);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230102", "20230103", "20230104", "20230105"});
        bp.ttl_backup_ssts("20230106", region_files);
        check_file_delete(region_files, {"20230102","20230103"});
        check_active_file_size(region_files, 3);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230108", "20230115", "20230120", "20230121", "20230122"});
        bp.ttl_backup_ssts("20230123", region_files);
        check_file_delete(region_files, {"20230120"});
        check_active_file_size(region_files, 5);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230108", "20230115", "20230122", "20230123", "20230124"});
        bp.ttl_backup_ssts("20230125", region_files);
        check_file_delete(region_files, {});
        check_active_file_size(region_files, 6);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230110", "20230121", "20230122", "20230123"});
        bp.ttl_backup_ssts("20230124", region_files);
        check_file_delete(region_files, {});
        check_active_file_size(region_files, 5);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230110", "20230121", "20230122"});
        region_files.emplace_back(RegionFile{"100", "20230122", 1122, "20230122_2"});
        generate_sst_info(region_files, {"20230123", "20230124"});
        bp.ttl_backup_ssts("20230125", region_files);
        check_file_delete(region_files, {"20230122", "20230122_2"});
        check_active_file_size(region_files, 5);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230101", "20230108", "20230115", "20230122", "20230129", "20230130", "20230131", "20230201"});
        bp.ttl_backup_ssts("20230202", region_files);
        check_file_delete(region_files, {"20230101","20230130"});
        check_active_file_size(region_files, 6);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230115", "20230122", "20230129", "20230205", "20230212", "20230213", "20230214"});
        bp.ttl_backup_ssts("20230215", region_files);
        check_file_delete(region_files, {"20230115"});
        check_active_file_size(region_files, 6);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20231210", "20231217", "20231224", "20231231", "20240101", "20240102", "20240103"});
        bp.ttl_backup_ssts("20240104", region_files);
        check_file_delete(region_files, {"20240101"});
        check_active_file_size(region_files, 6);
    }
    {
        std::vector<RegionFile> region_files;
        generate_sst_info(region_files, {"20230829", "20230905", "20230912", "20230919", "20230924", "20230926", "20230929", "20231008", "20231009", "20231012"});
        bp.ttl_backup_ssts("20231012", region_files);
        check_file_delete(region_files, {"20230829","20230905","20230912","20230924"});
        check_active_file_size(region_files, 6);
    }
}
}  // namespace baikal
