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

#include <sys/ioctl.h>
#include <signal.h>
#include <stdio.h>
#include <string>
#include <Configure.h>
#include <baidu/rpc/server.h>
#include <gflags/gflags.h>
#include "common.h"
#include "schema_factory.h"
#include "meta_server_interact.hpp"
#include "mut_table_key.h"

namespace baikaldb {
void create_schema(pb::SchemaInfo& table) 
{
    table.set_table_name("ideacontent");
    table.set_database("FC_Content");
    table.set_namespace_name("FENGCHAO");
    table.set_partition_num(1);
    table.set_resource_tag("adp");
    pb::FieldInfo* field;
    field = table.add_fields();
    field->set_field_name("account_id");
    field->set_mysql_type(pb::UINT32);

    field = table.add_fields();
    field->set_field_name("campaign_id");
    field->set_mysql_type(pb::UINT32);

    field = table.add_fields();
    field->set_field_name("adgroup_id");
    field->set_mysql_type(pb::INT64);

    field = table.add_fields();
    field->set_field_name("creative_id");
    field->set_mysql_type(pb::INT64);

    field = table.add_fields();
    field->set_field_name("magic_id");
    field->set_mysql_type(pb::INT64);

    field = table.add_fields();
    field->set_field_name("new_creative_title");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("new_creative_desc");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("new_creative_desc2");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("new_creative_url");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("new_creative_showurl");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("new_creative_miurl");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("new_creative_mshowurl");
    field->set_mysql_type(pb::STRING);

    field = table.add_fields();
    field->set_field_name("pc_version");
    field->set_mysql_type(pb::INT64);

    field = table.add_fields();
    field->set_field_name("m_version");
    field->set_mysql_type(pb::INT64);

    pb::IndexInfo* pk_idx = table.add_indexs();
    pk_idx->set_index_name("PRIMARY");
    pk_idx->set_index_type(pb::I_PRIMARY);
    pk_idx->add_field_names("account_id");
    pk_idx->add_field_names("campaign_id");
    pk_idx->add_field_names("adgroup_id");
    pk_idx->add_field_names("creative_id");

    pb::IndexInfo* sec_idx = table.add_indexs();
    sec_idx->set_index_name("title_key");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_title");

    sec_idx = table.add_indexs();
    sec_idx->set_index_name("new_creative_desc");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_desc");

    sec_idx = table.add_indexs();
    sec_idx->set_index_name("new_creative_desc2");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_desc2");

    sec_idx = table.add_indexs();
    sec_idx->set_index_name("new_creative_url");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_url");

    sec_idx = table.add_indexs();
    sec_idx->set_index_name("showurl_key");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_showurl");

    sec_idx = table.add_indexs();
    sec_idx->set_index_name("new_creative_miurl");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_miurl");

    sec_idx = table.add_indexs();
    sec_idx->set_index_name("new_creative_mshowurl");
    sec_idx->set_index_type(pb::I_FULLTEXT);
    sec_idx->add_field_names("new_creative_mshowurl");

    {
        MutTableKey key;
        key.append_u32(1381035);
        key.append_u32(72812635);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(1381035);
        key.append_u32(77594010);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(1381056);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(6674494);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(6674494);
        key.append_u32(36228636);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(7303550);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(7545404);
        key.append_u32(21839529);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(9766491);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(64364146);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65141597);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65141633);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65142065);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65142095);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65142140);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65142191);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10090929);
        key.append_u32(65142221);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198437);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198437);
        key.append_u32(27441822);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198437);
        key.append_u32(28131202);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198437);
        key.append_u32(30105170);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198467);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198467);
        key.append_u32(27184363);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198467);
        key.append_u32(28314234);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198467);
        key.append_u32(29896302);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10198467);
        key.append_u32(69220617);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10268850);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10276170);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10315069);
        key.append_u32(35181869);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10315069);
        key.append_u32(67072831);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10475894);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(36911836);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37200090);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37200108);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37200123);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37200138);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37200153);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37593156);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37606464);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(37819832);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(57944788);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(58253000);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(58253015);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(58253027);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(58253048);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(60494170);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(62154598);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(62154613);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(62763912);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(62763927);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(65051112);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(65051127);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(65051142);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826177);
        key.append_u32(65051154);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(37199311);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(37281995);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(37282013);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(37282028);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(37282043);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(37895946);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(38092400);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(38154476);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(38154491);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(48648315);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(48648330);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(57402751);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(57402766);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(57402784);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(61308689);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(62905661);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(62905682);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(62905703);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(64438192);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(64438207);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(10826178);
        key.append_u32(65044203);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(11200546);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(70549489);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(70550470);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(70550662);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(71306345);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(71306975);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(71368797);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17932582);
        key.append_u32(71369955);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(17967337);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(20304047);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(20877715);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(21256248);
        key.append_u32(72976923);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(22670178);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(23025284);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(23828194);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(24003116);
        key.append_u32(74512207);
        table.add_split_keys(key.data());
    }
    {
        MutTableKey key;
        key.append_u32(24733604);
        table.add_split_keys(key.data());
    }

}

int create_table() {
    MetaServerInteract interact;
    if (interact.init() != 0) {
        DB_WARNING("init fail");
        return -1;
    }
    pb::MetaManagerRequest request;
    request.set_op_type(pb::OP_CREATE_TABLE);
    create_schema(*request.mutable_table_info());
    pb::MetaManagerResponse response;
    if (interact.send_request("meta_manager", request, response) != 0) {
        DB_WARNING("send_request fail");
        return -1;
    }
    DB_WARNING("req:%s  \nres:%s", request.DebugString().c_str(), response.DebugString().c_str());
    if (response.errcode() != pb::SUCCESS) {
        DB_WARNING("err:%s", response.errmsg().c_str());
        return -1;
    }
    return 0;
}

} // namespace baikaldb

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    baikaldb::create_table();
    sleep(30);

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
