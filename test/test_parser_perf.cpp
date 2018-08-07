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

#include "common.h"
#include "parser.h"
#include <baidu/rpc/server.h>

void test_new_parser(std::vector<char*>& sqls, int num_thread, int loop) {
    baikaldb::TimeCost cost;
    baikaldb::BthreadCond cond;
    for (int i = 0; i < num_thread; ++i) {
        auto parse_sqls = [&sqls, &cond, i, loop] () {
            for (int j = 0; j < loop; ++j) {
                for (int num_sql = 0; num_sql < sqls.size(); ++num_sql) {
                    parser::SqlParser parser;
                    std::string sql(sqls[num_sql]);
                    parser.parse(sql);
                    if (parser.error != parser::SUCC) {
                        DB_WARNING("new parse failed: thread: %d, loop: %d, sql: %d, %s", 
                            i, j, num_sql, parser.syntax_err_str.c_str());
                        continue;
                    }
                    //parser::StmtNode* stmt = (parser::StmtNode*)parser.result[0];
                    //printf("sql: %s\n", stmt->to_string().c_str());
                }
                //DB_WARNING("new parse finished: thread: %d, loop: %d", i, j);
            }
            cond.decrease_signal();
            return;
        };
        cond.increase();
        baikaldb::Bthread bth;
        bth.run(parse_sqls);
    }
    cond.wait();
    DB_WARNING("new parser cost: %ld", cost.get_time());
}

int main(int argc, char** argv) {
    baidu::rpc::StartDummyServerAt(8888/*port*/);
    if (argc != 5) {
        DB_WARNING("usage: file num_thread num_loop old_or_new");
        exit(1);
    }
    baidu::rpc::StartDummyServerAt(8888/*port*/);
    FILE* my_fd = fopen(argv[1], "rb");
    if (my_fd == NULL) {
        printf("file %s does not exsit\n", argv[1]);
        exit(1);
    }
    int thread = atoi(argv[2]);
    int loop = atoi(argv[3]);
    int old_or_new = atoi(argv[4]);

    if (thread >= 10) {
        if (0 != bthread_setconcurrency(thread)) {
            DB_WARNING("set bthread concurrency failed");
            fclose(my_fd);
            exit(1);
        }
    }

    std::vector<char*> sqls;
    while (!feof(my_fd)) {
        char* sql = new char[4096];
        if (fgets(sql, 4096, my_fd) == NULL) {
        	delete []sql;
            continue;
        }
        while (true) {
	        int len = strlen(sql);
	        if (sql[len - 1] == '\n' || sql[len - 1] == '\r' || sql[len - 1] == ';') {
	        	sql[len - 1] = '\0';
	        } else {
	        	break;
	        }
        }
        //DB_WARNING("sql is: %s", sql);
        sqls.push_back(sql);
    }

    test_new_parser(sqls, thread, loop);

    for (auto sql : sqls) {
        delete[] sql;
    }
    fclose(my_fd);
    return 0;
}
