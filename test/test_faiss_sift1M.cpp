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
#include <random>
#include <ctime>
#include <vector>
#include <fstream>
#include <gflags/gflags.h>
#include <base/time.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexHNSW.h>
#include <faiss/AutoTune.h>
#include <faiss/index_io.h>
#include <faiss/index_factory.h>
#include "mysql_interact.h"
#include <boost/algorithm/string.hpp>

DEFINE_int32(nb, 100000, "number of base vectors for testing");
DEFINE_int32(nq, 1000, "number of query vectors for testing");
DEFINE_int32(m, 16, "HNSW parameter M");
DEFINE_int32(tp, 0, "thread pool size for parallel processing");
DEFINE_int32(nlist, 0, "number of IVF clusters (0 for auto-calculate)");
DEFINE_bool(read, false, "read existing index from file");
DEFINE_int32(batch, 0, "batch size for vector insertion (0 for single batch)");

DEFINE_bool(test_recall, false, "run recall test scenarios");
DEFINE_int32(d, 384, "vector dimension");
DEFINE_int32(vector_num, 0, "number of vectors (0 for auto-detect)");
DEFINE_int32(recall_s, 10, "sample size for recall calculation");
DEFINE_int32(refine_factor, 5, "refinement factor for RFlat indexes");
DEFINE_int32(ivf_nlist_factor, 1, "IVF nlist scaling factor");
DEFINE_int32(ivf_nprobe_factor, 10, "IVF nprobe scaling factor");
DEFINE_int32(pq_factor, 4, "PQ sub-quantizer factor");
DEFINE_int32(pqfs_factor, 2, "PQFS sub-quantizer factor");
DEFINE_string(target, "", "target index type to test");
DEFINE_int32(search_num, 10, "number of nearest neighbors to search");
DEFINE_int32(query_times, 100, "number of query iterations");
DEFINE_string(dataset, "sift", "dataset name (sift) - http://corpus-texmex.irisa.fr/");
DEFINE_int32(efsearch, 16, "HNSW efSearch parameter");
DEFINE_int32(efconstruction, 40, "HNSW efConstruction parameter");
DEFINE_int32(learn_vec_cnt, 100000, "number of vectors for index training");

DEFINE_bool(insert_to_baikaldb, false, "insert test data to BaikalDB");
DEFINE_int32(insert_row_cnt, 100000, "insert test data to BaikalDB");
DEFINE_bool(query_baikaldb, false, "read test data from BaikalDB");
DEFINE_string(table, "vec.test_ivf", "BaikalDB table name for testing");
DEFINE_int32(sql_print_time_limit, 100000, "SQL execution time limit for printing");
DEFINE_string(order, "asc", "order by use");
DEFINE_string(id2_value, "333", "id2 value");


// UTApplication('test_faiss_sift1m', Sources('test/test_faiss_sift1M.cpp', CxxFlags('-g -DBAIDU_INTERNAL -fno-access-control -Wno-sign-compare -Wno-unused-variable -std=c++17 -fopenmp')),
//         Libraries(
//             '$OUT/lib/libproto.a',
//             '$OUT/lib/libcapture.a',
//             '$OUT/lib/libwatt_proto.a',
//             '$OUT/lib/libcommon.a'))

int cnt = 0;
using idx_t = int64_t;

int main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    //cnt = std::stoi(argv[1]);
    return RUN_ALL_TESTS();
}
class TimeCost {
public:
    TimeCost() {
        _start = base::gettimeofday_us();
    }

    ~TimeCost() {}

    void reset() {
        _start = base::gettimeofday_us();
    }

    int64_t get_time() const {
        return base::gettimeofday_us() - _start;
    }

private:
    int64_t _start;
};
void query_and_calc_recall(faiss::Index* idx, int d, int nb, int nq, int nl,  float* xb, float* xq, float* xl, idx_t* ids, int sn, std::string desc, std::vector<std::set<int>>& flat_res, int ivf_nprobe) {
    faiss::Index& index = *idx;
    bool is_ivf = (desc.find("IVF") != std::string::npos);
    bool is_refine = (desc.find("RFlat") != std::string::npos);
    bool is_hnsw = (desc.find("HNSW") != std::string::npos);
    if (is_ivf) {
        faiss::ParameterSpace().set_index_parameter(idx, "nprobe", ivf_nprobe);
    }
    if (is_refine) {
        faiss::ParameterSpace().set_index_parameter(idx, "k_factor_rf", FLAGS_refine_factor);
    }
    if (is_hnsw) {
        faiss::ParameterSpace().set_index_parameter(idx, "efSearch", FLAGS_efsearch);
        faiss::ParameterSpace().set_index_parameter(idx, "efConstruction", FLAGS_efconstruction);
    }

    int k = sn;
    bool is_flat = flat_res.empty();
    std::vector<std::set<int>> res_vec;
    TimeCost cost;
    { // sanity check: search 5 first vectors of xb
        idx_t* I = new idx_t[k * FLAGS_recall_s];
        float* D = new float[k * FLAGS_recall_s];
        cost.reset();
        index.search(FLAGS_recall_s, xb, k, D, I);
        printf("search %d in %zd, cost=:%ld\n", FLAGS_recall_s, index.ntotal, cost.get_time());
        cost.reset();

        // print results
        printf("I=\n");
        for (int i = 0; i < FLAGS_recall_s; i++) {
            for (int j = 0; j < k; j++) {
                printf("%5zd ", I[i * k + j]);
            }
            printf("\n");
        }

        printf("D=\n");
        for (int i = 0; i < FLAGS_recall_s; i++) {
            for (int j = 0; j < k; j++)
                printf("%7g ", D[i * k + j]);
            printf("\n");
        }

        delete[] I;
        delete[] D;
    }

    { // search xq
        idx_t* I = new idx_t[k * nq];
        float* D = new float[k * nq];
        TimeCost cost2;
        for (int i = 0; i < nq; i++) {
            cost.reset();
            index.search(1, xq + i * d, k, D + i * k, I + i * k);
            //printf("search i = %d, cost=:%ld\n", i, cost.get_time());
        }
        printf("search avg, cost=:%ld\n", cost2.get_time()/nq);

        // print results
        printf("I (5 first results)=\n");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < k; j++)
                printf("%5zd ", I[i * k + j]);
            printf("\n");
        }

        printf("I (5 last results)=\n");
        for (int i = nq - 5; i < nq; i++) {
            for (int j = 0; j < k; j++)
                printf("%5zd ", I[i * k + j]);
            printf("\n");
        }

        bool is_flat_res = (flat_res.empty());
        float recall_rate = 0;
        for (int i = 0; i < nq; i++) {
            std::set<int> one_vec_res;
            for (int j = 0; j < k; j++) {
                one_vec_res.insert(I[i * k + j]);
            }
            if (is_flat_res) {
                flat_res.emplace_back(one_vec_res);
            } else {
                int recall_cnt = 0;
                for (int j = 0; j < k; j++) {
                    if (flat_res[i].count(I[i * k + j]) > 0) {
                        recall_cnt++;
                    }
                }
                recall_rate += (float)recall_cnt / k;
            }
        }
        std::cout << "recall_rate:" << recall_rate / nq << std::endl;
    }
}

void run_one_test(int d, int nb, int nq, int nl,  float* xb, float* xq, float* xl, idx_t* ids, int sn, std::string desc, std::vector<std::set<int>>& flat_res, int ivf_nprobe) {
    faiss::IndexFlatL2 quantizer(d); // call constructor
    printf("RUN TEST: dimension: %d, vector_cnt: %d, query_cnt: %d, ivf_nprobe: %d, desc: %s=======================\n", d, nb, nq, ivf_nprobe, desc.c_str());
    faiss::Index* idx = faiss::index_factory(d, desc.c_str());
    TimeCost cost;
    
    faiss::Index& index = *idx;
    index.train(nl, xl); // add vectors to the index
    printf("ntotal = %zd, nb:%d,nq:%d, train cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());
    cost.reset();
    if (FLAGS_batch == 0) {
        index.add_with_ids(nb, xb, ids); // add vectors to the index
    } else {
        for (int i = 0; i < nb; i+=FLAGS_batch) {
            TimeCost cost2;
            index.add_with_ids(FLAGS_batch, xb + i * d, ids + i); // add vectors to the index
            if (i % 1000 == 0) {
            printf(" batch= %d, cost=:%ld\n",  FLAGS_batch, cost2.get_time());
            }
        }
    }
    printf("ntotal = %zd, nb:%d,nq:%d, cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());
    cost.reset();
    std::string file_name = desc + ".index";
    faiss::write_index(&index, file_name.c_str());
    printf("ntotal = %zd, nb:%d,nq:%d, write cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());

    query_and_calc_recall(idx, d, nb, nq, nl,  xb, xq, xl, ids, sn, desc, flat_res, ivf_nprobe);
    printf("done=====================================================\n\n\n");
    sleep(1);
}

void load_data(const std::string filename, float*& data, unsigned& num, unsigned& dim) { 
  std::ifstream in(filename.c_str(), std::ios::binary);	//以二进制的方式打开文件
  if (!in.is_open()) {
    std::cout << "open file error" << std::endl;
    exit(-1);
  }
  in.read((char*)&dim, 4);	//读取向量维度
  in.seekg(0, std::ios::end);	//光标定位到文件末尾
  std::ios::pos_type ss = in.tellg();	//获取文件大小（多少字节）
  size_t fsize = (size_t)ss;
  num = (unsigned)(fsize / (dim + 1) / 4);	//数据的个数
  data = new float[(size_t)num * (size_t)dim];

  in.seekg(0, std::ios::beg);	//光标定位到起始处
  for (size_t i = 0; i < num; i++) {
    in.seekg(4, std::ios::cur);	//光标向右移动4个字节
    in.read((char*)(data + i * dim), dim * 4);	//读取数据到一维数据data中
  }
  for(size_t i = 0; i < num * dim; i++) {	//输出数据
    std::cout << (float)data[i];
    if(!i) {
        std::cout << " ";
        continue;
    }
    if(i % (dim - 1) != 0) {
      std::cout << " ";
    }
    else{
      std::cout << std::endl;
      break;
    }
  }
  in.close();
}

void load_shif1M(unsigned& d, unsigned& nb, unsigned& nq, unsigned& nl, float*& xb, float*& xq, float*& xl, idx_t*& ids) {
    unsigned base_dim = 0;
    unsigned learn_dim = 0;
    unsigned query_dim = 0;
    unsigned base_num = 0;
    unsigned learn_num = 0;
    unsigned query_num = 0;
    load_data("./sift_base.fvecs", xb, base_num, base_dim);
    load_data("./sift_learn.fvecs", xl, learn_num, learn_dim);
    load_data("./sift_query.fvecs", xq, query_num, query_dim);
    if (base_dim != learn_dim || base_dim != query_dim) {
        std::cout << "dim error: base_dim: " << base_dim << " learn_dim: " << learn_dim << " query_dim: " << query_dim << std::endl;
        exit(-1);
    }
    std::cout << "load finish: \n" << "base_num: " << base_num << " learn_num: " << learn_num << " query_num: " << query_num << std::endl;
    d = base_dim;
    nb = base_num;
    nq = query_num;
    nl = learn_num;

    ids = new idx_t[nb];
    for (int i = 0; i < nb; i++) {
        ids[i] = i * 2;
    }
    return;
}

TEST(test_sift_1m, case_all) {
    if (!FLAGS_test_recall) {
        return;
    }
    unsigned dim, nb, nq, nl, nrl;
    float* xb; // base
    float* xq; // query
    float* xl; // learn
    float* xrl; // real_learn
    idx_t* ids;// ids

    load_shif1M(dim, nb, nq, nl, xb, xq, xl, ids);

    nq = std::min(nq, (unsigned)FLAGS_query_times);
    std::vector<std::set<int>> flat_res;

    int ivf_nlist = FLAGS_ivf_nlist_factor * sqrt(nb);
    int ivf_nprobe = ivf_nlist / FLAGS_ivf_nprobe_factor;
    int pq_nsub = dim / FLAGS_pq_factor;
    int pqfs_nsub = dim / FLAGS_pqfs_factor;
    int sn = FLAGS_search_num;
    if (FLAGS_learn_vec_cnt <= 100000) {
        xrl = xl;
    } else {
        xrl = xb;
    }
    nrl = FLAGS_learn_vec_cnt;
    std::cout << "ivf_nlist: " << ivf_nlist << " ivf_nprobe: " << ivf_nprobe << "\npq_nsub: " << pq_nsub << " pqfs_nsub: " << pqfs_nsub << std::endl;
    std::cout << "search_num: " << sn << std::endl;
    std::cout << "learn_vec_cnt: " << nrl << std::endl;
    std::vector<std::string> targets;
    targets.emplace_back("IDMap2,Flat");
    if (FLAGS_target != "") {
        targets.emplace_back(FLAGS_target);
    } else {
        targets.emplace_back("IDMap2,HNSW32");
        {
            char desc[48];
            sprintf(desc, "IDMap2,HNSW32,PQ%d", pq_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,PQ%d", pq_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,PQ%d,RFlat", pq_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,PQ%dx4fs", pqfs_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,PQ%dx4fs,RFlat", pqfs_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,IVF%d,Flat", ivf_nlist);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,IVF%d,PQ%d", ivf_nlist, pq_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,IVF%d,PQ%d,RFlat", ivf_nlist, pq_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,IVF%d,PQ%dx4fs", ivf_nlist, pqfs_nsub);
            targets.emplace_back(desc);
        }

        {
            char desc[48];
            sprintf(desc, "IDMap2,IVF%d,PQ%dx4fs,RFlat", ivf_nlist, pqfs_nsub);
            targets.emplace_back(desc);
        }
    }

    for (auto target : targets) {
        run_one_test(dim, nb, nq, nrl, xb, xq, xrl, ids, sn, target, flat_res, ivf_nprobe);
    }

    delete[] xb;
    delete[] xq;
    delete[] ids;
}

TEST(test_load, case_all) {
    if (!FLAGS_read || FLAGS_target == "") {
        return;
    }
    unsigned dim, nb, nq, nl, nrl;
    float* xb; // base
    float* xq; // query
    float* xl; // learn
    float* xrl; // real_learn
    idx_t* ids;// ids

    load_shif1M(dim, nb, nq, nl, xb, xq, xl, ids);

    nq = std::min(nq, (unsigned)FLAGS_query_times);
    std::vector<std::set<int>> flat_res;

    int ivf_nlist = FLAGS_ivf_nlist_factor * sqrt(nb);
    int ivf_nprobe = ivf_nlist / FLAGS_ivf_nprobe_factor;
    int pq_nsub = dim / FLAGS_pq_factor;
    int pqfs_nsub = dim / FLAGS_pqfs_factor;
    int sn = FLAGS_search_num;
    if (FLAGS_learn_vec_cnt <= 100000) {
        xrl = xl;
    } else {
        xrl = xb;
    }
    nrl = FLAGS_learn_vec_cnt;
    std::cout << "ivf_nlist: " << ivf_nlist << " ivf_nprobe: " << ivf_nprobe << "\npq_nsub: " << pq_nsub << " pqfs_nsub: " << pqfs_nsub << std::endl;
    std::cout << "search_num: " << sn << std::endl;
    std::cout << "learn_vec_cnt: " << nrl << std::endl;
    
    std::vector<std::string> targets = {"IDMap2,Flat", FLAGS_target};
    for (auto target : targets) {
        run_one_test(dim, nb, nq, nrl, xb, xq, xrl, ids, sn, target, flat_res, ivf_nprobe);
    }

    // read
    faiss::Index* load_idx;
    TimeCost cost;
    std::string file_name = FLAGS_target + ".index";
    load_idx = faiss::read_index(file_name.c_str());
    printf("nb:%d,nq:%d, read cost=:%ld, is_trained: %d\n" ,nb, nq, cost.get_time(), load_idx->is_trained);
    cost.reset();

    if (!load_idx->is_trained) {
        load_idx->train(nrl, xrl);
        printf("train cost=:%ld\n", cost.get_time());
        cost.reset();
    }
    query_and_calc_recall(load_idx, dim, nb, nq, nl,  xb, xq, xl, ids, sn, FLAGS_target, flat_res, ivf_nprobe);
    // load_idx->add_with_ids(nrl, xrl, ids);
    // printf("add cost=:%ld\n", cost.get_time());
    delete[] xb;
    delete[] xq;
    delete[] ids;
}

int query(std::string sql, baikal::client::Service* _baikaldb, baikal::client::SmartConnection& connection) {
    int ret = 0;
    int retry = 0;
    int affected_rows = 0;
    do {
        if (connection == nullptr) {
            connection = _baikaldb->fetch_connection();
        }
        baikal::client::ResultSet result_set;
        ret = connection->execute(sql, true, &result_set);
        if (ret == 0) {
            affected_rows = result_set.get_affected_rows();
            break;
        }
        bthread_usleep(1000000);
        if (connection != nullptr) {
            connection->close();
        }
        connection = _baikaldb->fetch_connection();
        while (connection == nullptr) {
            if (retry > 3) {
                printf("service fetch_connection() failed");
                break;
            }
            bthread_usleep(1000000);
            connection = _baikaldb->fetch_connection();
            ++retry;
        }
    } while (++retry < 3);
    if (ret != 0) {
        printf("sql_len:%lu query fail finally: %s", sql.size(), sql.c_str());
        return -1;
    }
    return affected_rows;
}

TEST(test_query_baikaldb, case_all) {
    if (!FLAGS_query_baikaldb) {
        return;
    }
    baikal::client::Manager _manager;
    baikal::client::Service* _baikaldb;
    int rc = 0;
    rc = _manager.init("conf", "baikal_client.conf");
    if (rc != 0) {
        printf("baikal client init fail:%d", rc);
        return;
    }
    _baikaldb = _manager.get_service("baikaldb");
    if (_baikaldb == NULL) {
        printf("baikaldb is null");
        return;
    }
    
    unsigned dim, nb, nq, nl, nrl;
    float* xb; // base
    float* xq; // query
    float* xl; // learn
    float* xrl; // real_learn
    idx_t* ids;// ids

    load_shif1M(dim, nb, nq, nl, xb, xq, xl, ids);

    std::string sql;
    std::string vec_string;
    std::vector<std::string> sqls;
    for (int i = 0; i < nq; ++i) {
        sql = "";
        vec_string = "";
        for (int j = 0; j < dim; ++j) {
            vec_string += std::to_string(xq[i * dim + j]) + ",";
        }
        vec_string.pop_back();
        sql = "select id from " + FLAGS_table + " where match(vec) against ('" + vec_string + "' in vector mode) order by __weight " + FLAGS_order + " limit 10;";
        sqls.emplace_back(sql);
    }

    TimeCost total_time;
    TimeCost one_query_time;
    auto connection = _baikaldb->fetch_connection();
    for (int i = 0; i < nq; ++i) {
        one_query_time.reset();
        query(sqls[i], _baikaldb, connection);
        if (one_query_time.get_time() > FLAGS_sql_print_time_limit) {
            std::cout << "query " << i << " cost: " << one_query_time.get_time() << std::endl;
        }
    }
    printf("select %d vector, cost=:%ld\n", nq, total_time.get_time());
}


TEST(test_insert_baikaldb, case_all) {
    if (!FLAGS_insert_to_baikaldb) {
        return;
    }
    baikal::client::Manager _manager;
    baikal::client::Service* _baikaldb;
    int rc = 0;
    rc = _manager.init("conf", "baikal_client.conf");
    if (rc != 0) {
        printf("baikal client init fail:%d", rc);
        return;
    }
    _baikaldb = _manager.get_service("baikaldb");
    if (_baikaldb == NULL) {
        printf("baikaldb is null");
        return;
    }
    
    unsigned dim, nb, nq, nl, nrl;
    float* xb; // base
    float* xq; // query
    float* xl; // learn
    float* xrl; // real_learn
    idx_t* ids;// ids

    load_shif1M(dim, nb, nq, nl, xb, xq, xl, ids);

    std::string sql;
    std::string vec_string;
    std::vector<std::string> vec_strings;
    std::vector<std::string> tables;
    boost::split(tables, FLAGS_table, boost::is_any_of(","));
    for (int i = 0; i < nb && i < FLAGS_insert_row_cnt; ++i) {
        vec_string = "";
        for (int j = 0; j < dim; ++j) {
            vec_string += std::to_string(xb[i * dim + j]) + ",";
        }
        vec_string.pop_back();
        vec_strings.emplace_back(vec_string);
    }

    TimeCost total_time;
    TimeCost one_query_time;
    auto connection = _baikaldb->fetch_connection();
    for (auto table : tables) {
        total_time.reset();
        for (int i = 0; i < vec_strings.size(); ++i) {
            one_query_time.reset();
            sql = "replace into " + table + "(id, vec, id2) values (" + std::to_string(i) + ", '" + vec_strings[i] + "', '" + FLAGS_id2_value + "')";
            query(sql, _baikaldb, connection);
            if (one_query_time.get_time() > FLAGS_sql_print_time_limit) {
                std::cout << "insert " << i << " cost: " << one_query_time.get_time() << std::endl;
            }
        }
        printf("INSERT table: %s, %d vector, cost=:%ld\n", table.c_str(), nb, total_time.get_time());
    }
}

