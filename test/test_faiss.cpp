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
#include <gflags/gflags.h>
#include <base/time.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/IndexHNSW.h>
#include <faiss/AutoTune.h>
#include <faiss/index_io.h>
#include <faiss/index_factory.h>

DEFINE_int32(nb, 100000, "raft_write concurrency, default:40");
DEFINE_int32(nq, 1000, "service_write concurrency, default:40");
DEFINE_int32(m, 16, "service_write concurrency, default:40");
DEFINE_int32(tp, 0, "service_write concurrency, default:40");
DEFINE_int32(nlist, 0, "service_write concurrency, default:40");
DEFINE_bool(read, false, "service_write concurrency, default:40");
DEFINE_int32(batch, 0, "service_write concurrency, default:40");
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

TEST(test_flat, case_all) {
	int d = 128;      // dimension
    int nb = FLAGS_nb; // database size
    int nq = FLAGS_nq;  // nb of queries

    std::mt19937 rng;
    std::uniform_real_distribution<> distrib;
    TimeCost cost1;

    float* xb = new float[d * nb];
    float* xq = new float[d * nq];
    idx_t* ids = new idx_t[nb];

    printf("nb:%d,nq:%d, cost=:%ld\n", nb, nq, cost1.get_time());
    sleep(5);
    //if (!FLAGS_read) {
    for (int i = 0; i < nb; i++) {
        for (int j = 0; j < d; j++)
            xb[d * i + j] = distrib(rng);
        xb[d * i] += i / 1000.;
    }
    //}
    for (int i = 0; i < nb; i++) {
        ids[i] = i * 2;
    }

    for (int i = 0; i < nq; i++) {
        for (int j = 0; j < d; j++)
            xq[d * i + j] = distrib(rng);
        xq[d * i] += i / 1000.;
    }

    printf("nb:%d,nq:%d, cost=:%ld\n", nb, nq, cost1.get_time());
    sleep(10);

    if (FLAGS_tp == 0 || FLAGS_tp == 1) {
    faiss::Index* idx = faiss::index_factory(d, "IDMap,Flat");
    TimeCost cost;
    if (FLAGS_read) {
        idx = faiss::read_index("flat_index.faissindex");
        printf("ntotal=%zd nb:%d,nq:%d, read cost=:%ld\n", idx->ntotal,nb, nq, cost.get_time());
        cost.reset();
    } else {
        faiss::Index& index = *idx;
        printf("is_trained = %s\n", index.is_trained ? "true" : "false");
        TimeCost cost;
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
        printf(" ntotal = %zd, nb:%d,nq:%d, cost=:%ld\n",  index.ntotal, nb, nq, cost.get_time());
        cost.reset();
        faiss::write_index(&index, "flat_index.faissindex");
        printf("ntotal = %zd, nb:%d,nq:%d, write cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());
    }
    faiss::Index& index = *idx;

    int k = 4;

    { // sanity check: search 5 first vectors of xb
        idx_t* I = new idx_t[k * 5];
        float* D = new float[k * 5];

        index.search(5, xb, k, D, I);
        printf("search 5 = %zd, cost=:%ld\n", index.ntotal, cost.get_time());
        cost.reset();

        // print results
        printf("I=\n");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < k; j++)
                printf("%5zd ", I[i * k + j]);
            printf("\n");
        }

        printf("D=\n");
        for (int i = 0; i < 5; i++) {
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
            index.search(1, xq + i, k, D + i*k, I + i*k);
            printf("search i = %d, cost=:%ld\n", i, cost.get_time());
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

        delete[] I;
        delete[] D;
    }
    delete[] xb;
    delete[] xq;
    printf("done2\n");
    sleep(10);
    }

    if (FLAGS_tp == 0 || FLAGS_tp == 2) 
    {
    faiss::IndexFlatL2 quantizer(d); // call constructor
    int nlist = FLAGS_nlist == 0 ? sqrt(FLAGS_nb) : FLAGS_nlist;
    int m = 16;
    faiss::Index* idx = faiss::index_factory(d, "IVF1000,PQ16");
    TimeCost cost;
    if (FLAGS_read) {
        idx = faiss::read_index("ivf_index.faissindex");
        printf(" nb:%d,nq:%d, read cost=:%ld\n" ,nb, nq, cost.get_time());
        cost.reset();
    } else {
        faiss::Index& index = *idx;
        printf("is_trained = %s nlist=%d\n", index.is_trained ? "true" : "false", nlist);
        index.train(nb, xb); // add vectors to the index
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
        faiss::write_index(&index, "ivf_index.faissindex");
        printf("ntotal = %zd, nb:%d,nq:%d, write cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());
    }
    faiss::ParameterSpace().set_index_parameter(idx, "nprobe", 5);
    faiss::Index& index = *idx;

    int k = 4;

    { // sanity check: search 5 first vectors of xb
        idx_t* I = new idx_t[k * 5];
        float* D = new float[k * 5];

        index.search(5, xb, k, D, I);
        printf("search 5 = %zd, cost=:%ld\n", index.ntotal, cost.get_time());
        cost.reset();

        // print results
        printf("I=\n");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < k; j++)
                printf("%5zd ", I[i * k + j]);
            printf("\n");
        }

        printf("D=\n");
        for (int i = 0; i < 5; i++) {
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
            index.search(1, xq + i, k, D + i * k, I + i * k);
            printf("search i = %d, cost=:%ld\n", i, cost.get_time());
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

        delete[] I;
        delete[] D;
    }/*
    delete[] xb;
    delete[] xq;
    printf("done2\n");
    sleep(10);*/
    }

    if (FLAGS_tp == 0 || FLAGS_tp == 3) {
    int m = FLAGS_m;
    //faiss::Index* idx = new faiss::IndexHNSWFlat(d, m);
    faiss::Index* idx = faiss::index_factory(d, "IDMap,HNSW16");
    TimeCost cost;
    if (FLAGS_read) {
        idx = faiss::read_index("hnsw_index.faissindex");
        printf("nb:%d,nq:%d, read cost=:%ld\n",nb, nq, cost.get_time());
        cost.reset();
    } else {
        faiss::Index& index = *idx;
        printf("is_trained = %s\n", index.is_trained ? "true" : "false");
        TimeCost cost;
        if (!index.is_trained) {
            index.train(nb, xb); // add vectors to the index
            printf("ntotal = %zd, nb:%d,nq:%d, train cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());
        }
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
        faiss::write_index(&index, "hnsw_index.faissindex");
        printf("ntotal = %zd, nb:%d,nq:%d, write cost=:%ld\n", index.ntotal, nb, nq, cost.get_time());
    }
    faiss::Index& index = *idx;
    cost.reset();

    int k = 4;

    { // sanity check: search 5 first vectors of xb
        idx_t* I = new idx_t[k * 5];
        float* D = new float[k * 5];

        index.search(5, xb, k, D, I);
        printf("search 5 = %zd, cost=:%ld\n", index.ntotal, cost.get_time());
        cost.reset();

        // print results
        printf("I=\n");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < k; j++)
                printf("%5zd ", I[i * k + j]);
            printf("\n");
        }

        printf("D=\n");
        for (int i = 0; i < 5; i++) {
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
            index.search(1, xq + i, k, D + i*k, I + i*k);
            printf("search i = %d, cost=:%ld\n", i, cost.get_time());
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

        delete[] I;
        delete[] D;
    }
    delete[] xb;
    delete[] xq;
    printf("done2\n");
    sleep(10);
    }
    printf("done2\n");
    sleep(10);

}

