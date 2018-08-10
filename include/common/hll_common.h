// Modifications copyright (C) 2018, Baidu.com, Inc.
// Apache Impala (incubating)
// Copyright 2017 The Apache Software Foundation
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

#include "common.h"
#include "expr_value.h"
#include "hll_bias.h"

namespace baikaldb {
namespace hll {
static constexpr int HLL_PRECISION = 10;
static constexpr int HLL_LEN = 1 << HLL_PRECISION;

// Threshold for each precision where it's better to use linear counting instead
// of the bias corrected estimate.
inline float hll_threshold(int p) {
    switch (p) {
        case 4:
            return 10.0;
        case 5:
            return 20.0;
        case 6:
            return 40.0;
        case 7:
            return 80.0;
        case 8:
            return 220.0;
        case 9:
            return 400.0;
        case 10:
            return 900.0;
        case 11:
            return 1800.0;
        case 12:
            return 3100.0;
        case 13:
            return 6500.0;
        case 14:
            return 11500.0;
        case 15:
            return 20000.0;
        case 16:
            return 50000.0;
        case 17:
            return 120000.0;
        case 18:
            return 350000.0;
    }
    return 0.0;
}

// Implements k nearest neighbor interpolation for k=6,
// we choose 6 bassed on the HLL++ paper
inline int64_t hll_estimate_bias(int64_t estimate) {
    const size_t K = 6;

    // Precision index into data arrays
    // We don't have data for precisions less than 4
    static constexpr size_t idx = HLL_PRECISION - 4;

    // Calculate the square of the difference of this estimate to all
    // precalculated estimates for a particular precision
    std::map<double, size_t> distances;
    for (size_t i = 0;
            i < HLL_DATA_SIZES[idx] / sizeof(double); ++i) {
        double val = estimate - HLL_RAW_ESTIMATE_DATA[idx][i];
        distances.insert(std::make_pair(val * val, i));
    }

    size_t nearest[K];
    size_t j = 0;
    // Use a sorted map to find the K closest estimates to our initial estimate
    for (std::map<double, size_t>::iterator it = distances.begin();
            j < K && it != distances.end(); ++it, ++j) {
        nearest[j] = it->second;
    }

    // Compute the average bias correction the K closest estimates
    double bias = 0.0;
    for (size_t i = 0; i < K; ++i) {
        bias += HLL_BIAS_DATA[idx][nearest[i]];
    }

    return bias / K;
}

inline ExprValue hll_init() {
    ExprValue hll(pb::HLL);
    hll.str_val.resize(HLL_LEN);
    return hll;
}

inline int count_trailing_zeros(uint64_t v, int otherwise = 64) {
    if (v == 0) return otherwise;
    return __builtin_ctzl(v);
}

inline void hll_add(uint8_t* buckets, uint64_t hash_value) {
    int idx = hash_value & (HLL_LEN - 1);
    const uint8_t first_one_bit =
        1 + count_trailing_zeros(hash_value >> HLL_PRECISION, 64 - HLL_PRECISION);
    buckets[idx] = std::max(buckets[idx], first_one_bit);
}

inline ExprValue& hll_add(ExprValue& hll, uint64_t hash_value) {
    hll_add((uint8_t*)hll.str_val.data(), hash_value);
    return hll;
}

inline void hll_merge(uint8_t* buckets1, uint8_t* buckets2) {
    for (int i = 0; i < HLL_LEN; i++) {
        buckets1[i] = std::max(buckets1[i], buckets2[i]);
    }
}

inline ExprValue& hll_merge(ExprValue& hll1, ExprValue& hll2) {
    for (size_t i = 0; i < hll1.str_val.size(); i++) {
        hll1.str_val[i] = std::max((uint8_t)hll1.str_val[i], (uint8_t)hll2.str_val[i]);
    }
    return hll1;
}

inline int64_t hll_estimate(uint8_t* buckets, int num_buckets) {
    // Empirical constants for the algorithm.
    float alpha = 0;
    if (HLL_LEN == 16) {
        alpha = 0.673f;
    } else if (HLL_LEN == 32) {
        alpha = 0.697f;
    } else if (HLL_LEN == 64) {
        alpha = 0.709f;
    } else {
        alpha = 0.7213f / (1 + 1.079f / HLL_LEN);
    }

    float harmonic_mean = 0;
    int num_zero_registers = 0;
    // TODO: Consider improving this loop (e.g. replacing 'if' with arithmetic op).
    for (int i = 0; i < num_buckets; ++i) {
        harmonic_mean += powf(2.0f, -buckets[i]);
        if (buckets[i] == 0) ++num_zero_registers;
    }
    harmonic_mean = 1.0f / harmonic_mean;
    int64_t estimate = alpha * HLL_LEN * HLL_LEN * harmonic_mean;
    // Adjust for Hll bias based on Hll++ algorithm
    if (estimate <= 5 * HLL_LEN) {
        estimate -= hll_estimate_bias(estimate);
    }

    if (num_zero_registers == 0) {
        return estimate;
    }

    // Estimated cardinality is too low. Hll is too inaccurate here, instead use
    // linear counting.
    int64_t h = HLL_LEN * log(static_cast<float>(HLL_LEN) / num_zero_registers);

    return (h <= hll_threshold(HLL_PRECISION)) ? h : estimate;
}

inline int64_t hll_estimate(const ExprValue& hll) {
    uint8_t* buckets = (uint8_t*)hll.str_val.data();
    int num_buckets = hll.str_val.size();
    return hll_estimate(buckets, num_buckets);
}

}
}
