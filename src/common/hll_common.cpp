/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
// redis HyperLogLog implementation to C++

#include "hll_common.h"
#include "log.h"

#include <math.h>
#include <cstring>
namespace baikaldb {
namespace hll {

static int hll_pat_len(uint64_t hash_value, long *regp) {
    uint64_t bit = 0, index = 0;
    int count = 0;
    index = hash_value & HLL_P_MASK; /* Register index. */
    hash_value >>= HLL_P; /* Remove bits used to address the register. */
    hash_value |= ((uint64_t)1<<HLL_Q); /* Make sure the loop terminates
                                          and count will be <= Q+1. */
    bit = 1;
    count = 1;
    while((hash_value & bit) == 0) {
        count++;
        bit <<= 1;
    }
    *regp = (int) index;
    return count;
}

static bool is_hll_object(const std::string& hll) {
    if (hll.size() < HLL_HDR_SIZE) {
        DB_WARNING("hll has wrong size %lu", hll.size());
        return false;
    }

    struct hllhdr *hdr = (struct hllhdr *)hll.data();

    if (hdr->magic[0] != 'H' || hdr->magic[1] != 'Y' ||
        hdr->magic[2] != 'L' || hdr->magic[3] != 'L') {
        DB_WARNING("magic error");
        return false;
    }

    if (hdr->encoding > HLL_MAX_ENCODING && hdr->encoding != HLL_RAW) {
        DB_WARNING("encoding error");
        return false;
    }
    if (hdr->encoding == HLL_DENSE &&
        hll.size() != HLL_DENSE_SIZE) {
        DB_WARNING("dense encode has wrong size %lu", hll.size());
        return false;
    }
    return true;
}

static int hll_dense_set(uint8_t *registers, long index, uint8_t count) {
    uint8_t oldcount = 0;

    HLL_DENSE_GET_REGISTER(oldcount,registers,index);
    if (count > oldcount) {
        HLL_DENSE_SET_REGISTER(registers,index,count);
        return 1;
    } else {
        return 0;
    }
}

static int hll_dense_add(uint8_t *registers, uint64_t hash_value) {
    long index = 0;
    uint8_t count = hll_pat_len(hash_value, &index);
    return hll_dense_set(registers,index,count);
}

static void hll_dense_reghisto(uint8_t *registers, int* reghisto) {
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
                      r10, r11, r12, r13, r14, r15;
        for (int j = 0; j < 1024; j++) {
            /* Handle 16 registers per iteration. */
            r0 = r[0] & 63;
            r1 = (r[0] >> 6 | r[1] << 2) & 63;
            r2 = (r[1] >> 4 | r[2] << 4) & 63;
            r3 = (r[2] >> 2) & 63;
            r4 = r[3] & 63;
            r5 = (r[3] >> 6 | r[4] << 2) & 63;
            r6 = (r[4] >> 4 | r[5] << 4) & 63;
            r7 = (r[5] >> 2) & 63;
            r8 = r[6] & 63;
            r9 = (r[6] >> 6 | r[7] << 2) & 63;
            r10 = (r[7] >> 4 | r[8] << 4) & 63;
            r11 = (r[8] >> 2) & 63;
            r12 = r[9] & 63;
            r13 = (r[9] >> 6 | r[10] << 2) & 63;
            r14 = (r[10] >> 4 | r[11] << 4) & 63;
            r15 = (r[11] >> 2) & 63;

            reghisto[r0]++;
            reghisto[r1]++;
            reghisto[r2]++;
            reghisto[r3]++;
            reghisto[r4]++;
            reghisto[r5]++;
            reghisto[r6]++;
            reghisto[r7]++;
            reghisto[r8]++;
            reghisto[r9]++;
            reghisto[r10]++;
            reghisto[r11]++;
            reghisto[r12]++;
            reghisto[r13]++;
            reghisto[r14]++;
            reghisto[r15]++;

            r += 12;
        }
    } else {
        for(int j = 0; j < HLL_REGISTERS; j++) {
            unsigned long reg;
            HLL_DENSE_GET_REGISTER(reg,registers,j);
            reghisto[reg]++;
        }
    }
}

static int hll_sparse_set_promote(std::string& hll, long index, uint8_t count) {
    if (hll_sparse_to_dense(hll) < 0) {
        DB_WARNING("sparse to dense failed index:%ld count:%d", index, count);
        return -1;
    }
    struct hllhdr *hdr = (struct hllhdr *)hll.data();
    int dense_retval = hll_dense_set(hdr->registers, index, count);
    if (dense_retval != 1) {
        return -1;
    }
    HLL_INVALIDATE_CACHE(hdr);
    return dense_retval;
}

int hll_sparse_set_update(std::string& hll, size_t begin, size_t end) {
    int scanlen = 5;
    uint8_t *p = (uint8_t *)hll.data();
    while (begin < end && scanlen--) {
        if (HLL_SPARSE_IS_XZERO(p+begin)) {
            begin += 2;
            continue;
        } else if (HLL_SPARSE_IS_ZERO(p+begin)) {
            begin++;
            continue;
        }
        if (begin+1 < end && HLL_SPARSE_IS_VAL(p+begin+1)) {
            int v1 = HLL_SPARSE_VAL_VALUE(p+begin);
            int v2 = HLL_SPARSE_VAL_VALUE(p+begin+1);
            if (v1 == v2) {
                int len = HLL_SPARSE_VAL_LEN(p+begin)+HLL_SPARSE_VAL_LEN(p+begin+1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN) {
                    HLL_SPARSE_VAL_SET(p+begin+1,v1,len);
                    hll.erase(begin, 1);
                    p = (uint8_t *)hll.data();
                    end--;
                    continue;
                }
            }
        }
        begin++;
    }
    struct hllhdr *hdr = (struct hllhdr *)hll.data();
    HLL_INVALIDATE_CACHE(hdr);
    return 1;
}

static int hll_sparse_set(std::string& hll, long index, uint8_t count) {
    if (count > HLL_SPARSE_VAL_MAX_VALUE) {
        DB_WARNING("cout > 32 hll_sparse_to_dense");
        return hll_sparse_set_promote(hll, index, count);
    }
    size_t  hll_size = hll.size();
    uint8_t *sparse = (uint8_t*)hll.data();
    uint8_t *start = sparse;
    uint8_t *p = sparse + HLL_HDR_SIZE;
    uint8_t *end = sparse + hll_size;
    sparse = p;
    long first = 0;
    long span = 0;
    uint8_t *prev = nullptr;
    uint8_t oldcount = 0;
    while(p < end) {
        long oplen = 1;
        if (HLL_SPARSE_IS_ZERO(p)) {
            span = HLL_SPARSE_ZERO_LEN(p);
        } else if (HLL_SPARSE_IS_VAL(p)) {
            span = HLL_SPARSE_VAL_LEN(p);
        } else {
            span = HLL_SPARSE_XZERO_LEN(p);
            oplen = 2;
        }
        if (index <= first+span-1) break;
        prev = p;
        p += oplen;
        first += span;
    }
    if (span == 0 || p >= end) {
        DB_WARNING("Invalid format span:%ld p:%p end:%p", span, p, end);
        return -1;
    }
    long is_zero = 0;
    long is_xzero = 0;
    long is_val = 0;
    long runlen = 0;
    if (HLL_SPARSE_IS_ZERO(p)) {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    } else if (HLL_SPARSE_IS_XZERO(p)) {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    } else {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }
    if (is_val) {
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        if (oldcount >= count) {
            return 0;
        }

        if (runlen == 1) {
            HLL_SPARSE_VAL_SET(p,count,1);
            p = prev ? prev : sparse;
            return hll_sparse_set_update(hll, p - start, end - start);
        }
    }
    if (is_zero && runlen == 1) {
        HLL_SPARSE_VAL_SET(p,count,1);
        p = prev ? prev : sparse;
        return hll_sparse_set_update(hll, p - start, end - start);
    }
    uint8_t seq[5], *n = seq;
    int len;
    int last = first+span-1;
    if (is_zero || is_xzero) {
        if (index != first) {
            len = index-first;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n,len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n,len);
                n++;
            }
        }
    } else {
        int curval = HLL_SPARSE_VAL_VALUE(p);
        if (index != first) {
            len = index-first;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
        HLL_SPARSE_VAL_SET(n,count,1);
        n++;
        if (index != last) {
            len = last-index;
            HLL_SPARSE_VAL_SET(n,curval,len);
            n++;
        }
    }
    int seqlen = n-seq;
    int oldlen = is_xzero ? 2 : 1;
    int deltalen = seqlen-oldlen;

    if (deltalen > 0 && (hll_size+deltalen > hll_sparse_max_bytes)) {
        //DB_WARNING("size > 3000 hll_sparse_to_dense");
        return hll_sparse_set_promote(hll, index, count);
    }
    size_t front= p - start;
    hll.replace(front, oldlen, (char *)seq, seqlen);
    end += deltalen;
    p = prev ? prev : sparse;
    return hll_sparse_set_update(hll, p - start, end - start);
}

int hll_sparse_to_dense(std::string& hll) {
    uint8_t *sparse = (uint8_t *)hll.data();
    struct hllhdr *oldhdr = (struct hllhdr*)sparse;

    if (oldhdr->encoding == HLL_DENSE) {
        return 0;
    }
    std::string dense;
    dense.resize(HLL_DENSE_SIZE);
    struct hllhdr *hdr = (struct hllhdr*)dense.data();
    *hdr = *oldhdr;
    hdr->encoding = HLL_DENSE;
    if (oldhdr->encoding == HLL_RAW) {
        uint8_t *max = oldhdr->registers;
        for (int j = 0; j < HLL_REGISTERS; j++) {
            if (max[j] == 0) continue;
            hdr = (struct hllhdr *)dense.data();
            switch (hdr->encoding) {
            case HLL_DENSE: {
                hll_dense_set(hdr->registers, j, max[j]);
                break;
            }
            case HLL_SPARSE: {
                hll_sparse_set(dense, j, max[j]);
                break;
            }
            default:
                break;
            }
        }
    } else {
        uint8_t *p   = (uint8_t*)sparse;
        uint8_t *end = p + hll.size();
        int idx = 0, runlen = 0, regval = 0;
        p += HLL_HDR_SIZE;
        while(p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                idx += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                idx += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                if ((runlen + idx) > HLL_REGISTERS) break;
                while(runlen--) {
                    HLL_DENSE_SET_REGISTER(hdr->registers,idx,regval);
                    idx++;
                }
                p++;
            }
        }
        // != 16384
        if (idx != HLL_REGISTERS) {
            DB_WARNING("WRONG idx:%d", idx);
            return -1;
        }
    }
    hll = dense;
    return 0;
}

int hll_raw_to_sparse(std::string& hll) {
    uint8_t *raw = (uint8_t *)hll.data();
    struct hllhdr *oldhdr = (struct hllhdr*)raw;
    if (oldhdr->encoding != HLL_RAW) {
        return 0;
    }
    std::string sparse;
    hll_sparse_init(sparse);
    struct hllhdr *hdr = (struct hllhdr*)sparse.data();
    uint8_t *max = oldhdr->registers;
    for (int j = 0; j < HLL_REGISTERS; j++) {
        if (max[j] == 0) continue;
        hdr = (struct hllhdr *)sparse.data();
        switch (hdr->encoding) {
        case HLL_DENSE: {
            hll_dense_set(hdr->registers, j, max[j]);
            break;
        }
        case HLL_SPARSE: {
            hll_sparse_set(sparse, j, max[j]);
            break;
        }
        default:
            break;
        }
    }
    hll = sparse;
    return 0;
}

static int hll_sparse_add(std::string& hll, uint64_t hash_value) {
    long index = 0;
    uint8_t count = hll_pat_len(hash_value, &index);
    return hll_sparse_set(hll, index, count);
}

static int hll_row_add(std::string& hll, uint64_t hash_value) {
    long index = 0;
    uint8_t count = hll_pat_len(hash_value, &index);
    struct hllhdr *hdr = (struct hllhdr *)hll.data();
    if (hdr->registers[index] < count) {
        hdr->registers[index] = count;
        HLL_INVALIDATE_CACHE(hdr);
    }
    return 0;
}

int hll_add(std::string& hll, uint64_t hash_value) {
    if (is_hll_object(hll)) { 
        struct hllhdr *hdr = (struct hllhdr *)hll.data();
        switch(hdr->encoding) {
            case HLL_DENSE: {
                int ret = hll_dense_add(hdr->registers, hash_value);
                if (ret == 1) {
                     HLL_INVALIDATE_CACHE(hdr);
                }
                return ret;
            }
            case HLL_SPARSE:
                return hll_sparse_add(hll, hash_value);
            case HLL_RAW:
                return hll_row_add(hll, hash_value);
            default:
                DB_WARNING("unknown encode type");
                return -1;
        }
    } else {
        DB_WARNING("wrong hll object");
        return -1;
    }
}

ExprValue& hll_add(ExprValue& hll, uint64_t hash_value) {
    if (is_hll_object(hll.str_val)) { 
        hll_add(hll.str_val, hash_value);
    } else {
        DB_WARNING("wrong hll object");
    }
    return hll;
}

void hll_add(std::string* hll, uint64_t hash_value) {
    if (is_hll_object(*hll)) { 
        hll_add(*hll, hash_value);
    } else {
        DB_WARNING("wrong hll object");
    }
}

static void hll_sparse_reghisto(uint8_t *sparse, int sparselen, bool* invalid, int* reghisto) {
    int idx = 0, runlen = 0, regval = 0;
    uint8_t *end = sparse+sparselen, *p = sparse;
    while(p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            idx += runlen;
            reghisto[0] += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            idx += runlen;
            reghisto[0] += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            idx += runlen;
            reghisto[regval] += runlen;
            p++;
        }
    }
    if (idx != HLL_REGISTERS && invalid) {
        *invalid = true;
        DB_WARNING("invalid idx =%d HLL_REGISTERS:%d", idx, HLL_REGISTERS);
    }
}

static void hll_row_reghisto(uint8_t *registers, int* reghisto) {
    uint64_t *word = (uint64_t*) registers;
    for (int j = 0; j < HLL_REGISTERS/8; j++) {
        if (*word == 0) {
            reghisto[0] += 8;
        } else {
            uint8_t *bytes = (uint8_t*) word;
            reghisto[bytes[0]]++;
            reghisto[bytes[1]]++;
            reghisto[bytes[2]]++;
            reghisto[bytes[3]]++;
            reghisto[bytes[4]]++;
            reghisto[bytes[5]]++;
            reghisto[bytes[6]]++;
            reghisto[bytes[7]]++;
        }
        word++;
    }
}

static double hll_sigma(double x) {
    if (x == 1.) return INFINITY;
    double zPrime = 0.0;
    double y = 1;
    double z = x;
    do {
        x *= x;
        zPrime = z;
        z += x * y;
        y += y;
    } while(zPrime != z);
    return z;
}

static double hll_tau(double x) {
    if (x == 0. || x == 1.) return 0.;
    double zPrime = 0.0;
    double y = 1.0;
    double z = 1 - x;
    do {
        x = sqrt(x);
        zPrime = z;
        y *= 0.5;
        z -= pow(1 - x, 2)*y;
    } while(zPrime != z);
    return z / 3;
}

uint64_t hll_estimate(const std::string& hll, bool* invalid) {
    struct hllhdr *hdr = (struct hllhdr *)hll.data();
    uint64_t card = 0;
    if (HLL_VALID_CACHE(hdr)) {
        /* Just return the cached value. */
        card = (uint64_t)hdr->card[0];
        card |= (uint64_t)hdr->card[1] << 8;
        card |= (uint64_t)hdr->card[2] << 16;
        card |= (uint64_t)hdr->card[3] << 24;
        card |= (uint64_t)hdr->card[4] << 32;
        card |= (uint64_t)hdr->card[5] << 40;
        card |= (uint64_t)hdr->card[6] << 48;
        card |= (uint64_t)hdr->card[7] << 56;
    } else {
        double m = HLL_REGISTERS;
        double E = 0.0;
        int reghisto[64] = {0};

        if (hdr->encoding == HLL_DENSE) {
            hll_dense_reghisto(hdr->registers,reghisto);
        } else if (hdr->encoding == HLL_SPARSE) {
            hll_sparse_reghisto(hdr->registers,
                         hll.size()-HLL_HDR_SIZE,invalid,reghisto);
        } else if (hdr->encoding == HLL_RAW) {
            hll_row_reghisto(hdr->registers,reghisto);
        } else {
            DB_WARNING("Unknown HyperLogLog encoding in hll_estimate()");
            return 0;
        }
        double z = m * hll_tau((m-reghisto[HLL_Q+1])/(double)m);
        for (int j = HLL_Q; j >= 1; --j) {
            z += reghisto[j];
            z *= 0.5;
        }
        z += m * hll_sigma(reghisto[0]/(double)m);
        if (!float_equal(z, 0.0)) {
            E = llroundl(HLL_ALPHA_INF*m*m/z);
        }
        card = (uint64_t) E;
        hdr->card[0] = card & 0xff;
        hdr->card[1] = (card >> 8) & 0xff;
        hdr->card[2] = (card >> 16) & 0xff;
        hdr->card[3] = (card >> 24) & 0xff;
        hdr->card[4] = (card >> 32) & 0xff;
        hdr->card[5] = (card >> 40) & 0xff;
        hdr->card[6] = (card >> 48) & 0xff;
        hdr->card[7] = (card >> 56) & 0xff;
    }
    return card;
}

uint64_t hll_estimate(const std::string& hll) {
    uint64_t count = 0;
    bool invalid = false;
    if (is_hll_object(hll)) { 
        count = hll_estimate(hll, &invalid);
        if (invalid) {
            DB_WARNING("cardinality counting is invalid count:%lu", count);
        }
    } else {
        DB_WARNING("wrong hll object");
    }    
    return count;
}

uint64_t hll_estimate(const ExprValue& hll) {
    uint64_t count = 0;
    bool invalid = false;
    if (is_hll_object(hll.str_val)) { 
        count = hll_estimate(hll.str_val, &invalid);
        if (invalid) {
            DB_WARNING("cardinality counting is invalid count:%lu", count);
        }
    } else {
        DB_WARNING("wrong hll object");
    }    
    return count;
}

int hll_merge_agg(std::string& hll1, std::string& hll2) {
    if (!is_hll_object(hll1) || !is_hll_object(hll2)) {
        DB_WARNING("wrong hll object");
        return -1;
    }
    struct hllhdr *hdr1 = (struct hllhdr *)hll1.data();
    if (hdr1->encoding != HLL_RAW) {
        if (hll_sparse_to_dense(hll1) < 0) {
            return -1;
        }
        hdr1 = (struct hllhdr *)hll1.data();
    }
    struct hllhdr *hdr2 = (struct hllhdr *)hll2.data();
    uint8_t *max = hdr1->registers;
    if (hdr2->encoding == HLL_DENSE) {
        uint8_t val = 0;
        for (int i = 0; i < HLL_REGISTERS; i++) {
            HLL_DENSE_GET_REGISTER(val,hdr2->registers,i);
            if (hdr1->encoding != HLL_RAW) {
                hll_dense_set(hdr1->registers,i,val);
            } else {
                if (val > max[i]) {
                    max[i] = val;
                }
            }
        }
    } else if (hdr2->encoding == HLL_RAW) {
        for (int i = 0; i < HLL_REGISTERS; i++) {
            if (hdr2->registers[i] > max[i]) {
                max[i] = hdr2->registers[i];
            }
        }
    } else {
        uint8_t *p = (uint8_t *)hll2.data();
        uint8_t *end = p + hll2.size();
        long runlen = 0 , regval = 0;
        p += HLL_HDR_SIZE;
        int i = 0;
        while(p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                if ((runlen + i) > HLL_REGISTERS) {
                    break;
                }
                while(runlen--) {
                    if (hdr1->encoding != HLL_RAW) {
                        hll_dense_set(hdr1->registers,i,regval);
                    } else {
                        if (regval > max[i]) {
                            max[i] = regval;
                        }
                    }
                    i++;
                }
                p++;
            }
        }
        if (i != HLL_REGISTERS) {
            return -1;
        }
    }

    HLL_INVALIDATE_CACHE(hdr1);
    return 0;
}

int hll_merge(uint8_t *max, std::string& hll) {
    struct hllhdr *hdr = (struct hllhdr *)hll.data();
    int i = 0;
    if (hdr->encoding == HLL_DENSE) {
        uint8_t val = 0;
        for (i = 0; i < HLL_REGISTERS; i++) {
            HLL_DENSE_GET_REGISTER(val,hdr->registers,i);
            if (val > max[i]) {
                max[i] = val;
            }
        }
    } else if (hdr->encoding == HLL_RAW) {
        for (i = 0; i < HLL_REGISTERS; i++) {
            if (hdr->registers[i] > max[i]) {
                max[i] = hdr->registers[i];
            }
        }
    } else {
        uint8_t *p = (uint8_t *)hll.data();
        uint8_t *end = p + hll.size();
        long runlen = 0;
        long regval = 0;
        p += HLL_HDR_SIZE;
        i = 0;
        while (p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                if ((runlen + i) > HLL_REGISTERS) {
                    break;
                }
                while (runlen--) {
                    if (regval > max[i]) {
                        max[i] = regval;
                    }
                    i++;
                }
                p++;
            }
        }
        if (i != HLL_REGISTERS) {
            return -1;
        }
    }
    return 0;
}

int hll_merge(std::string& hll1, std::string& hll2) {
    if (!is_hll_object(hll1) || !is_hll_object(hll2)) {
        DB_WARNING("wrong hll object");
        return -1;
    }
    uint8_t max[HLL_REGISTERS];
    memset(max,0,sizeof(max));
    int use_dense = 0;
    struct hllhdr *hdr = (struct hllhdr *)hll1.data();
    if (hdr->encoding == HLL_DENSE) {
        use_dense = 1;
    }
    hdr = (struct hllhdr *)hll2.data();
    if (hdr->encoding == HLL_DENSE) {
        use_dense = 1;
    }
    if (hll_merge(max,hll1) < 0) {
        return -1;
    }
    if (hll_merge(max,hll2) < 0) {
        return -1;
    }
    if (use_dense && hll_sparse_to_dense(hll1) < 0) {
        return -1;
    }
    hdr = (struct hllhdr *)hll1.data();
    for (int j = 0; j < HLL_REGISTERS; j++) {
        if (max[j] == 0) {
            continue;
        }
        hdr = (struct hllhdr *)hll1.data();
        switch (hdr->encoding) {
        case HLL_DENSE: {
            hll_dense_set(hdr->registers,j,max[j]);
            break;
        }
        case HLL_SPARSE: {
            hll_sparse_set(hll1,j,max[j]);
            break;
        }
        }
    }
    hdr = (struct hllhdr *)hll1.data();
    HLL_INVALIDATE_CACHE(hdr);
    return 0;
}

extern void hll_sparse_init(std::string& val) {
    int sparselen = HLL_HDR_SIZE +
                    (((HLL_REGISTERS+(HLL_SPARSE_XZERO_MAX_LEN-1)) /
                     HLL_SPARSE_XZERO_MAX_LEN)*2);
    val.resize(sparselen);
    val.replace(0, 4, "HYLL");
    struct hllhdr *hdr = (struct hllhdr *)val.data();
    hdr->encoding = HLL_SPARSE;
    int aux = HLL_REGISTERS;
    uint8_t *p = hdr->registers;
    while(aux) {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux) {
            xzero = aux;
        }
        HLL_SPARSE_XZERO_SET(p,xzero);
        p += 2;
        aux -= xzero;
    }
}

ExprValue hll_init() {
    ExprValue hll(pb::HLL);
    hll_sparse_init(hll.str_val);
    return hll;
}

ExprValue hll_row_init() {
    ExprValue hll(pb::HLL);
    int rowlen = HLL_HDR_SIZE + HLL_REGISTERS;
    hll.str_val.resize(rowlen);
    hll.str_val.replace(0, 4, "HYLL");
    struct hllhdr *hdr = (struct hllhdr *)hll.str_val.data();
    hdr->encoding = HLL_RAW;
    return hll;
}

} // namespace hll
} // namespace baikaldb
