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

#include <cstdlib>
#include <cstdio>
#include <cstring>
#ifdef BAIDU_INTERNAL
#include <base/sha1.h>
#else
#include <butil/sha1.h>
#endif
#include "password.h"

namespace baikaldb {
static inline void my_xor(uint8_t* to, const uint8_t* s1, const uint8_t* s2, uint32_t len) {
    const uint8_t* s1_end = s1 + len;
    while (s1 < s1_end) {
        *to++ = *s1++ ^ *s2++;
    }
}

void scramble(uint8_t* to, const char* message, const char* password) {
    uint8_t hash_stage1[butil::kSHA1Length];
    uint8_t hash_stage2[butil::kSHA1Length];

    /* stage 1: hash password */
    butil::SHA1HashBytes((const uint8_t*)password, strlen(password), hash_stage1);
    /* stage 2: hash stage 1; note that hash_stage2 is stored in the database */
    butil::SHA1HashBytes(hash_stage1, butil::kSHA1Length, hash_stage2);
    /* create crypt string as sha1(message, hash_stage2) */
    uint8_t final_buf[butil::kSHA1Length * 2];
    memcpy(final_buf, message, butil::kSHA1Length);
    memcpy(final_buf + butil::kSHA1Length, hash_stage2, butil::kSHA1Length);
    butil::SHA1HashBytes((const uint8_t*)final_buf, butil::kSHA1Length * 2, to); 
    /* xor allows 'from' and 'to' overlap: lets take advantage of it */
    my_xor(to, (const uint8_t *) to, hash_stage1, butil::kSHA1Length);
}
} //namespace baikal
