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

#pragma once
#include <sys/types.h>
#include "common.h"

namespace baikaldb {
const int SCRAMBLE_LENGTH   = 20;
const int SHA1_HASH_SIZE    = 20; // Hash size in bytes

void scramble(uint8 *to, const char *message, const char *password);

enum sha_result_codes {
    SHA_SUCCESS         = 0,
    SHA_NULL            = 1, // Null pointer parameter
    SHA_INPUT_TOO_LONG  = 2, // input data too long
    SHA_STATE_ERROR     = 3  // called Input after Result
};

typedef struct SHA1_CONTEXT {
    ulonglong  Length;          // Message length in bits
    uint32 Intermediate_Hash[SHA1_HASH_SIZE / 4]; // Message Digest
    int Computed;               // Is the digest computed?
    int Corrupted;              // Is the message digest corrupted?
    int16 Message_Block_Index;  // Index into message block array
    uint8 Message_Block[64];    // 512-bit message blocks

} SHA1_CONTEXT;

} //namespace baikaldb
