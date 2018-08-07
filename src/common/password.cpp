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

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include "password.h"

#define SHA1CircularShift(bits, word) \
    (((word) << (bits)) | ((word) >> (32 - (bits))))

namespace baikaldb {

const uint32 sha_const_key[5] = {
    0x67452301,
    0xEFCDAB89,
    0x98BADCFE,
    0x10325476,
    0xC3D2E1F0
};

static const uint32  K[] = {
    0x5A827999,
    0x6ED9EBA1,
    0x8F1BBCDC,
    0xCA62C1D6
};

static void sha1_process_message_block(SHA1_CONTEXT *context) {
    //int        t;               /* Loop counter          */
    uint32 temp;        /* Temporary word value      */
    uint32 w[80];       /* Word sequence          */
    uint32 aa;
    uint32 bb;
    uint32 cc;
    uint32 dd;
    uint32 ee;         /* Word buffers          */

    //Initialize the first 16 words in the array W
    for (int t = 0; t < 16; t++) {
        int idx = t * 4;
        w[t] = context->Message_Block[idx] << 24;
        w[t] |= context->Message_Block[idx + 1] << 16;
        w[t] |= context->Message_Block[idx + 2] << 8;
        w[t] |= context->Message_Block[idx + 3];
    }

    for (int t = 16; t < 80; t++) {
        w[t] = SHA1CircularShift(1, w[t-3] ^ w[t-8] ^ w[t-14] ^ w[t-16]);
    }

    aa = context->Intermediate_Hash[0];
    bb = context->Intermediate_Hash[1];
    cc = context->Intermediate_Hash[2];
    dd = context->Intermediate_Hash[3];
    ee = context->Intermediate_Hash[4];

    for (int t = 0; t < 20; t++) {
        temp = SHA1CircularShift(5, aa) + ((bb & cc) |
          ((~bb) & dd)) + ee + w[t] + K[0];
        ee = dd;
        dd = cc;
        cc = SHA1CircularShift(30, bb);
        bb = aa;
        aa = temp;
    }
    for (int t = 20; t < 40; t++) {
        temp = SHA1CircularShift(5, aa) + (bb ^ cc ^ dd) + ee + w[t] + K[1];
        ee = dd;
        dd = cc;
        cc = SHA1CircularShift(30, bb);
        bb = aa;
        aa = temp;
    }
    for (int t = 40; t < 60; t++) {
        temp = (SHA1CircularShift(5, aa) + ((bb & cc) |
          (bb & dd) | (cc & dd)) + ee + w[t] + K[2]);
        ee = dd;
        dd = cc;
        cc = SHA1CircularShift(30, bb);
        bb = aa;
        aa = temp;
    }
    for (int t = 60; t < 80; t++) {
        temp = SHA1CircularShift(5, aa) + (bb ^ cc ^ dd) + ee + w[t] + K[3];
        ee = dd;
        dd = cc;
        cc = SHA1CircularShift(30, bb);
        bb = aa;
        aa = temp;
    }
    context->Intermediate_Hash[0] += aa;
    context->Intermediate_Hash[1] += bb;
    context->Intermediate_Hash[2] += cc;
    context->Intermediate_Hash[3] += dd;
    context->Intermediate_Hash[4] += ee;
    context->Message_Block_Index = 0;
}

static void sha1_pad_message(SHA1_CONTEXT *context) {
    /*
      Check to see if the current message block is too small to hold
      the initial padding bits and length.  If so, we will pad the
      block, process it, and then continue padding into a second
      block.
    */
    int i = context->Message_Block_Index;

    if (i > 55) {
        context->Message_Block[i++] = 0x80;
        bzero((char*) &context->Message_Block[i],
        sizeof(context->Message_Block[0])*(64 - i));
        context->Message_Block_Index = 64;

        /* This function sets context->Message_Block_Index to zero    */
        sha1_process_message_block(context);
        //SHA1ProcessMessageBlock(context);

        bzero((char*) &context->Message_Block[0],
        sizeof(context->Message_Block[0]) * 56);
        context->Message_Block_Index = 56;
    }
    else {
        context->Message_Block[i++] = 0x80;
        bzero((char*) &context->Message_Block[i],
        sizeof(context->Message_Block[0]) * (56 - i));
        context->Message_Block_Index = 56;
    }

    //Store the message length as the last 8 octets
    context->Message_Block[56] = (int8) (context->Length >> 56);
    context->Message_Block[57] = (int8) (context->Length >> 48);
    context->Message_Block[58] = (int8) (context->Length >> 40);
    context->Message_Block[59] = (int8) (context->Length >> 32);
    context->Message_Block[60] = (int8) (context->Length >> 24);
    context->Message_Block[61] = (int8) (context->Length >> 16);
    context->Message_Block[62] = (int8) (context->Length >> 8);
    context->Message_Block[63] = (int8) (context->Length);

    sha1_process_message_block(context);
}
static void my_crypt(uint8_t *to, const uint8 *s1, const uint8 *s2, uint len) {
    const uint8 *s1_end = s1 + len;
    while (s1 < s1_end) {
        *to++ = *s1++ ^ *s2++;
    }
}

int mysql_sha1_reset(SHA1_CONTEXT *context){

#ifndef DBUG_OFF
if (!context) {
    return SHA_NULL;
}
#endif

    context->Length          = 0;
    context->Message_Block_Index      = 0;

    context->Intermediate_Hash[0]   = sha_const_key[0];
    context->Intermediate_Hash[1]   = sha_const_key[1];
    context->Intermediate_Hash[2]   = sha_const_key[2];
    context->Intermediate_Hash[3]   = sha_const_key[3];
    context->Intermediate_Hash[4]   = sha_const_key[4];

    context->Computed   = 0;
    context->Corrupted  = 0;

    return SHA_SUCCESS;
}

int mysql_sha1_input(SHA1_CONTEXT *context, const uint8 *message_array, unsigned  length)
{
    if (!length){
        return SHA_SUCCESS;
    }

#ifndef DBUG_OFF
    /* We assume client konows what it is doing in non-debug mode */
    if (!context || !message_array) {
        return SHA_NULL;
    }
    if (context->Computed){
        return (context->Corrupted = SHA_STATE_ERROR);
    }
    if (context->Corrupted){
        return context->Corrupted;
    }
#endif

    while (length--) {
        context->Message_Block[context->Message_Block_Index++] =
            (*message_array & 0xFF);
        context->Length += 8;  /* Length is in bits */

        if (context->Message_Block_Index == 64) {
            sha1_process_message_block(context);
        }
        message_array++;
    }
    return SHA_SUCCESS;
}

int mysql_sha1_result(SHA1_CONTEXT *context, uint8 Message_Digest[SHA1_HASH_SIZE]) {

#ifndef DBUG_OFF
    if (!context || !Message_Digest){
        return SHA_NULL;
    }

    if (context->Corrupted){
        return context->Corrupted;
    }
#endif

    if (!context->Computed) {
        sha1_pad_message(context);
        //SHA1PadMessage(context);
         /* message may be sensitive, clear it out */
        bzero((char*) context->Message_Block, 64);
        context->Length   = 0;    /* and clear length  */
        context->Computed = 1;
    }
    for (int i = 0; i < SHA1_HASH_SIZE; i++) {
        Message_Digest[i] = (int8)((context->Intermediate_Hash[i >> 2] >> 8
           * (3 - (i & 0x03))));
    }
    return SHA_SUCCESS;
}

void scramble(uint8_t *to, const char *message, const char *password) {

    SHA1_CONTEXT sha1_context;
    uint8 hash_stage1[SHA1_HASH_SIZE];
    uint8 hash_stage2[SHA1_HASH_SIZE];

    mysql_sha1_reset(&sha1_context);
    /* stage 1: hash password */
    mysql_sha1_input(&sha1_context, (uint8 *) password, (uint) strlen(password));
    mysql_sha1_result(&sha1_context, hash_stage1);
    /* stage 2: hash stage 1; note that hash_stage2 is stored in the database */
    mysql_sha1_reset(&sha1_context);
    mysql_sha1_input(&sha1_context, hash_stage1, SHA1_HASH_SIZE);
    mysql_sha1_result(&sha1_context, hash_stage2);
    /* create crypt string as sha1(message, hash_stage2) */
    mysql_sha1_reset(&sha1_context);
    mysql_sha1_input(&sha1_context, (const uint8 *) message, SCRAMBLE_LENGTH);
    mysql_sha1_input(&sha1_context, hash_stage2, SHA1_HASH_SIZE);
    /* xor allows 'from' and 'to' overlap: lets take advantage of it */
    mysql_sha1_result(&sha1_context, (uint8 *) to);
    my_crypt(to, (const uint8 *) to, hash_stage1, SCRAMBLE_LENGTH);
}

} //namespace baikal
