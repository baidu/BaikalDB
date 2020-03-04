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

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <stdlib.h>
#include <sstream>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

using std::cout;
using std::string;
using std::vector;
using std::ifstream;
using std::ios;
using std::endl;

void change(string& field, int type) {
    if (field.empty()) {
        field = "null";
    } else {
        if (type == 0) {
            //string
            field = "'" + field + "'";
        }
    }
}

string reverse(string& str) {
    if (str.empty()) {
        return string("");
    }
    unsigned long l = 0;
    sscanf(str.c_str(), "%lu", &l);
    char* ptr = (char*)&l;
    char tmp = ptr[0];
    ptr[0] = ptr[7];
    ptr[7] = tmp;
    tmp = ptr[1];
    ptr[1] = ptr[6];
    ptr[6] = tmp;
    tmp = ptr[2];
    ptr[2] = ptr[5];
    ptr[5] = tmp;
    tmp = ptr[3];
    ptr[3] = ptr[4];
    ptr[4] = tmp;
    char s[30];
    snprintf(s, sizeof(s), "%lu", l);
    return string(s);    
}

int main(int argc, char* argv[]) 
{
    string line;
    ifstream fin(argv[1], ios::in);
    while (getline(fin, line)) {
        // imageid,imageid_reverse,url,width,height,source,userid,score,industryid,feedref,outerid,visualid,samefeature,similarfeature,signature,fcref
        string value;
        vector<string> fields;
        boost::split(fields, line, boost::is_any_of("\t"));
        if (fields[0] == "1") {
            if (fields.size() < 32) {
                continue;
            }
            value.append(fields[1]);
            fields[1] = reverse(fields[1]);
            value.append("\t" + fields[1]);
            value.append("\t" + fields[6]);
            value.append("\t" + fields[5]);
            value.append("\t" + fields[4]);
            value.append("\t" + fields[10]);
            value.append("\t" + fields[2]);
            value.append("\t" + fields[31]);
            value.append("\t" + fields[18]);
            value.append("\t");
            value.append("\t" + fields[14]);
            value.append("\t" + fields[20]);
            value.append("\t" + fields[22]);
            value.append("\t" + fields[23]);
            value.append("\t" + fields[8]);
            value.append("\t");
            
        } else if (fields[0] == "2"){
            if (fields.size() < 29) {
                continue;
            }
            value.append(fields[1]);
            fields[1] = reverse(fields[1]);
            value.append("\t" + fields[1]);
            value.append("\t" + fields[5]);
            value.append("\t" + fields[4]);
            value.append("\t" + fields[3]);
            value.append("\t" + fields[9]);
            value.append("\t0");
            value.append("\t" + fields[13]);
            value.append("\t" + fields[17]);
            value.append("\t" + fields[24]);
            value.append("\t" + fields[15]);
            value.append("\t" + fields[19]);
            value.append("\t" + fields[21]);
            value.append("\t" + fields[22]);
            value.append("\t" + fields[7]);
            value.append("\t" + fields[28]);
            
        } else {
            continue;
        }
        cout << value << endl;
    }
    fin.close();
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
