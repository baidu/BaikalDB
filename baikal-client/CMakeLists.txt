# Copyright (c) 2019 Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.1)
project(baikal_client C CXX)

file(GLOB BAIKAL_CLIENT
        ${CMAKE_SOURCE_DIR}/baikal-client/src/*.h
        ${CMAKE_SOURCE_DIR}/baikal-client/src/*.hpp
        ${CMAKE_SOURCE_DIR}/baikal-client/src/*.cpp
        )

# for *.so
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../output/lib)
# for *.a
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../output/lib)

add_library(baikal_client ${BAIKAL_CLIENT})
if (NOT WITH_SYSTEM_LIBS)
    add_dependencies(baikal_client ssl crypto zlib boost rapidjson glog gflags protobuf mariadb brpc)
endif ()
target_include_directories(baikal_client PUBLIC ${DEP_INC} ${CMAKE_SOURCE_DIR}/baikal-client/include)
target_link_libraries(baikal_client PUBLIC ${DEP_LIB})

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../output)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../output/include)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/../output/lib)
file(COPY ${CMAKE_SOURCE_DIR}/baikal-client/include/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/../output/include/
        FILES_MATCHING PATTERN "*.h")

#TODO.. install
