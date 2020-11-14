# Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

INCLUDE(ExternalProject)

SET(BRPC_SOURCES_DIR ${THIRD_PARTY_PATH}/brpc)
SET(BRPC_INSTALL_DIR ${THIRD_PARTY_PATH}/install/brpc)
SET(BRPC_INCLUDE_DIR "${BRPC_INSTALL_DIR}/include" CACHE PATH "brpc include directory." FORCE)
SET(BRPC_LIBRARIES "${BRPC_INSTALL_DIR}/lib/libbrpc.a" CACHE FILEPATH "brpc library." FORCE)

# Reference https://stackoverflow.com/questions/45414507/pass-a-list-of-prefix-paths-to-externalproject-add-in-cmake-args
set(prefix_path "${THIRD_PARTY_PATH}/install/gflags|${THIRD_PARTY_PATH}/install/protobuf|${THIRD_PARTY_PATH}/install/zlib|${THIRD_PARTY_PATH}/install/glog|${THIRD_PARTY_PATH}/install/leveldb")

# If minimal .a is need, you can set  WITH_DEBUG_SYMBOLS=OFF
ExternalProject_Add(
        extern_brpc
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS ssl crypto zlib protobuf leveldb gflags glog
#        GIT_REPOSITORY "https://github.com/apache/incubator-brpc"
#        GIT_TAG "0.9.7"
        URL "https://github.com/apache/incubator-brpc/archive/0.9.7.tar.gz"
        PREFIX ${BRPC_SOURCES_DIR}
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${BRPC_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${BRPC_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DWITH_GLOG=ON
        -DDOWNLOAD_GTEST=OFF
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${BRPC_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR:PATH=${BRPC_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR} brpc-static
        INSTALL_COMMAND mkdir -p ${BRPC_INSTALL_DIR}/lib/ COMMAND cp ${BRPC_SOURCES_DIR}/src/extern_brpc/output/lib/libbrpc.a ${BRPC_LIBRARIES} COMMAND cp -r ${BRPC_SOURCES_DIR}/src/extern_brpc/output/include ${BRPC_INCLUDE_DIR}/
)
ADD_DEPENDENCIES(extern_brpc ssl crypto zlib protobuf leveldb gflags glog)
ADD_LIBRARY(brpc STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET brpc PROPERTY IMPORTED_LOCATION ${BRPC_LIBRARIES})
ADD_DEPENDENCIES(brpc extern_brpc)

add_definitions(-DBRPC_WITH_GLOG=1)
