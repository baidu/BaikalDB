# Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
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

INCLUDE(ExternalProject)

SET(BRAFT_SOURCES_DIR ${THIRD_PARTY_PATH}/braft)
SET(BRAFT_INSTALL_DIR ${THIRD_PARTY_PATH}/install/braft)
SET(BRAFT_INCLUDE_DIR "${BRAFT_INSTALL_DIR}/include" CACHE PATH "braft include directory." FORCE)
SET(BRAFT_LIBRARIES "${BRAFT_INSTALL_DIR}/lib/libbraft.a" CACHE FILEPATH "braft library." FORCE)

set(prefix_path "${THIRD_PARTY_PATH}/install/brpc|${THIRD_PARTY_PATH}/install/gflags|${THIRD_PARTY_PATH}/install/protobuf|${THIRD_PARTY_PATH}/install/zlib|${THIRD_PARTY_PATH}/install/glog|${THIRD_PARTY_PATH}/install/leveldb")

ExternalProject_Add(
        extern_braft
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS brpc
#        GIT_REPOSITORY "https://github.com/brpc/braft.git"
#        GIT_TAG "v1.1.1"
        URL "https://github.com/baidu/braft/archive/v1.1.1.tar.gz"
        PREFIX ${BRAFT_SOURCES_DIR}
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${BRAFT_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${BRAFT_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DBRPC_WITH_GLOG=ON
        -DWITH_DEBUG_SYMBOLS=OFF
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${BRAFT_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR:PATH=${BRAFT_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR} braft-static
        INSTALL_COMMAND mkdir -p ${BRAFT_INSTALL_DIR}/lib/ COMMAND cp ${BRAFT_SOURCES_DIR}/src/extern_braft/output/lib/libbraft.a ${BRAFT_LIBRARIES} COMMAND cp -r ${BRAFT_SOURCES_DIR}/src/extern_braft/output/include ${BRAFT_INCLUDE_DIR}/
)
ADD_DEPENDENCIES(extern_braft brpc)
ADD_LIBRARY(braft STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET braft PROPERTY IMPORTED_LOCATION ${BRAFT_LIBRARIES})
ADD_DEPENDENCIES(braft extern_braft)
