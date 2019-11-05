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

INCLUDE(ExternalProject)

SET(LEVELDB_SOURCES_DIR ${THIRD_PARTY_PATH}/leveldb)
SET(LEVELDB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/leveldb)
SET(LEVELDB_INCLUDE_DIR "${LEVELDB_INSTALL_DIR}/include" CACHE PATH "leveldb include directory." FORCE)
SET(LEVELDB_LIBRARIES "${LEVELDB_INSTALL_DIR}/lib/libleveldb.a" CACHE FILEPATH "leveldb library." FORCE)
INCLUDE_DIRECTORIES(${LEVELDB_INCLUDE_DIR})

ExternalProject_Add(
        extern_leveldb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${LEVELDB_SOURCES_DIR}
        GIT_REPOSITORY "https://github.com/google/leveldb"
        GIT_TAG "1.22"
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${LEVELDB_INSTALL_DIR}
        -DLEVELDB_BUILD_TESTS=OFF
        -DLEVELDB_BUILD_BENCHMARKS=OFF
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        ${EXTERNAL_OPTIONAL_ARGS}
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${LEVELDB_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
        ${THIRD_PARTY_DEP_LOG}
)

ADD_LIBRARY(leveldb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET leveldb PROPERTY IMPORTED_LOCATION ${LEVELDB_LIBRARIES})
ADD_DEPENDENCIES(leveldb extern_leveldb)