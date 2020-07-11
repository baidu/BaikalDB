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

SET(ROCKSDB_SOURCES_DIR ${THIRD_PARTY_PATH}/rocksdb)
SET(ROCKSDB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/rocksdb)
SET(ROCKSDB_INCLUDE_DIR "${ROCKSDB_INSTALL_DIR}/include" CACHE PATH "rocksdb include directory." FORCE)
SET(ROCKSDB_LIBRARIES "${ROCKSDB_INSTALL_DIR}/lib/librocksdb.a" CACHE FILEPATH "rocksdb library." FORCE)
INCLUDE_DIRECTORIES(${ROCKSDB_INCLUDE_DIR})

FILE(WRITE ${ROCKSDB_SOURCES_DIR}/src/build.sh
        "PORTABLE=1 make -j${NUM_OF_PROCESSOR} static_lib"
        )

ExternalProject_Add(
        extern_rocksdb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS gflags zlib snappy
        PREFIX ${ROCKSDB_SOURCES_DIR}
#        GIT_REPOSITORY "https://github.com/facebook/rocksdb.git"
#        GIT_TAG "v6.8.1"
        URL "https://github.com/facebook/rocksdb/archive/v6.8.1.tar.gz"
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
        INSTALL_COMMAND mkdir -p ${ROCKSDB_INSTALL_DIR}/lib COMMAND cp -r include ${ROCKSDB_INSTALL_DIR}/ COMMAND cp librocksdb.a ${ROCKSDB_LIBRARIES}
)

ADD_DEPENDENCIES(extern_rocksdb zlib snappy gflags)
ADD_LIBRARY(rocksdb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${ROCKSDB_LIBRARIES})
ADD_DEPENDENCIES(rocksdb extern_rocksdb)
