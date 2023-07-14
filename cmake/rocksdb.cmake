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
SET(ROCKSDB_LIBRARIES "${ROCKSDB_INSTALL_DIR}/lib64/librocksdb.a" CACHE FILEPATH "rocksdb library." FORCE)
# For some reason, libraries are under either lib or lib64
include(GNUInstallDirs)
SET(ROCKSDB_LIBRARIES "${ROCKSDB_INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/librocksdb.a" CACHE FILEPATH "rocksdb library." FORCE)

set(prefix_path "${THIRD_PARTY_PATH}/install/snappy|${THIRD_PARTY_PATH}/install/lz4|${THIRD_PARTY_PATH}/install/zstd|${THIRD_PARTY_PATH}/install/zlib|${THIRD_PARTY_PATH}/install/gflags|${THIRD_PARTY_PATH}/install/liburing")

#FILE(WRITE ${ROCKSDB_SOURCES_DIR}/src/build.sh
#        "PORTABLE=1 make -j${NUM_OF_PROCESSOR} static_lib"
#        )

ExternalProject_Add(
        extern_rocksdb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS gflags zlib snappy zstd lz4 liburing
        PREFIX ${ROCKSDB_SOURCES_DIR}
        URL "https://github.com/facebook/rocksdb/archive/v7.10.2.tar.gz"
        UPDATE_COMMAND ""
#        CONFIGURE_COMMAND ""
#        BUILD_IN_SOURCE 1
#        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
#        INSTALL_COMMAND mkdir -p ${ROCKSDB_INSTALL_DIR}/lib COMMAND cp -r include ${ROCKSDB_INSTALL_DIR}/ COMMAND cp librocksdb.a ${ROCKSDB_LIBRARIES}
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
        -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
        -DCMAKE_INSTALL_PREFIX=${ROCKSDB_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DPORTABLE=ON
        -DWITH_SNAPPY=ON
        -DWITH_ZSTD=ON
        -DWITH_LZ4=ON
        -DWITH_RUNTIME_DEBUG=ON
        -DROCKSDB_BUILD_SHARED=OFF
        -DWITH_BENCHMARK_TOOLS=OFF
        -DWITH_CORE_TOOLS=OFF
        -DWITH_TOOLS=OFF
        -DUSE_RTTI=ON
	-DWITH_LIBURING=ON
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${ROCKSDB_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_DEPENDENCIES(extern_rocksdb zlib snappy zstd lz4 gflags liburing)
ADD_LIBRARY(rocksdb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${ROCKSDB_LIBRARIES})
ADD_DEPENDENCIES(rocksdb extern_rocksdb)
