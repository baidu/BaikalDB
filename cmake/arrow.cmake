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

SET(ARROW_SOURCES_DIR ${THIRD_PARTY_PATH}/arrow)
SET(ARROW_INSTALL_DIR ${THIRD_PARTY_PATH}/install/arrow)
SET(ARROW_INCLUDE_DIR "${ARROW_INSTALL_DIR}/include" CACHE PATH "arrow include directory." FORCE)
SET(ARROW_LIBRARIES "${ARROW_INSTALL_DIR}/lib/libarrow.a" CACHE FILEPATH "arrow library." FORCE)
SET(ARROW_PARQUET_LIB "${ARROW_INSTALL_DIR}/lib/libparquet.a" CACHE FILEPATH "arrow parquet." FORCE)
SET(ARROW_ACERO_LIB "${ARROW_INSTALL_DIR}/lib/libarrow_acero.a" CACHE FILEPATH "arrow acero." FORCE)
SET(ARROW_BUNDLED_DEP_LIB "${ARROW_INSTALL_DIR}/lib/libarrow_bundled_dependencies.a" CACHE FILEPATH "arrow dependencies." FORCE)

FILE(WRITE ${ARROW_SOURCES_DIR}/src/build.sh
        "cd cpp && cmake -DCMAKE_BUILD_TYPE=release -DARROW_JEMALLOC=OFF -DARROW_BUILD_SHARED=OFF -DARROW_PARQUET=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_COMPUTE=ON -DARROW_ACERO=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON -DARROW_PARQUET=ON -DARROW_BUILD_TESTS=OFF -DARROW_BUILD_STATIC=ON && make -j${NUM_OF_PROCESSOR}"
        )

ExternalProject_Add(
        extern_arrow
        ${EXTERNAL_PROJECT_LOG_ARGS}
#        GIT_REPOSITORY "https://github.com/apache/arrow.git"
#        GIT_TAG "apache-arrow-0.17.1"
        URL "https://github.com/baikalgroup/arrow/archive/refs/tags/16.1.0.tar.gz"
        PREFIX ${ARROW_SOURCES_DIR}
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
        INSTALL_COMMAND
            mkdir -p ${ARROW_INSTALL_DIR}/lib/
            COMMAND cp ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/build/release/libarrow.a ${ARROW_LIBRARIES}
            COMMAND cp ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/build/release/libparquet.a ${ARROW_INSTALL_DIR}/lib/
            COMMAND cp ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/build/release/libarrow_acero.a ${ARROW_INSTALL_DIR}/lib/
            COMMAND cp ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/build/release/libarrow_bundled_dependencies.a ${ARROW_INSTALL_DIR}/lib/
            COMMAND mkdir -p ${ARROW_INCLUDE_DIR}
            COMMAND cp -r ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/src/arrow ${ARROW_INCLUDE_DIR}/
            COMMAND cp -r ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/src/parquet ${ARROW_INCLUDE_DIR}/
)

ADD_DEPENDENCIES(extern_arrow zlib snappy zstd lz4 re2 protobuf rapidjson)
ADD_LIBRARY(arrow STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET arrow PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARIES})
ADD_LIBRARY(parquet STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET parquet PROPERTY IMPORTED_LOCATION ${ARROW_PARQUET_LIB})
ADD_LIBRARY(acero STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET acero PROPERTY IMPORTED_LOCATION ${ARROW_ACERO_LIB})
ADD_LIBRARY(arrow_deps STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET arrow_deps PROPERTY IMPORTED_LOCATION ${ARROW_BUNDLED_DEP_LIB})
ADD_DEPENDENCIES(arrow parquet acero arrow_deps extern_arrow)
