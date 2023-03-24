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

SET(MARIADB_SOURCES_DIR ${THIRD_PARTY_PATH}/mariadb)
SET(MARIADB_DOWNLOAD_DIR ${MARIADB_SOURCES_DIR}/src/extern_mariadb)
SET(MARIADB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/mariadb)
SET(MARIADB_INCLUDE_DIR "${MARIADB_INSTALL_DIR}/include/mariadb" CACHE PATH "mariadb include directory." FORCE)
SET(MARIADB_LIBRARIES "${MARIADB_INSTALL_DIR}/lib/mariadb/libmariadbclient.a" CACHE FILEPATH "mariadb library." FORCE)

set(prefix_path "${THIRD_PARTY_PATH}/install/zlib")

ExternalProject_Add(
        extern_mariadb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS zlib
        PREFIX ${MARIADB_SOURCES_DIR}
        GIT_REPOSITORY https://github.com/MariaDB/mariadb-connector-c.git
        GIT_TAG "v3.2.0"
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
        -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
        -DBUILD_STATIC_LIBS=ON
        -DCMAKE_INSTALL_PREFIX=${MARIADB_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DWITH_MYSQLCOMPAT=OFF
        -DWITH_EXTERNAL_ZLIB=ON
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${MARIADB_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_DEPENDENCIES(extern_mariadb zlib)
ADD_LIBRARY(mariadb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET mariadb PROPERTY IMPORTED_LOCATION ${MARIADB_LIBRARIES})
ADD_DEPENDENCIES(mariadb extern_mariadb)
