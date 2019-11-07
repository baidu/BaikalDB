# Copyright (c) 2016 Baidu Authors. All Rights Reserved.
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

SET(MARIADB_SOURCES_DIR ${THIRD_PARTY_PATH}/mariadb)
SET(MARIADB_DOWNLOAD_DIR ${MARIADB_SOURCES_DIR}/src/extern_mariadb)
SET(MARIADB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/mariadb)
SET(MARIADB_INCLUDE_DIR "${MARIADB_INSTALL_DIR}/include/mariadb" CACHE PATH "mariadb include directory." FORCE)
SET(MARIADB_LIBRARIES "${MARIADB_INSTALL_DIR}/lib/mariadb/libmariadb.a" CACHE FILEPATH "mariadb library." FORCE)
INCLUDE_DIRECTORIES(${MARIADB_INCLUDE_DIR})

FILE(WRITE ${MARIADB_DOWNLOAD_DIR}/CMakeLists.txt
        "PROJECT(MARIADB)\n"
        "cmake_minimum_required(VERSION 3.0)\n"
        "install(DIRECTORY mariadb-connector-c-2.3.1-linux-x86_64/include mariadb-connector-c-2.3.1-linux-x86_64/lib \n"
        "        DESTINATION mariadb)\n")

ExternalProject_Add(
        extern_mariadb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${MARIADB_SOURCES_DIR}
        DOWNLOAD_DIR ${MARIADB_DOWNLOAD_DIR}
        DOWNLOAD_NO_PROGRESS 1
        DOWNLOAD_COMMAND wget --no-check-certificate https://downloads.mariadb.com/Connectors/c/connector-c-2.3.1/mariadb-connector-c-2.3.1-linux-x86_64.tar.gz -O mc.tar.gz COMMAND tar -zxf mc.tar.gz
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${THIRD_PARTY_PATH}/install
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${THIRD_PARTY_PATH}/install
)

ADD_LIBRARY(mariadb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET mariadb PROPERTY IMPORTED_LOCATION ${MARIADB_LIBRARIES})
ADD_DEPENDENCIES(mariadb extern_mariadb)
