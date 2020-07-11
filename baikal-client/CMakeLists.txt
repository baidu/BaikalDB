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

#mysqlclient
if (WITH_SYSTEM_LIBS)
    find_path(MARIADB_INCLUDE_DIR NAMES mariadb/mysql.h)
    if (MARIADB_INCLUDE_DIR)
        set(MARIADB_INCLUDE_DIR ${MARIADB_INCLUDE_DIR}/mariadb)
    else ()
        find_path(MARIADB_INCLUDE_DIR NAMES mysql.h)
    endif ()
    find_library(MARIADB_LIBRARIES NAMES mariadbclient)
    if ((NOT MARIADB_INCLUDE_DIR) OR (NOT MARIADB_LIBRARIES))
        message(FATAL_ERROR "Fail to find mariadbclient")
    endif ()
else ()
    include(libmariadb)
endif ()
message(mysqlclient: ${MARIADB_INCLUDE_DIR}, ${MARIADB_LIBRARIES})

message(CUR_DIR : ${CMAKE_CURRENT_BINARY_DIR}, SRC_DIR : ${CMAKE_SOURCE_DIR})

set(DEP_INC
        ${DEP_INC}
        ${MARIADB_INCLUDE_DIR}
        )

set(DEP_LIB
        ${MARIADB_LIBRARIES}
        ${DEP_LIB}
        )

message("DEP INCLUDE: ${DEP_INC}")
message("DEP LIBRARY: ${DEP_LIB}")

file(GLOB BAIKAL_CLIENT
        src/*.h
        src/*.hpp
        src/*.cpp
        )

# for *.so
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/lib)
# for *.a
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/lib)

add_library(baikal_client ${BAIKAL_CLIENT})
if (NOT WITH_SYSTEM_LIBS)
    add_dependencies(baikal_client boost brpc mariadb)
endif ()
include_directories(${DEP_INC} include)
target_link_libraries(baikal_client ${DEP_LIB})

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/include)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/lib)
file(COPY ${CMAKE_SOURCE_DIR}/include/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/output/include/
        FILES_MATCHING PATTERN "*.h")

#TODO.. install
