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

option(WITH_SYSTEM_LIBS "use system installed headers and libraries instead of auto dependency" OFF)
option(WITH_DEBUG_SYMBOLS "With debug symbols" OFF)

message(STATUS "CXX compiler: ${CMAKE_CXX_COMPILER}, version: "
        "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C compiler: ${CMAKE_C_COMPILER}, version: "
        "${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
else ()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif ()

SET(THIRD_PARTY_PATH ${CMAKE_CURRENT_BINARY_DIR}/third-party)
SET(THIRD_PARTY_BUILD_TYPE Release)
SET(EXTERNAL_PROJECT_LOG_ARGS
        LOG_DOWNLOAD 1
        LOG_UPDATE 1
        LOG_CONFIGURE 1
        LOG_BUILD 1
        LOG_TEST 1
        LOG_INSTALL 1)

include(ProcessorCount)
ProcessorCount(NUM_OF_PROCESSOR)
message(NUM_OF_PROCESSOR: ${NUM_OF_PROCESSOR})

#openssl
include(FindOpenSSL)

#zlib
if (NOT WITH_SYSTEM_LIBS)
    include(zlib)
else()
    set(ZLIB_LIBRARIES z)
endif()

#boost
if (WITH_SYSTEM_LIBS)
    set(Boost_NO_BOOST_CMAKE 1)
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_LIBS ON)
    find_package(Boost 1.56.0 REQUIRED thread)
else ()
    include(boost)
endif ()
message(boost: ${Boost_VERSION}, ${Boost_LIB_VERSION}, DIRS: ${Boost_INCLUDE_DIRS}, LIBS: ${Boost_LIBRARIES})

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

#gflags
if (WITH_SYSTEM_LIBS)
    find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h)
    find_library(GFLAGS_LIBRARIES NAMES gflags)
    if ((NOT GFLAGS_INCLUDE_DIR) OR (NOT GFLAGS_LIBRARIES))
        message(FATAL_ERROR "Fail to find gflags")
    endif ()
else ()
    include(gflags)
endif ()

#glog
if (WITH_SYSTEM_LIBS)
    find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h)
    find_library(GLOG_LIBRARIES NAMES glog)
    if ((NOT GLOG_INCLUDE_DIR) OR (NOT GLOG_LIBRARIES))
        message(FATAL_ERROR "Fail to find glog")
    endif ()
else ()
    include(glog)
endif ()

#brpc
if (WITH_SYSTEM_LIBS)
    #protobuf(for brpc)
    include(FindProtobuf)

    #leveldb(for brpc)
    find_library(LEVELDB_LIBRARIES NAMES leveldb)
    if (NOT LEVELDB_LIBRARIES)
        message(FATAL_ERROR "Fail to find leveldb")
    endif ()

    find_path(BRPC_INCLUDE_DIR NAMES brpc/server.h)
    find_library(BRPC_LIBRARIES NAMES libbrpc.a brpc)
    if ((NOT BRPC_INCLUDE_DIR) OR (NOT BRPC_LIBRARIES))
        message(FATAL_ERROR "Fail to find brpc")
    endif ()
else ()
    include(protobuf)
    include(leveldb)
    include(brpc)
endif ()
message(BRPC:${BRPC_INCLUDE_DIR}, ${BRPC_LIBRARIES})

#tcmalloc
#find_library(TCMALLOC_LIB NAMES tcmalloc)
#if ((NOT TCMALLOC_LIB))
#    message(FATAL_ERROR "Fail to find tcmalloc")
#endif ()

message(CUR_DIR : ${CMAKE_CURRENT_BINARY_DIR}, SRC_DIR : ${CMAKE_SOURCE_DIR})

set(DEP_INC
        ${OPENSSL_INCLUDE_DIR}
        ${ZLIB_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_DIR}
        ${GLOG_INCLUDE_DIR}
        ${BRPC_INCLUDE_DIR}
        ${MARIADB_INCLUDE_DIR}
        #${TCMALLOC_INCLUDE_PATH}
        )

set(DEP_LIB
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${Boost_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${GLOG_LIBRARIES}
        ${LEVELDB_LIBRARIES}
        ${BRPC_LIBRARIES}
        ${MARIADB_LIBRARIES}
        #${TCMALLOC_LIB}
        dl
        pthread
        )

message("DEP INCLUDE: ${DEP_INC}")
message("DEP LIBRARY: ${DEP_LIB}")

if(WITH_DEBUG_SYMBOLS)
    set(DEBUG_SYMBOL "-ggdb")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 ${DEBUG_SYMBOL} -O2 -pipe -m64 -Wall -W -fPIC -Wno-unused-parameter")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 ${DEBUG_SYMBOL} -O2 -pipe -m64 -Wall -W -fPIC -Wno-unused-parameter")

macro(use_cxx11)
    if (CMAKE_VERSION VERSION_LESS "3.1.3")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
        endif ()
        if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
        endif ()
    else ()
        set(CMAKE_CXX_STANDARD 11)
        set(CMAKE_CXX_STANDARD_REQUIRED ON)
    endif ()
endmacro(use_cxx11)

use_cxx11()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(DEP_LIB ${DEP_LIB} rt)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DEP_LIB ${DEP_LIB}
            "-framework CoreFoundation"
            "-framework CoreGraphics"
            "-framework CoreData"
            "-framework CoreText"
            "-framework Security"
            "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
            "-Wl,-U,_ProfilerStart"
            "-Wl,-U,_ProfilerStop")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
    if (NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif ()
endif ()

add_definitions(
        ${DEFINE_CLOCK_GETTIME}
        -D_GNU_SOURCE
        -D__STDC_FORMAT_MACROS
        -D__STDC_LIMIT_MACROS
        -D__STDC_CONSTANT_MACROS
        -DBRPC_WITH_GLOG=1
)

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