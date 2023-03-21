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

cmake_minimum_required(VERSION 3.2)
project(BaikalDB C CXX)

option(WITH_BAIKAL_CLIENT "build baikal-client" ON)
option(DEBUG "Print debug logs" OFF)
option(WITH_SYSTEM_LIBS "use system installed headers and libraries instead of auto dependency" OFF)
option(WITH_DEBUG_SYMBOLS "With debug symbols" OFF)
option(WITH_TESTS "With tests" OFF)
option(WITH_GPERF "Link tcmalloc and profiler" ON)

message(STATUS "CXX compiler: ${CMAKE_CXX_COMPILER}, version: "
        "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C compiler: ${CMAKE_C_COMPILER}, version: "
        "${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")

SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

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
        LOG_DOWNLOAD 0
        LOG_UPDATE 1
        LOG_CONFIGURE 0
        LOG_BUILD 0
        LOG_TEST 1
        LOG_INSTALL 0)

include(ProcessorCount)
ProcessorCount(NUM_OF_PROCESSOR)
message(NUM_OF_PROCESSOR: ${NUM_OF_PROCESSOR})

#thread
include(FindThreads)

#openssl
find_package(OpenSSL REQUIRED)

message(STATUS "ssl:" ${OPENSSL_SSL_LIBRARY})
message(STATUS "crypto:" ${OPENSSL_CRYPTO_LIBRARY})

ADD_LIBRARY(ssl SHARED IMPORTED GLOBAL)
SET_PROPERTY(TARGET ssl PROPERTY IMPORTED_LOCATION ${OPENSSL_SSL_LIBRARY})

ADD_LIBRARY(crypto SHARED IMPORTED GLOBAL)
SET_PROPERTY(TARGET crypto PROPERTY IMPORTED_LOCATION ${OPENSSL_CRYPTO_LIBRARY})

#zlib
if (NOT WITH_SYSTEM_LIBS)
    include(zlib)
else ()
    ADD_LIBRARY(zlib SHARED IMPORTED GLOBAL)
    SET(ZLIB_LIBRARIES z)
endif ()

#bzip2
#include(bz2)

#boost
if (WITH_SYSTEM_LIBS)
    SET(Boost_NO_BOOST_CMAKE 1)
    SET(Boost_USE_MULTITHREADED ON)
    SET(Boost_USE_STATIC_LIBS ON)
    find_package(Boost 1.56.0 REQUIRED thread filesystem)
    ADD_LIBRARY(boost SHARED IMPORTED GLOBAL)
else ()
    include(boost)
endif ()
message(boost: ${Boost_VERSION}, ${Boost_LIB_VERSION}, DIRS: ${Boost_INCLUDE_DIRS}, LIBS: ${Boost_LIBRARIES})

#rapidjson
if (WITH_SYSTEM_LIBS)
    find_path(RAPIDJSON_INCLUDE_DIR NAMES rapidjson/rapidjson.h)
    if (NOT RAPIDJSON_INCLUDE_DIR)
        message(FATAL_ERROR "Fail to find rapidjson")
    endif ()
    ADD_LIBRARY(rapidjson SHARED IMPORTED GLOBAL)
else ()
    include(rapidjson)
endif ()

#apache arrow
if (WITH_SYSTEM_LIBS)
    find_path(ARROW_INCLUDE_DIR NAMES arrow/api.h)
    find_library(ARROW_LIBRARIES NAMES arrow)
    if ((NOT ARROW_INCLUDE_DIR) OR (NOT ARROW_LIBRARIES))
        message(FATAL_ERROR "Fail to find arrow")
    endif ()
    ADD_LIBRARY(arrow SHARED IMPORTED GLOBAL)
else ()
    include(arrow)
endif ()

#croaring
if (WITH_SYSTEM_LIBS)
    find_path(CROARING_INCLUDE_DIR NAMES roaring/roaring.h)
    find_library(CROARING_LIBRARIES NAMES croaring)
    if ((NOT CROARING_INCLUDE_DIR) OR (NOT CROARING_LIBRARIES))
        message(FATAL_ERROR "Fail to find croaring")
    endif ()
    ADD_LIBRARY(croaring SHARED IMPORTED GLOBAL)
else ()
    include(croaring)
endif ()

#gflags
if (WITH_SYSTEM_LIBS)
    find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h)
    find_library(GFLAGS_LIBRARIES NAMES gflags)
    if ((NOT GFLAGS_INCLUDE_DIR) OR (NOT GFLAGS_LIBRARIES))
        message(FATAL_ERROR "Fail to find gflags")
    endif ()
    ADD_LIBRARY(gflags SHARED IMPORTED GLOBAL)
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
    ADD_LIBRARY(glog SHARED IMPORTED GLOBAL)
else ()
    include(glog)
endif ()

#snappy
if (WITH_SYSTEM_LIBS)
    find_path(SNAPPY_INCLUDE_DIR NAMES snappy.h)
    find_library(SNAPPY_LIBRARIES NAMES snappy)
    if ((NOT SNAPPY_INCLUDE_DIR) OR (NOT SNAPPY_LIBRARIES))
        message(FATAL_ERROR "Fail to find snappy")
    endif ()
    ADD_LIBRARY(snappy SHARED IMPORTED GLOBAL)
else ()
    include(snappy)
endif ()

#re2
if (WITH_SYSTEM_LIBS)
    find_path(RE2_INCLUDE_DIR NAMES re2/re2.h)
    find_library(RE2_LIBRARIES NAMES re2)
    if ((NOT RE2_INCLUDE_DIR) OR (NOT RE2_LIBRARIES))
        message(FATAL_ERROR "Fail to find re2")
    endif ()
    ADD_LIBRARY(re2 SHARED IMPORTED GLOBAL)
else ()
    include(re2)
endif ()

#protobuf
include(protobuf)

#rocksdb
if (WITH_SYSTEM_LIBS)
    find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h)
    find_library(ROCKSDB_LIBRARIES NAMES rocksdb)
    if ((NOT ROCKSDB_INCLUDE_DIR) OR (NOT ROCKSDB_LIBRARIES))
        message(FATAL_ERROR "Fail to find rocksdb")
    endif ()
    ADD_LIBRARY(rocksdb SHARED IMPORTED GLOBAL)
else ()
    include(rocksdb)
endif ()

#brpc
if (WITH_SYSTEM_LIBS)
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
    include(leveldb)
    include(brpc)
endif ()
message(BRPC:${BRPC_INCLUDE_DIR}, ${BRPC_LIBRARIES})

#braft
if (WITH_SYSTEM_LIBS)
    find_path(BRAFT_INCLUDE_DIR NAMES braft/raft.h)
    find_library(BRAFT_LIBRARIES NAMES libbraft.a brpc)
    if ((NOT BRAFT_INCLUDE_DIR) OR (NOT BRAFT_LIBRARIES))
        message(FATAL_ERROR "Fail to find braft")
    endif ()
else ()
    include(braft)
endif ()
message(braft lib : ${BRAFT_LIBRARIES})

if (WITH_BAIKAL_CLIENT)
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
        ADD_LIBRARY(mariadb SHARED IMPORTED GLOBAL)
    else ()
        include(mariadb)
    endif ()
    message(mysqlclient: ${MARIADB_INCLUDE_DIR}, ${MARIADB_LIBRARIES})
endif ()

#tcmalloc
if (WITH_GPERF)
    include(FindGperftools)
    if (GPERFTOOLS_FOUND)
        add_library(gperf SHARED IMPORTED GLOBAL)
        set_property(TARGET gperf PROPERTY IMPORTED_LOCATION tcmalloc_and_profiler)
    else ()
        message("GPERFTOOLS NOT FOUND, downloading and compiling")
        include(gperftools)
    endif ()
endif ()

message(CUR_DIR : ${CMAKE_CURRENT_BINARY_DIR}, SRC_DIR : ${CMAKE_SOURCE_DIR})

file(GLOB PROTO_FILES ${CMAKE_SOURCE_DIR}/proto/*.proto)
message("protoc: ${PROTOBUF_PROTOC_EXECUTABLE}, proto inc: ${PROTOBUF_INCLUDE_DIRS}, lib: ${PROTOBUF_LIBRARIES}, ${PROTOBUF_PROTOC_LIBRARY}, protos: ${PROTO_FILES}")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
foreach (PROTO ${PROTO_FILES})
    message(proto : ${PROTO})
    get_filename_component(PROTO_WE ${PROTO} NAME)
    string(REPLACE ".proto" "" PROTO_WE ${PROTO_WE})
    list(APPEND PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.h")
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.cc")
    add_custom_command(
            OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.h ${CMAKE_CURRENT_BINARY_DIR}/proto/${PROTO_WE}.pb.cc
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/proto
            --proto_path=${PROTOBUF_INCLUDE_DIR}
            --proto_path=${CMAKE_SOURCE_DIR}/proto ${PROTO}
            DEPENDS protobuf
    )
endforeach ()
add_library(PROTO_OBJS OBJECT ${PROTO_SRCS} ${PROTO_HDRS})
message("protoc: ${PROTOBUF_PROTOC_EXECUTABLE}, proto srcs : ${PROTO_SRCS}")

#sql parser
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/sqlparser)
execute_process(
        COMMAND bash ${CMAKE_SOURCE_DIR}/include/sqlparser/gen_source.sh ${CMAKE_SOURCE_DIR} opensource ${CMAKE_CURRENT_BINARY_DIR}/sqlparser
        RESULT_VARIABLE CMD_RET
)
if (NOT CMD_RET EQUAL 0)
    message(FATAL_ERROR "failed to generate expression, please install flex and bison first")
endif ()

SET(DEP_INC
        ${CMAKE_CURRENT_BINARY_DIR}/proto

        ${OPENSSL_INCLUDE_DIR}
        ${ZLIB_INCLUDE_DIR}
        #        ${BZ2_INCLUDE_DIR}

        ${Boost_INCLUDE_DIR}
        ${ARROW_INCLUDE_DIR}
        ${CROARING_INCLUDE_DIR}
        ${RAPIDJSON_INCLUDE_DIR}

        ${PROTOBUF_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
        ${GLOG_INCLUDE_DIR}
        ${SNAPPY_INCLUDE_DIR}
        ${RE2_INCLUDE_DIR}

        ${ROCKSDB_INCLUDE_DIR}

        ${BRPC_INCLUDE_DIR}
        ${BRAFT_INCLUDE_DIR}

        ${GPERFTOOLS_INCLUDE_DIR}
        )

SET(DEP_LIB
        ${BRAFT_LIBRARIES}
        ${BRPC_LIBRARIES}

        ${LEVELDB_LIBRARIES}
        ${ROCKSDB_LIBRARIES}
        )

if (WITH_BAIKAL_CLIENT)
    SET(DEP_LIB ${DEP_LIB} ${MARIADB_LIBRARIES})
    SET(DEP_INC ${DEP_INC} ${MARIADB_INCLUDE_DIR})
endif ()

SET(DEP_LIB
        ${DEP_LIB}
        ${PROTOBUF_LIBRARIES}
        ${PROTOBUF_PROTOC_LIBRARY}
        ${GFLAGS_LIBRARIES}
        ${GLOG_LIBRARIES}
        ${SNAPPY_LIBRARIES}
        ${RE2_LIBRARIES}

        ${ARROW_LIBRARIES}
        ${CROARING_LIBRARIES}
        ${Boost_LIBRARIES}
        #        ${BZ2_LIBRARIES}
        )

if (WITH_GPERF)
    SET(DEP_LIB ${DEP_LIB} ${GPERFTOOLS_LIBRARIES})
endif ()

SET(DEP_LIB
        ${DEP_LIB}
        ${ZLIB_LIBRARIES}
        ${OPENSSL_SSL_LIBRARY}
        ${OPENSSL_CRYPTO_LIBRARY}
        dl
        rt
        m
        ${CMAKE_THREAD_LIBS_INIT}
        )

message("DEP INCLUDES: ${DEP_INC}")
message("DEP LIBRARYS: ${DEP_LIB}")

if (WITH_DEBUG_SYMBOLS)
    SET(DEBUG_SYMBOL "-ggdb")
endif ()

if (NOT DEBUG)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DNDEBUG")
endif ()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 ${DEBUG_SYMBOL} -O2 -pipe -m64 -Wall -W -fPIC -Wno-unused-parameter -Wno-strict-aliasing -Wno-parentheses -fno-omit-frame-pointer ")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 ${DEBUG_SYMBOL} -O2 -pipe -m64 -Wall -W -fPIC -Wno-unused-parameter -Wno-strict-aliasing -Wno-parentheses -fno-omit-frame-pointer")

add_definitions(
        -D_GNU_SOURCE
        -D__STDC_FORMAT_MACROS
        -D__STDC_LIMIT_MACROS
        -D__STDC_CONSTANT_MACROS
        -DBRPC_WITH_GLOG=1
)

file(GLOB ENGINE src/engine/*.cpp)
file(GLOB EXEC src/exec/*.cpp)
file(GLOB EXPR src/expr/*.cpp)
file(GLOB LOGICAL_PLAN src/logical_plan/*.cpp)
file(GLOB MEM_ROW src/mem_row/*.cpp)
file(GLOB PHYSICAL_PLAN src/physical_plan/*.cpp)
file(GLOB PROTOCOL src/protocol/*.cpp)
list(REMOVE_ITEM PROTOCOL src/protocol/main.cpp)
file(GLOB RAFT src/raft/*.cpp)
file(GLOB RAFT_META src/raft_meta/*.cpp)
file(GLOB REVERSE src/reverse/*.cpp)
file(GLOB RUNTIME src/runtime/*.cpp)
file(GLOB SESSION src/session/*.cpp)
file(GLOB SQLPARSER include/sqlparser/*.cc ${CMAKE_CURRENT_BINARY_DIR}/sqlparser/*.cc)
file(GLOB STORE src/store/*.cpp)
list(REMOVE_ITEM STORE src/store/main.cpp)

set(COMMON_INC
        ${DEP_INC}
        ${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}/proto
        ${CMAKE_CURRENT_BINARY_DIR}/sqlparser
        include/common
        include/engine
        include/exec
        include/expr
        include/logical_plan
        include/mem_row
        include/physical_plan
        include/protocol
        include/raft
        include/reverse
        include/reverse/boolean_engine
        include/runtime
        include/session
        include/sqlparser
        include/store
        )

file(GLOB COMMON
        ${CMAKE_CURRENT_BINARY_DIR}/proto/*.pb.*
        src/common/*.cpp
        ${ENGINE}
        ${EXEC}
        ${EXPR}
        ${LOGICAL_PLAN}
        ${MEM_ROW}
        ${PHYSICAL_PLAN}
        ${PROTOCOL}
        ${RAFT}
        ${RAFT_META}
        ${REVERSE}
        ${RUNTIME}
        ${SESSION}
        ${SQLPARSER}
        ${STORE}
        )

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/bin)

set(COMMON_DEPS ssl crypto zlib boost rapidjson arrow croaring glog gflags protobuf re2 snappy rocksdb brpc braft)
if (WITH_GPERF)
    set(COMMON_DEPS ${COMMON_DEPS} gperf)
endif ()

add_library(common STATIC ${COMMON} $<TARGET_OBJECTS:PROTO_OBJS>)
add_dependencies(common ${COMMON_DEPS})
target_include_directories(common PUBLIC ${COMMON_INC})

# baikalMeta
file(GLOB META_SERVER src/meta_server/*.cpp)
set(BAIKALMETA_INC ${COMMON_INC} include/meta_server)
add_executable(baikalMeta ${META_SERVER} ${RAFT_META})
add_dependencies(baikalMeta common)
target_include_directories(baikalMeta PUBLIC ${BAIKALMETA_INC})
target_link_libraries(baikalMeta PUBLIC common ${DEP_LIB})

# baikalStore
file(GLOB RAFT_STORE src/raft_store/*.cpp)
set(BAIKALSTORE_INC ${COMMON_INC})
add_executable(baikalStore src/store/main.cpp ${RAFT_STORE})
add_dependencies(baikalStore common)
target_include_directories(baikalStore PUBLIC ${BAIKALSTORE_INC})
target_link_libraries(baikalStore PUBLIC common ${DEP_LIB})

# baikaldb
file(GLOB RAFT_DUMMPY src/raft_dummy/*.cpp)
set(BAIKALDB_INC ${COMMON_INC})
add_executable(baikaldb src/protocol/main.cpp ${RAFT_DUMMPY})
add_dependencies(baikaldb common)
target_include_directories(baikaldb PUBLIC ${BAIKALDB_INC})
target_link_libraries(baikaldb PUBLIC common ${DEP_LIB})

if (WITH_BAIKAL_CLIENT)
    add_subdirectory(baikal-client)
endif ()
