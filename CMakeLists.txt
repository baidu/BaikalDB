cmake_minimum_required(VERSION 3.0)
project(BaikalDB CXX)

#thread
include(FindThreads)

#openssl
include(FindOpenSSL)

#boost
include(FindBoost)

#rapidjson
find_path(RAPIDJSON_INCLUDE_PATH NAMES rapidjson/rapidjson.h)
find_library(RAPIDJSON_LIB NAMES glog)
if ((NOT RAPIDJSON_INCLUDE_PATH) OR (NOT RAPIDJSON_LIB))
    message(FATAL_ERROR "Fail to find rapidjson")
endif ()

#GLOG
find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
find_library(GLOG_LIB NAMES glog)
if ((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIB))
    message(FATAL_ERROR "Fail to find glog")
endif ()

#GFLAGS
find_path(GFLAGS_INCLUDE_PATH NAMES gflags/gflags.h)
find_library(GFLAGS_LIB NAMES gflags)
if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIB))
    message(FATAL_ERROR "Fail to find gflags")
endif ()

#level
find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif ()

#rocksdb
find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
find_library(ROCKSDB_LIB NAMES rocksdb)
if ((NOT ROCKSDB_INCLUDE_PATH) OR (NOT ROCKSDB_LIB))
    message(FATAL_ERROR "Fail to find rocksdb")
endif ()

#brpc
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif ()
message(brpc lib : ${BRPC_LIB})

#braft
find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
find_library(BRAFT_LIB NAMES libbraft.a brpc)
if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
    message(FATAL_ERROR "Fail to find braft")
endif ()
message(braft lib : ${BRAFT_LIB})

#tcmalloc
find_library(TCMALLOC_LIB NAMES tcmalloc)
if ((NOT TCMALLOC_LIB))
    message(FATAL_ERROR "Fail to find tcmalloc")
endif ()

message(CUR_DIR : ${CMAKE_CURRENT_BINARY_DIR}, SRC_DIR : ${CMAKE_SOURCE_DIR})

#protobuf
find_package(Protobuf REQUIRED)
file(GLOB PROTO_FILES "${CMAKE_SOURCE_DIR}/proto/*.proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
foreach (PROTO ${PROTO_FILES})
    message(proto : ${PROTO})
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_WE}.pb.cc")
    execute_process(
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR}/proto
            --proto_path=${PROTOBUF_INCLUDE_DIR}
            --proto_path=${CMAKE_SOURCE_DIR}/proto ${PROTO}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            ERROR_VARIABLE PROTO_ERROR
            RESULT_VARIABLE PROTO_RESULT
    )
    if (${PROTO_RESULT} EQUAL 0)
    else ()
        message(FATAL_ERROR "Fail to generate cpp of ${PROTO} : ${PROTO_ERROR}")
    endif ()
endforeach ()
message(proto srcs : ${PROTO_SRCS})


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer")

add_definitions(
        -DBRPC_WITH_GLOG=1
)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 11)

include_directories(
        ${RAPIDJSON_INCLUDE_PATH}
        ${GLOG_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH}
        #${LEVELDB_INCLUDE_PATH}
        ${ROCKSDB_INCLUDE_PATH}
        ${OPENSSL_INCLUDE_DIR}
        ${Boost_INCLUDE_DIRS}
        ${PROTOBUF_INCLUDE_DIR}
        ${BRPC_INCLUDE_PATH}
        ${BRAFT_INCLUDE_PATH}

        ${CMAKE_CURRENT_BINARY_DIR}
)

link_libraries(
        z
        dl
        rt
        ssl
        crypto
        pthread

        ${RAPIDJSON_LIB}
        ${GLOG_LIB}
        ${GFLAGS_LIB}
        #        ${LEVELDB_LIB}
        ${ROCKSDB_LIB}
        ${PROTOC_LIB}
        ${CMAKE_THREAD_LIBS_INIT}
        ${OPENSSL_LIBRARIES}
        ${OPENSSL_CRYPTO_LIBRARY}
        ${Boost_LIBRARY_DIRS}
        ${PROTOBUF_LIBRARY}
        ${PROTOBUF_PROTOC_LIBRARY}
        ${BRPC_LIB}
        ${BRAFT_LIB}
        ${TCMALLOC_LIB}
)

include_directories(
        include
        include/common
        include/engine
        include/exec
        include/expr
        include/mem_row
        include/meta_server
        include/new_logical_plan
        include/physical_plan
        include/protocol
        include/raft
        include/reverse
        include/reverse/boolean_engine
        include/runtime
        include/session
        include/sqlparser
        include/store

        third-party/snappy_config
)

file(GLOB COMMON_SRC
        ${PROTO_SRCS}
        proto/*.pb.*
        src/common/*.cpp
        )

file(GLOB RAFT_SRC
        src/raft/*.cpp
        )

file(GLOB RAFT_META_SRC
        src/raft_meta/*.cpp
        )

file(GLOB META_SERVER_SRC
        src/meta_server/*.cpp
        )

file(GLOB src
        include/*/*.h
        include/*/*.hpp
        src/*/*.cpp

        include/reverse/boolean_engine/*.h
        include/reverse/boolean_engine/*.hpp

        third-party/snappy_config/*.h
        )

add_executable(baikaldb
        ${COMMON_SRC}
        ${RAFT_SRC}
        ${RAFT_META_SRC}
        ${META_SERVER_SRC}
        )
#add_executable(baikalmeta ${src})
#add_executable(baikalstore ${src})