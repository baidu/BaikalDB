cmake_minimum_required(VERSION 3.0)
project(baikal_client C CXX)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

#openssl
include(FindOpenSSL)

#boost
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.56.0 REQUIRED filesystem regex system thread)
message(boost: ${Boost_VERSION}, ${Boost_LIB_VERSION}, DIRS: ${Boost_INCLUDE_DIRS}, LIBS: ${Boost_LIBRARIES})

#mysqlclient
find_path(MYSQL_INCLUDE_PATH NAMES mysql.h)
if (NOT MYSQL_INCLUDE_PATH)
    message(FATAL_ERROR "Fail to find mysql")
endif ()
find_library(MYSQL_LIB NAMES mariadbclient)
if (NOT MYSQL_LIB)
    message(FATAL_ERROR "Fail to find mysql")
endif ()

#gflags
find_path(GFLAGS_INCLUDE_PATH NAMES gflags/gflags.h)
find_library(GFLAGS_LIB NAMES gflags)
if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIB))
    message(FATAL_ERROR "Fail to find gflags")
endif ()

find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
find_library(GLOG_LIB NAMES glog)
if ((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIB))
    message(FATAL_ERROR "Fail to find glog")
endif ()

#brpc
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif ()
message(brpc lib : ${BRPC_LIB})

#tcmalloc
#find_library(TCMALLOC_LIB NAMES tcmalloc)
#if ((NOT TCMALLOC_LIB))
#    message(FATAL_ERROR "Fail to find tcmalloc")
#endif ()

message(CUR_DIR : ${CMAKE_CURRENT_BINARY_DIR}, SRC_DIR : ${CMAKE_SOURCE_DIR})

set(DEP_INC
        ${CMAKE_CURRENT_BINARY_DIR}/proto

        ${Boost_INCLUDE_DIRS}
        ${OPENSSL_INCLUDE_DIR}

        ${GLOG_INCLUDE_PATH}
        ${BRPC_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH}

#        ${TCMALLOC_INCLUDE_PATH}
        ${MYSQL_INCLUDE_PATH}
        )

set(DEP_LIB
        ${CMAKE_THREAD_LIBS_INIT}

        ${Boost_LIBRARIES}
        ${OPENSSL_LIBRARIES}

        ${GLOG_LIB}
        ${BRPC_LIB}
        ${GFLAGS_LIB}

#        ${TCMALLOC_LIB}

        ${MYSQL_LIB}

        z
        dl
        pthread
        )

message("dep inc : ${DEP_INC}")
message("dep lib : ${DEP_LIB}")

include_directories(
        ${DEP_INC}
        include
)
link_libraries(
        ${DEP_LIB}
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -ggdb -pipe -m64 -Wall -W -fPIC -Wno-unused-parameter -Wno-bool-compare")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -ggdb -pipe -m64 -Wall -W -fPIC -Wno-unused-parameter -Wno-bool-compare")

macro(use_cxx11)
    if(CMAKE_VERSION VERSION_LESS "3.1.3")
        if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
        endif()
        if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
        endif()
    else()
        set(CMAKE_CXX_STANDARD 11)
        set(CMAKE_CXX_STANDARD_REQUIRED ON)
    endif()
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

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    #required by butil/crc32.cc to boost performance for 10x
    if (NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.4))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4 -msse4.2")
    endif ()
    if (NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-aligned-new")
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

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/include)
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/lib)
file(COPY ${CMAKE_SOURCE_DIR}/include/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/output/include/
        FILES_MATCHING
        PATTERN "*.h"
        )