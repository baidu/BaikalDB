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
# Always invoke `FIND_PACKAGE(Protobuf)` for importing function protobuf_generate_cpp
FIND_PACKAGE(Protobuf QUIET)
macro(UNSET_VAR VAR_NAME)
    UNSET(${VAR_NAME} CACHE)
    UNSET(${VAR_NAME})
endmacro()

UNSET_VAR(PROTOBUF_INCLUDE_DIR)
UNSET_VAR(PROTOBUF_FOUND)
UNSET_VAR(PROTOBUF_PROTOC_EXECUTABLE)
UNSET_VAR(PROTOBUF_PROTOC_LIBRARY)
UNSET_VAR(PROTOBUF_LITE_LIBRARY)
UNSET_VAR(PROTOBUF_LIBRARY)
UNSET_VAR(PROTOBUF_INCLUDE_DIR)
UNSET_VAR(Protobuf_PROTOC_EXECUTABLE)

# Print and set the protobuf library information,
# finish this cmake process and exit from this file.
macro(PROMPT_PROTOBUF_LIB)
    SET(protobuf_DEPS ${ARGN})

    MESSAGE(STATUS "Protobuf protoc executable: ${PROTOBUF_PROTOC_EXECUTABLE}")
    MESSAGE(STATUS "Protobuf-lite library: ${PROTOBUF_LITE_LIBRARY}")
    MESSAGE(STATUS "Protobuf library: ${PROTOBUF_LIBRARY}")
    MESSAGE(STATUS "Protoc library: ${PROTOBUF_PROTOC_LIBRARY}")
    MESSAGE(STATUS "Protobuf version: ${PROTOBUF_VERSION}")
    INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})

    # Assuming that all the protobuf libraries are of the same type.
    IF (${PROTOBUF_LIBRARY} MATCHES ${CMAKE_STATIC_LIBRARY_SUFFIX})
        SET(protobuf_LIBTYPE STATIC)
    ELSEIF (${PROTOBUF_LIBRARY} MATCHES "${CMAKE_SHARED_LIBRARY_SUFFIX}$")
        SET(protobuf_LIBTYPE SHARED)
    ELSE ()
        MESSAGE(FATAL_ERROR "Unknown library type: ${PROTOBUF_LIBRARY}")
    ENDIF ()

    ADD_LIBRARY(protobuf ${protobuf_LIBTYPE} IMPORTED GLOBAL)
    SET_PROPERTY(TARGET protobuf PROPERTY IMPORTED_LOCATION ${PROTOBUF_LIBRARY})

    ADD_LIBRARY(protobuf_lite ${protobuf_LIBTYPE} IMPORTED GLOBAL)
    SET_PROPERTY(TARGET protobuf_lite PROPERTY IMPORTED_LOCATION ${PROTOBUF_LITE_LIBRARY})

    ADD_LIBRARY(libprotoc ${protobuf_LIBTYPE} IMPORTED GLOBAL)
    SET_PROPERTY(TARGET libprotoc PROPERTY IMPORTED_LOCATION ${PROTOC_LIBRARY})

    ADD_EXECUTABLE(protoc IMPORTED GLOBAL)
    SET_PROPERTY(TARGET protoc PROPERTY IMPORTED_LOCATION ${PROTOBUF_PROTOC_EXECUTABLE})
    SET(Protobuf_PROTOC_EXECUTABLE ${PROTOBUF_PROTOC_EXECUTABLE})

    FOREACH (dep ${protobuf_DEPS})
        ADD_DEPENDENCIES(protobuf ${dep})
        ADD_DEPENDENCIES(protobuf_lite ${dep})
        ADD_DEPENDENCIES(libprotoc ${dep})
        ADD_DEPENDENCIES(protoc ${dep})
    ENDFOREACH ()

    RETURN()
endmacro()
macro(SET_PROTOBUF_VERSION)
    EXEC_PROGRAM(${PROTOBUF_PROTOC_EXECUTABLE} ARGS --version OUTPUT_VARIABLE PROTOBUF_VERSION)
    STRING(REGEX MATCH "[0-9]+.[0-9]+" PROTOBUF_VERSION "${PROTOBUF_VERSION}")
endmacro()

set(PROTOBUF_ROOT "" CACHE PATH "Folder contains protobuf")

if (NOT "${PROTOBUF_ROOT}" STREQUAL "")
    message("found system protobuf")

    find_path(PROTOBUF_INCLUDE_DIR google/protobuf/message.h PATHS ${PROTOBUF_ROOT}/include NO_DEFAULT_PATH)
    find_library(PROTOBUF_LIBRARY protobuf libprotobuf.lib PATHS ${PROTOBUF_ROOT}/lib NO_DEFAULT_PATH)
    find_library(PROTOBUF_LITE_LIBRARY protobuf-lite libprotobuf-lite.lib PATHS ${PROTOBUF_ROOT}/lib NO_DEFAULT_PATH)
    find_library(PROTOBUF_PROTOC_LIBRARY protoc libprotoc.lib PATHS ${PROTOBUF_ROOT}/lib NO_DEFAULT_PATH)
    find_program(PROTOBUF_PROTOC_EXECUTABLE protoc PATHS ${PROTOBUF_ROOT}/bin NO_DEFAULT_PATH)
    if (PROTOBUF_INCLUDE_DIR AND PROTOBUF_LIBRARY AND PROTOBUF_LITE_LIBRARY AND PROTOBUF_PROTOC_LIBRARY AND PROTOBUF_PROTOC_EXECUTABLE)
        message(STATUS "Using custom protobuf library in ${PROTOBUF_ROOT}.")
        SET(PROTOBUF_FOUND true)
        SET_PROTOBUF_VERSION()
        PROMPT_PROTOBUF_LIB()
    else ()
        message(WARNING "Cannot find protobuf library in ${PROTOBUF_ROOT}")
    endif ()
endif ()

FUNCTION(build_protobuf TARGET_NAME)
    STRING(REPLACE "extern_" "" TARGET_DIR_NAME "${TARGET_NAME}")
    SET(PROTOBUF_SOURCES_DIR ${THIRD_PARTY_PATH}/${TARGET_DIR_NAME})
    SET(PROTOBUF_INSTALL_DIR ${THIRD_PARTY_PATH}/install/${TARGET_DIR_NAME})

    SET(${TARGET_NAME}_INCLUDE_DIR "${PROTOBUF_INSTALL_DIR}/include" PARENT_SCOPE)
    SET(PROTOBUF_INCLUDE_DIR "${PROTOBUF_INSTALL_DIR}/include" PARENT_SCOPE)
    SET(${TARGET_NAME}_LITE_LIBRARY
            "${PROTOBUF_INSTALL_DIR}/lib/libprotobuf-lite${CMAKE_STATIC_LIBRARY_SUFFIX}"
            PARENT_SCOPE)
    SET(${TARGET_NAME}_LIBRARY
            "${PROTOBUF_INSTALL_DIR}/lib/libprotobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
            PARENT_SCOPE)
    SET(${TARGET_NAME}_PROTOC_LIBRARY
            "${PROTOBUF_INSTALL_DIR}/lib/libprotoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
            PARENT_SCOPE)
    SET(${TARGET_NAME}_PROTOC_EXECUTABLE
            "${PROTOBUF_INSTALL_DIR}/bin/protoc${CMAKE_EXECUTABLE_SUFFIX}"
            PARENT_SCOPE)

    set(prefix_path "${THIRD_PARTY_PATH}/install/zlib")

    # Make sure zlib's two headers are in your /Path/to/install/include path,
    # and delete libz.so which we don't need
    FILE(WRITE ${PROTOBUF_SOURCES_DIR}/src/config.sh
            "rm ${THIRD_PARTY_PATH}/install/zlib/lib/libz.so* -f && mkdir -p ${THIRD_PARTY_PATH}/install/protobuf/include && cp ${THIRD_PARTY_PATH}/install/zlib/include/* ${THIRD_PARTY_PATH}/install/protobuf/include/ && ${CMAKE_COMMAND} ${PROTOBUF_SOURCES_DIR}/src/${TARGET_NAME}/cmake -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_C_FLAGS='${CMAKE_C_FLAGS}' -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG} -DCMAKE_C_FLAGS_RELEASE='${CMAKE_C_FLAGS_RELEASE}' -DCMAKE_CXX_FLAGS='${CMAKE_CXX_FLAGS}' -DCMAKE_CXX_FLAGS_RELEASE='${CMAKE_CXX_FLAGS_RELEASE}' -DCMAKE_CXX_FLAGS_DEBUG='${CMAKE_CXX_FLAGS_DEBUG}' -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_SKIP_RPATH=ON -Dprotobuf_WITH_ZLIB=ON -DZLIB_INCLUDE_DIR=${THIRD_PARTY_PATH}/install/zlib/include -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_BUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX=${PROTOBUF_INSTALL_DIR} -DCMAKE_INSTALL_LIBDIR=lib -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE='${THIRD_PARTY_BUILD_TYPE}' -DCMAKE_PREFIX_PATH=${prefix_path} ${EXTERNAL_OPTIONAL_ARGS}"
            )

    ExternalProject_Add(
            ${TARGET_NAME}
            ${EXTERNAL_PROJECT_LOG_ARGS}
            PREFIX ${PROTOBUF_SOURCES_DIR}
            UPDATE_COMMAND ""
            DEPENDS zlib
            #            GIT_REPOSITORY  "https://github.com/protocolbuffers/protobuf.git"
            #            GIT_TAG         "v3.6.1"
            URL "https://github.com/protocolbuffers/protobuf/archive/v3.6.1.tar.gz"
            CONFIGURE_COMMAND mv ../config.sh . COMMAND sh config.sh
            CMAKE_CACHE_ARGS
            -DCMAKE_INSTALL_PREFIX:PATH=${PROTOBUF_INSTALL_DIR}
            -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
            -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF
            -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
            ${OPTIONAL_CACHE_ARGS}
    )

ENDFUNCTION()

SET(PROTOBUF_VERSION 3.6.1)

IF (NOT PROTOBUF_FOUND)
    message("build protobuf")

    build_protobuf(extern_protobuf)

    SET(PROTOBUF_INCLUDE_DIR ${extern_protobuf_INCLUDE_DIR}
            CACHE PATH "protobuf include directory." FORCE)
    SET(PROTOBUF_LITE_LIBRARY ${extern_protobuf_LITE_LIBRARY}
            CACHE FILEPATH "protobuf lite library." FORCE)
    SET(PROTOBUF_LIBRARY ${extern_protobuf_LIBRARY}
            CACHE FILEPATH "protobuf library." FORCE)
    SET(PROTOBUF_LIBRARIES ${extern_protobuf_LIBRARY}
            CACHE FILEPATH "protobuf library." FORCE)
    SET(PROTOBUF_PROTOC_LIBRARY ${extern_protobuf_PROTOC_LIBRARY}
            CACHE FILEPATH "protoc library." FORCE)

    SET(PROTOBUF_PROTOC_EXECUTABLE ${extern_protobuf_PROTOC_EXECUTABLE}
            CACHE FILEPATH "protobuf executable." FORCE)
    PROMPT_PROTOBUF_LIB(extern_protobuf zlib)
ENDIF (NOT PROTOBUF_FOUND)