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

SET(RAPIDJSON_SOURCES_DIR ${THIRD_PARTY_PATH}/rapidjson)
SET(RAPIDJSON_INSTALL_DIR ${THIRD_PARTY_PATH}/install/rapidjson)
SET(RAPIDJSON_INCLUDE_DIR "${RAPIDJSON_INSTALL_DIR}/include" CACHE PATH "rapidjson include directory." FORCE)
SET(RAPIDJSON_LIBRARIES "${RAPIDJSON_INSTALL_DIR}/lib/librapidjson.a" CACHE FILEPATH "rapidjson library." FORCE)

ExternalProject_Add(
        extern_rapidjson
        ${EXTERNAL_PROJECT_LOG_ARGS}
        GIT_REPOSITORY "https://github.com/Tencent/rapidjson.git"
        GIT_TAG "v1.1.0"
        PREFIX ${RAPIDJSON_SOURCES_DIR}
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}
        -DCMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_C_FLAGS_DEBUG=${CMAKE_C_FLAGS_DEBUG}
        -DCMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}
        -DCMAKE_INSTALL_PREFIX=${RAPIDJSON_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${RAPIDJSON_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DRAPIDJSON_BUILD_DOC=OFF
        -DRAPIDJSON_BUILD_EXAMPLES=OFF
        -DRAPIDJSON_BUILD_TESTS=OFF
        -DRAPIDJSON_BUILD_THIRDPARTY_GTEST=OFF
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        ${EXTERNAL_OPTIONAL_ARGS}
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${RAPIDJSON_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR:PATH=${RAPIDJSON_INSTALL_DIR}/lib
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_LIBRARY(rapidjson STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET rapidjson PROPERTY IMPORTED_LOCATION ${RAPIDJSON_LIBRARIES})
ADD_DEPENDENCIES(rapidjson extern_rapidjson)