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

SET(RE2_SOURCES_DIR ${THIRD_PARTY_PATH}/re2)
SET(RE2_INSTALL_DIR ${THIRD_PARTY_PATH}/install/re2)
SET(RE2_INCLUDE_DIR "${RE2_INSTALL_DIR}/include" CACHE PATH "re2 include directory." FORCE)
SET(RE2_LIBRARIES "${RE2_INSTALL_DIR}/lib/libre2.a" CACHE FILEPATH "re2 library." FORCE)

ExternalProject_Add(
        extern_re2
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${RE2_SOURCES_DIR}
        GIT_REPOSITORY "https://github.com/google/re2.git"
        GIT_TAG "2019-01-01"
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
        -DCMAKE_INSTALL_PREFIX=${RE2_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${prefix_path}
        -DRE2_BUILD_TESTING=OFF
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${RE2_INSTALL_DIR}
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
)

ADD_LIBRARY(re2 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET re2 PROPERTY IMPORTED_LOCATION ${RE2_LIBRARIES})
ADD_DEPENDENCIES(re2 extern_re2)
