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

SET(GPERF_SOURCES_DIR ${THIRD_PARTY_PATH}/gperf)
SET(GPERF_INSTALL_DIR ${THIRD_PARTY_PATH}/install/gperf)
SET(GPERFTOOLS_INCLUDE_DIR "${GPERF_INSTALL_DIR}/include" CACHE PATH "gperf include directory." FORCE)
SET(GPERFTOOLS_LIBRARIES "${GPERF_INSTALL_DIR}/lib/libtcmalloc_and_profiler.a" CACHE FILEPATH "gperf library." FORCE)

ExternalProject_Add(
        extern_gperf
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${GPERF_SOURCES_DIR}
        GIT_REPOSITORY "https://github.com/gperftools/gperftools.git"
        GIT_TAG "gperftools-2.7"
        CONFIGURE_COMMAND sh autogen.sh COMMAND sh ./configure --prefix=${GPERF_INSTALL_DIR} --disable-debugalloc --enable-frame-pointers
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR}
        INSTALL_COMMAND $(MAKE) install
)

ADD_LIBRARY(gperf STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET gperf PROPERTY IMPORTED_LOCATION ${GPERFTOOLS_LIBRARIES})
ADD_DEPENDENCIES(gperf extern_gperf)
