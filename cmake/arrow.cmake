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

SET(ARROW_SOURCES_DIR ${THIRD_PARTY_PATH}/arrow)
SET(ARROW_INSTALL_DIR ${THIRD_PARTY_PATH}/install/arrow)
SET(ARROW_INCLUDE_DIR "${ARROW_INSTALL_DIR}/include" CACHE PATH "arrow include directory." FORCE)
SET(ARROW_LIBRARIES "${ARROW_INSTALL_DIR}/lib/libarrow.a" CACHE FILEPATH "arrow library." FORCE)

FILE(WRITE ${ARROW_SOURCES_DIR}/src/build.sh
        "cd cpp && cmake -DCMAKE_BUILD_TYPE=release -DARROW_JEMALLOC=OFF -DARROW_BUILD_SHARED=OFF && make -j${NUM_OF_PROCESSOR}"
        )

ExternalProject_Add(
        extern_arrow
        ${EXTERNAL_PROJECT_LOG_ARGS}
#        GIT_REPOSITORY "https://github.com/apache/arrow.git"
#        GIT_TAG "apache-arrow-0.17.1"
        URL "https://github.com/apache/arrow/archive/apache-arrow-0.17.1.tar.gz"
        PREFIX ${ARROW_SOURCES_DIR}
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
        INSTALL_COMMAND mkdir -p ${ARROW_INSTALL_DIR}/lib/ COMMAND cp ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/build/release/libarrow.a ${ARROW_LIBRARIES} COMMAND mkdir -p ${ARROW_INCLUDE_DIR} COMMAND cp -r ${ARROW_SOURCES_DIR}/src/extern_arrow/cpp/src/arrow ${ARROW_INCLUDE_DIR}/
)

ADD_LIBRARY(arrow STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET arrow PROPERTY IMPORTED_LOCATION ${ARROW_LIBRARIES})
ADD_DEPENDENCIES(arrow extern_arrow)
