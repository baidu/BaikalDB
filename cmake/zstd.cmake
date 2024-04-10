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

SET(ZSTD_SOURCES_DIR ${THIRD_PARTY_PATH}/zstd)
SET(ZSTD_INSTALL_DIR ${THIRD_PARTY_PATH}/install/zstd)
SET(ZSTD_INCLUDE_DIR "${ZSTD_INSTALL_DIR}/include" CACHE PATH "zstd include directory." FORCE)
SET(ZSTD_LIBRARIES "${ZSTD_INSTALL_DIR}/lib/libzstd.a" CACHE FILEPATH "zstd library." FORCE)

FILE(WRITE ${ZSTD_SOURCES_DIR}/src/build.sh
        "cd build/cmake && cmake -DZSTD_LEGACY_SUPPORT=5 -DXXH_NAMESPACE=ZSTD_ && make -j${NUM_OF_PROCESSOR}"
        )

ExternalProject_Add(
        extern_zstd
        ${EXTERNAL_PROJECT_LOG_ARGS}
#        GIT_REPOSITORY "https://github.com/google/zstd.git"
#        GIT_TAG "apache-zstd-0.17.1"
        URL "https://github.com/facebook/zstd/archive/v1.4.4.tar.gz"
        PREFIX ${ZSTD_SOURCES_DIR}
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
        INSTALL_COMMAND mkdir -p ${ZSTD_INSTALL_DIR}/lib/ COMMAND cp ${ZSTD_SOURCES_DIR}/src/extern_zstd/build/cmake/lib/libzstd.a ${ZSTD_LIBRARIES} COMMAND mkdir -p ${ZSTD_INCLUDE_DIR} COMMAND cp ${ZSTD_SOURCES_DIR}/src/extern_zstd/lib/zstd.h ${ZSTD_SOURCES_DIR}/src/extern_zstd/lib/dictBuilder/zdict.h ${ZSTD_INCLUDE_DIR}
)

ADD_LIBRARY(zstd STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET zstd PROPERTY IMPORTED_LOCATION ${ZSTD_LIBRARIES})
ADD_DEPENDENCIES(zstd extern_zstd)
