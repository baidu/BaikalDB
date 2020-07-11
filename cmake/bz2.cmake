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

SET(BZ2_SOURCES_DIR ${THIRD_PARTY_PATH}/bz2)
SET(BZ2_DOWNLOAD_DIR "${BZ2_SOURCES_DIR}/src/extern_bz2")
SET(BZ2_INSTALL_DIR ${THIRD_PARTY_PATH}/install/bz2)
SET(BZ2_INCLUDE_DIR "${BZ2_INSTALL_DIR}/include" CACHE PATH "bz2 include directory." FORCE)
SET(BZ2_LIBRARIES "${BZ2_INSTALL_DIR}/lib/libbz2.a" CACHE FILEPATH "bz2 library." FORCE)

SET(BZ2_TAR "bzip2-1.0.8")

FILE(WRITE ${BZ2_DOWNLOAD_DIR}/build.sh
        "cd ${BZ2_DOWNLOAD_DIR}/${BZ2_TAR} \n"
        "sed -i 's/CFLAGS=-Wall -Winline -O2 -g $(BIGFILES)/CFLAGS=-fPIC -Wall -Winline -O2 -g $(BIGFILES)/' Makefile\n"
        "${CMAKE_MAKE_PROGRAM} install PREFIX=${BZ2_INSTALL_DIR}"
        )

ExternalProject_Add(
        extern_bz2
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${BZ2_SOURCES_DIR}
        DOWNLOAD_DIR ${BZ2_DOWNLOAD_DIR}
        DOWNLOAD_COMMAND wget -q ftp://sourceware.org/pub/bzip2/${BZ2_TAR}.tar.gz -O ${BZ2_TAR}.tar.gz COMMAND tar -zxf ${BZ2_TAR}.tar.gz
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND sh build.sh
        INSTALL_COMMAND ""
)

ADD_LIBRARY(bz2 STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET bz2 PROPERTY IMPORTED_LOCATION ${BZ2_LIBRARIES})
ADD_DEPENDENCIES(bz2 extern_bz2)