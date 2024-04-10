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

SET(LIBURING_SOURCES_DIR ${THIRD_PARTY_PATH}/liburing)
SET(LIBURING_INSTALL_DIR ${THIRD_PARTY_PATH}/install/liburing)
SET(LIBURING_INCLUDE_DIR "${LIBURING_INSTALL_DIR}/include" CACHE PATH "liburing include directory." FORCE)
SET(LIBURING_LIBRARIES "${LIBURING_INSTALL_DIR}/lib/liburing.a" CACHE FILEPATH "liburing library." FORCE)

FILE(WRITE ${LIBURING_SOURCES_DIR}/src/build.sh
        "make -j${NUM_OF_PROCESSOR}"
        )

ExternalProject_Add(
        extern_liburing
        ${EXTERNAL_PROJECT_LOG_ARGS}
        URL "https://github.com/axboe/liburing/archive/refs/tags/liburing-2.4.tar.gz"
        PREFIX ${LIBURING_SOURCES_DIR}
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
        INSTALL_COMMAND mkdir -p ${LIBURING_INSTALL_DIR}/lib/ COMMAND cp ${LIBURING_SOURCES_DIR}/src/extern_liburing/src/liburing.a ${LIBURING_LIBRARIES} COMMAND mkdir -p ${LIBURING_INCLUDE_DIR} COMMAND cp -r ${LIBURING_SOURCES_DIR}/src/extern_liburing/src/include/liburing.h ${LIBURING_SOURCES_DIR}/src/extern_liburing/src/include/liburing ${LIBURING_INCLUDE_DIR}
)

ADD_LIBRARY(liburing STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET liburing PROPERTY IMPORTED_LOCATION ${LIBURING_LIBRARIES})
ADD_DEPENDENCIES(liburing extern_liburing)
