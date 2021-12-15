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

SET(CROARING_SOURCES_DIR ${THIRD_PARTY_PATH}/croaring)
SET(CROARING_INSTALL_DIR ${THIRD_PARTY_PATH}/install/croaring)
SET(CROARING_INCLUDE_DIR "${CROARING_INSTALL_DIR}/include" CACHE PATH "croaring include directory." FORCE)
SET(CROARING_LIBRARIES "${CROARING_INSTALL_DIR}/lib/libroaring.a" CACHE FILEPATH "croaring library." FORCE)

FILE(WRITE ${CROARING_SOURCES_DIR}/src/build.sh
        "cmake -DROARING_BUILD_STATIC=ON -DENABLE_ROARING_TESTS=OFF . && make -j${NUM_OF_PROCESSOR}"
        )

ExternalProject_Add(
        extern_croaring
        ${EXTERNAL_PROJECT_LOG_ARGS}
#        GIT_REPOSITORY "https://github.com/RoaringBitmap/CRoaring/archive/v0.2.66.tar.gz"
#        GIT_TAG "CRoaring-0.2.66"
        URL "http://github.com/RoaringBitmap/CRoaring/archive/v0.2.66.tar.gz"
        PREFIX ${CROARING_SOURCES_DIR}
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND mv ../build.sh . COMMAND sh build.sh
        INSTALL_COMMAND mkdir -p ${CROARING_INSTALL_DIR}/lib/ COMMAND cp ${CROARING_SOURCES_DIR}/src/extern_croaring/src/libroaring.a ${CROARING_LIBRARIES} COMMAND mkdir -p ${CROARING_INCLUDE_DIR} COMMAND cp -r ${CROARING_SOURCES_DIR}/src/extern_croaring/include/roaring ${CROARING_INCLUDE_DIR} COMMAND cp -r ${CROARING_SOURCES_DIR}/src/extern_croaring/cpp/roaring.hh ${CROARING_INCLUDE_DIR}/
)

ADD_LIBRARY(croaring STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET croaring PROPERTY IMPORTED_LOCATION ${CROARING_LIBRARIES})
ADD_DEPENDENCIES(croaring extern_croaring)
