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

SET(LEVELDB_SOURCES_DIR ${THIRD_PARTY_PATH}/leveldb)
SET(LEVELDB_INSTALL_DIR ${THIRD_PARTY_PATH}/install/leveldb)
SET(LEVELDB_INCLUDE_DIR "${LEVELDB_INSTALL_DIR}/include" CACHE PATH "leveldb include directory." FORCE)
SET(LEVELDB_LIBRARIES "${LEVELDB_INSTALL_DIR}/lib/libleveldb.a" CACHE FILEPATH "leveldb library." FORCE)

ExternalProject_Add(
        extern_leveldb
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${LEVELDB_SOURCES_DIR}
        GIT_REPOSITORY "https://github.com/google/leveldb"
        GIT_TAG "v1.18"
        CONFIGURE_COMMAND sed -i "s/CXXFLAGS += -fPIC//" ${LEVELDB_SOURCES_DIR}/src/extern_leveldb/Makefile COMMAND sed -i "/CXXFLAGS +=/aCXXFLAGS += -fPIC" ${LEVELDB_SOURCES_DIR}/src/extern_leveldb/Makefile
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR} libleveldb.a
        INSTALL_COMMAND mkdir -p ${LEVELDB_INSTALL_DIR}/lib/ COMMAND cp ${LEVELDB_SOURCES_DIR}/src/extern_leveldb/libleveldb.a ${LEVELDB_LIBRARIES} COMMAND cp -r ${LEVELDB_SOURCES_DIR}/src/extern_leveldb/include ${LEVELDB_INSTALL_DIR}/
)

ADD_LIBRARY(leveldb STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET leveldb PROPERTY IMPORTED_LOCATION ${LEVELDB_LIBRARIES})
ADD_DEPENDENCIES(leveldb extern_leveldb)
