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

include(ExternalProject)

SET(BOOST_PROJECT "extern_boost")

SET(Boost_VERSION "106300")
SET(Boost_LIB_VERSION "1_63_0")
SET(BOOST_VER "1.63.0")
SET(BOOST_TAR "boost_1_63_0" CACHE STRING "" FORCE)
SET(BOOST_URL "https://jaist.dl.sourceforge.net/project/boost/boost/1.63.0/${BOOST_TAR}.tar.gz" CACHE STRING "" FORCE)

MESSAGE(STATUS "BOOST_TAR: ${BOOST_TAR}, BOOST_URL: ${BOOST_URL}")

SET(BOOST_SOURCES_DIR ${THIRD_PARTY_PATH}/boost)
SET(BOOST_DOWNLOAD_DIR "${BOOST_SOURCES_DIR}/src/${BOOST_PROJECT}")
SET(BOOST_INSTALL_DIR "${THIRD_PARTY_PATH}/install/boost")
SET(BOOST_INCLUDE_DIR "${BOOST_INSTALL_DIR}/include" CACHE PATH "boost include directory." FORCE)

SET(Boost_INCLUDE_DIR ${BOOST_INCLUDE_DIR})
SET(Boost_INCLUDE_DIRS ${BOOST_INCLUDE_DIR})
SET(Boost_LIBRARIES ${BOOST_INSTALL_DIR}/lib/libboost_thread.a
        ${BOOST_INSTALL_DIR}/lib/libboost_filesystem.a
        ${BOOST_INSTALL_DIR}/lib/libboost_regex.a
        ${BOOST_INSTALL_DIR}/lib/libboost_system.a
        )

set_directory_properties(PROPERTIES CLEAN_NO_CUSTOM 1)
include_directories(${BOOST_INCLUDE_DIR})

FILE(WRITE ${BOOST_DOWNLOAD_DIR}/build.sh
        "cd ${BOOST_DOWNLOAD_DIR} && mkdir -p build && sh bootstrap.sh --prefix=${BOOST_INSTALL_DIR} --with-libraries=thread,filesystem,regex,system && ./b2 install"
        )

ExternalProject_Add(
        ${BOOST_PROJECT}
        ${EXTERNAL_PROJECT_LOG_ARGS}
        PREFIX ${BOOST_SOURCES_DIR}
        #GIT_REPOSITORY "https://github.com/boostorg/boost.git"
        #GIT_TAG "boost-1.63.0"
        DOWNLOAD_DIR ${BOOST_DOWNLOAD_DIR}
        DOWNLOAD_NO_PROGRESS 1
        DOWNLOAD_COMMAND wget --no-check-certificate ${BOOST_URL} -O ${BOOST_TAR}.tar.gz COMMAND tar -zxf ${BOOST_TAR}.tar.gz --strip-components=1
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        #BUILD_COMMAND sh bootstrap.sh --prefix=${BOOST_INSTALL_DIR} --with-libraries=thread && ./b2 install
        BUILD_COMMAND sh ${BOOST_DOWNLOAD_DIR}/build.sh
        INSTALL_COMMAND ""
)

add_library(boost STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET boost PROPERTY IMPORTED_LOCATION ${Boost_LIBRARIES})
add_dependencies(boost ${BOOST_PROJECT})
