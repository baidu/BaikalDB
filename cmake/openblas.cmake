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

SET(OPENBLAS_SOURCES_DIR ${THIRD_PARTY_PATH}/openblas)
SET(OPENBLAS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/openblas)
SET(OPENBLAS_INCLUDE_DIR "${OPENBLAS_INSTALL_DIR}/include" CACHE PATH "openblas include directory." FORCE)
SET(OPENBLAS_LIBRARIES "${OPENBLAS_INSTALL_DIR}/lib/libopenblas.a" CACHE FILEPATH "openblas library." FORCE)

ExternalProject_Add(
        extern_openblas
        ${EXTERNAL_PROJECT_LOG_ARGS}
        URL "https://github.com/xianyi/OpenBLAS/archive/refs/tags/v0.3.24.tar.gz"
        PREFIX ${OPENBLAS_SOURCES_DIR}
        UPDATE_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR} PREFIX=${OPENBLAS_INSTALL_DIR}  
        INSTALL_COMMAND $(MAKE) install PREFIX=${OPENBLAS_INSTALL_DIR}
)
ADD_LIBRARY(openblas STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET openblas PROPERTY IMPORTED_LOCATION ${OPENBLAS_LIBRARIES})
ADD_DEPENDENCIES(openblas extern_openblas)
