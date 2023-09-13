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

SET(FAISS_SOURCES_DIR ${THIRD_PARTY_PATH}/faiss)
SET(FAISS_INSTALL_DIR ${THIRD_PARTY_PATH}/install/faiss)
SET(FAISS_INCLUDE_DIR "${FAISS_INSTALL_DIR}/include" CACHE PATH "faiss include directory." FORCE)
SET(FAISS_LIBRARIES "${FAISS_INSTALL_DIR}/lib/libfaiss.a" CACHE FILEPATH "faiss library." FORCE)

ExternalProject_Add(
        extern_faiss
        ${EXTERNAL_PROJECT_LOG_ARGS}
        DEPENDS openblas
        URL "https://github.com/facebookresearch/faiss/archive/refs/tags/v1.7.4.tar.gz"
        PREFIX ${FAISS_SOURCES_DIR}
        UPDATE_COMMAND ""
        CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
        -DCMAKE_INSTALL_PREFIX=${FAISS_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR=${FAISS_INSTALL_DIR}/lib
        -DFAISS_ENABLE_PYTHON=OFF
        -DBUILD_TESTING=OFF 
        -DFAISS_ENABLE_GPU=OFF   
        -DFAISS_ENABLE_C_API=OFF
        -DFAISS_OPT_LEVEL=avx2
        -DMKL_LIBRARIES=${THIRD_PARTY_PATH}/install/openblas
        -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
        -DCMAKE_PREFIX_PATH=${FAISS_INSTALL_DIR}
        ${EXTERNAL_OPTIONAL_ARGS}
        LIST_SEPARATOR |
        CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${FAISS_INSTALL_DIR}
        -DCMAKE_INSTALL_LIBDIR:PATH=${FAISS_INSTALL_DIR}/lib
        -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
        BUILD_IN_SOURCE 1
        BUILD_COMMAND $(MAKE) -j ${NUM_OF_PROCESSOR} faiss
        INSTALL_COMMAND $(MAKE) install
)
ADD_DEPENDENCIES(extern_faiss openblas)
ADD_LIBRARY(faiss STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET faiss PROPERTY IMPORTED_LOCATION ${FAISS_LIBRARIES})
ADD_DEPENDENCIES(faiss extern_faiss)
