#!/usr/bin/env bash

set -ex
PROJ_DIR="$(cd "$(dirname "$0")" && pwd)"/..
PACK_DIR=/work/pack
BAZEL_BIN=/work/BaikalDB/bazel-bin/

for op in $@; do
  eval "$op"
done

rm -rf $PACK_DIR && mkdir -p $PACK_DIR 
cd $PACK_DIR
modules=(baikaldb baikalMeta baikalStore)
for module in ${modules[@]}
do
    mkdir -p $module/bin $module/conf $module/log $module/script
    cp $BAZEL_BIN/$module $module/bin
    cp $PROJ_DIR/conf/$module/* $module/conf
    cp -r $PROJ_DIR/src/tools/script/* $module/script
    
done

tar czf baikal-all-$version-$os.tgz -C $PACK_DIR baikaldb baikalMeta baikalStore
cp baikal-all-$version-$os.tgz /__w
