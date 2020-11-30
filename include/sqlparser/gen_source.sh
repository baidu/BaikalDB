#!/bin/bash

prefix='/opt/compiler/gcc-4.8.2/bin/'

cur_dir='.'
if [[ $2 == "opensource" ]]; then
  prefix=''
  cur_dir=$1
fi

echo "prefix: ${prefix}"
echo "output: ${out_dir}"

cd ${cur_dir}/include/sqlparser && ${prefix}flex sql_lex.l && ${prefix}bison sql_parse.y

dest_dir=$3
if [ ! -z "${dest_dir}" ]; then
  mv *.flex.* ${dest_dir}/ && mv *.yacc.* ${dest_dir}/
fi 
