#!/bin/bash
path=../../../../proto
protoc --python_out=./protoout -I=$path $path/*.proto
