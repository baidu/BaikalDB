#!/usr/bin/env bash
set -ex
for op in $@; do
  eval "$op"
done

RELEASE="https://api.github.com/repos/$repo/releases/tags/$tag"
upload_url=`curl -s $RELEASE | grep 'upload_url' |awk -F": \"|\{" '{print $2}'`
content_type=$(file -b --mime-type $filepath)
filename=$(basename "$filepath")
curl -s -X POST \
     -H "authorization: Bearer $github_token" \
     -H "content-type: $content_type" \
     --data-binary @"$filepath" \
     "$upload_url?name=$filename"