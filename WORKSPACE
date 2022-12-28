#load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
#load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")


http_archive(
  name = "com_google_googletest",
  strip_prefix = "googletest-0fe96607d85cf3a25ac40da369db62bbee2939a5",
  url = "https://github.com/google/googletest/archive/0fe96607d85cf3a25ac40da369db62bbee2939a5.tar.gz",
)

bind(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
)

http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-ab8edf1dbe2237b4717869eaab11a2998541ad8d",
    url = "https://github.com/google/protobuf/archive/ab8edf1dbe2237b4717869eaab11a2998541ad8d.tar.gz",
)

bind(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

new_http_archive(
    name = "com_github_apache_arrow",
    build_file = "third-party/com_github_apache_arrow/BUILD",
    strip_prefix = "arrow-apache-arrow-0.17.1",
    url = "https://github.com/apache/arrow/archive/apache-arrow-0.17.1.tar.gz",
)

bind(
    name = "arrow",
    actual = "@com_github_apache_arrow//:arrow",
)

http_archive(
  name = "com_github_gflags_gflags",
  strip_prefix = "gflags-46f73f88b18aee341538c0dfc22b1710a6abedef",
  url = "https://github.com/gflags/gflags/archive/46f73f88b18aee341538c0dfc22b1710a6abedef.tar.gz",
)

bind(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

new_http_archive(
  name = "com_github_google_glog",
  build_file = "third-party/glog.BUILD",
  strip_prefix = "glog-a6a166db069520dbbd653c97c2e5b12e08a8bb26",
  url = "https://github.com/google/glog/archive/a6a166db069520dbbd653c97c2e5b12e08a8bb26.tar.gz"
)

bind(
    name = "glog",
    actual = "@com_github_google_glog//:glog",
)

new_http_archive(
  name = "com_github_google_leveldb",
  build_file = "third-party/leveldb.BUILD",
  strip_prefix = "leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6",
  url = "https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz"
)


#zstd
new_http_archive(
    name = "com_github_facebook_zstd",
    urls = ["https://github.com/facebook/zstd/archive/v1.4.4.tar.gz",],
    strip_prefix = "zstd-1.4.4",
    build_file = "third-party/zstd.BUILD",
    sha256 = "a364f5162c7d1a455cc915e8e3cf5f4bd8b75d09bc0f53965b0c9ca1383c52c8",
)

bind(
    name = "zstd",
    actual = "@com_github_facebook_zstd//:zstd",
)

# from https://github.com/nelhage/rules_boost
# load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
_RULES_BOOST_COMMIT = "08afe477b894ac99dd23d4d85dd6a189b3eabeff"

http_archive(
    name = "com_github_nelhage_rules_boost",
    strip_prefix = "rules_boost-%s" % _RULES_BOOST_COMMIT,
    urls = [
        "https://github.com/nelhage/rules_boost/archive/%s.tar.gz" % _RULES_BOOST_COMMIT,
    ],
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
boost_deps()

# from https://github.com/envoyproxy/envoy/blob/master/bazel/repositories.bzl
new_http_archive(
    name = "com_github_tencent_rapidjson",
    url = "https://github.com/Tencent/rapidjson/archive/v1.1.0.tar.gz",
    strip_prefix = "rapidjson-1.1.0",
    build_file = "third-party/rapidjson.BUILD",
)

bind(
    name = "rapidjson",
    actual = "@com_github_tencent_rapidjson//:rapidjson",
)

new_http_archive(
    name = "com_github_facebook_rocksdb",
    url = "https://github.com/facebook/rocksdb/archive/v6.26.0.tar.gz",
    strip_prefix = "rocksdb-6.26.0",
    sha256 = "b793066463da71d31a46f31946e4fca435a7c3e73435e2bb1d062d95e0a20116",
    build_file = "third-party/com_github_facebook_rocksdb/BUILD",
)

bind(
    name = "rocksdb",
    actual = "@com_github_facebook_rocksdb//:rocksdb",
)

new_http_archive(
    name = "com_github_RoaringBitmap_CRoaring",
    url = "https://github.com/RoaringBitmap/CRoaring/archive/v0.2.66.tar.gz",
    strip_prefix = "CRoaring-0.2.66",
    sha256 = "df98bd8f6ff09097ada529a004af758ff4d33faf6a06fadf8fad9a6533afc241",
    build_file = "third-party/com_github_RoaringBitmap_CRoaring/BUILD",
)

bind(
    name = "croaring",
    actual = "@com_github_RoaringBitmap_CRoaring//:croaring",
)

# snappy
new_http_archive(
    name = "com_github_google_snappy",
    url = "https://github.com/google/snappy/archive/ed3b7b2.tar.gz",
    strip_prefix = "snappy-ed3b7b242bd24de2ca6750c73f64bee5b7505944",
    sha256 = "88a644b224f54edcd57d01074c2d6fd6858888e915c21344b8622c133c35a337",
    build_file = "third-party/snappy.BUILD",
)

# zlib
new_git_repository(
    name = "com_github_madler_zlib",
    remote = "https://github.com/madler/zlib.git",
    tag = "v1.2.11",
    #sha256 = "629380c90a77b964d896ed37163f5c3a34f6e6d897311f1df2a7016355c45eff",
    build_file = "third-party/zlib.BUILD",
)

bind(
    name = "zlib",
    actual = "@com_github_madler_zlib//:zlib",
)

bind(
    name = "snappy",
    actual = "@com_github_google_snappy//:snappy",
)

bind(
    name = "snappy_config",
    actual = "//third-party/snappy_config:config"
)

# lz4
new_http_archive(
    name = "com_github_lz4_lz4",
    urls = ["https://github.com/lz4/lz4/archive/v1.9.2.tar.gz"],
    strip_prefix = "lz4-1.9.2",
    build_file = "third-party/lz4.BUILD",
    sha256 = "658ba6191fa44c92280d4aa2c271b0f4fbc0e34d249578dd05e50e76d0e5efcc",
)
bind(
    name = "lz4",
    actual = "@com_github_lz4_lz4//:lz4",
)



git_repository(
    name = "com_github_brpc_braft",
    remote = "https://github.com/baidu/braft.git",
    tag = "v1.1.2",
)

bind(
    name = "braft",
    actual = "@com_github_brpc_braft//:braft",
)

git_repository(
    name = "com_github_google_re2",
    remote = "https://github.com/google/re2.git",
    tag = "2019-01-01",
)

bind(
    name = "re2",
    actual = "@com_github_google_re2//:re2",
)

git_repository(
    name = "com_github_brpc_brpc",
    remote= "https://github.com/apache/incubator-brpc.git",
    tag = "0.9.7",
)

bind(
    name = "brpc",
    actual = "@com_github_brpc_brpc//:brpc",
)

bind(
    name = "butil",
    actual = "@com_github_brpc_brpc//:butil",
)

bind(
    name = "bthread",
    actual = "@com_github_brpc_brpc//:bthread",
)

bind(
    name = "bvar",
    actual = "@com_github_brpc_brpc//:bvar",
)

bind(
    name = "json2pb",
    actual = "@com_github_brpc_brpc//:json2pb",
)

# gperftools
new_http_archive(
    name = "com_github_gperftools_gperftools",
    url = "https://github.com/gperftools/gperftools/archive/gperftools-2.7.tar.gz",
    strip_prefix = "gperftools-gperftools-2.7",
    sha256 = "3a88b4544315d550c87db5c96775496243fb91aa2cea88d2b845f65823f3d38a",
    build_file = "third-party/gperftools.BUILD",
)

bind(
    name = "tcmalloc_and_profiler",
    actual = "@com_github_gperftools_gperftools//:tcmalloc_and_profiler",
)
