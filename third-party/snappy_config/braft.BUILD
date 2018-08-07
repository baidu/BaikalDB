licenses(["notice"])

cc_library(
    name = "braft",
    srcs = glob([
        "src/braft/*.cpp",
    ]),

    hdrs = glob([
        "src/braft/*.h",
    ]),

    includes = [
        "src",
    ],
    defines = [
    ],
    copts = [
        "-DGFLAGS=gflags",
        "-DOS_LINUX",
        "-DSNAPPY",
        "-DHAVE_SSE42",
        "-DNDEBUG",
        "-fno-omit-frame-pointer",
        "-momit-leaf-frame-pointer",
        "-msse4.2",
        "-pthread",
        "-Werror",
        "-Wsign-compare",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
        "-Woverloaded-virtual",
        "-Wnon-virtual-dtor",
        "-Wno-missing-field-initializers",
        "-std=c++11",
    ],
    linkopts = [
        "-lm",
        "-lpthread",
    ],
    deps = [
        "//external:gflags",
        "//external:glog",
        "//external:gtest",
        "//external:brpc",
        "@com_google_protobuf//:protobuf",
        ":raft_cc_proto",
        ":local_storage_cc_proto",
        ":local_file_meta_cc_proto",
        ":file_service_cc_proto",
        ":errno_cc_proto",
        ":enum_cc_proto",
        ":builtin_service_cc_proto",
        ":cli_cc_proto",
    ],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "raft_cc_proto",
    deps = [":raft_proto"],
)

cc_proto_library(
    name = "local_storage_cc_proto",
    deps = [":local_storage_proto"],
)

cc_proto_library(
    name = "local_file_meta_cc_proto",
    deps = [":local_file_meta_proto"],
)

cc_proto_library(
    name = "file_service_cc_proto",
    deps = [":file_service_proto"],
)

cc_proto_library(
    name = "errno_cc_proto",
    deps = [":errno_proto"],
)

cc_proto_library(
    name = "enum_cc_proto",
    deps = [":enum_proto"],
)

cc_proto_library(
    name = "builtin_service_cc_proto",
    deps = [":builtin_service_proto"],
)

cc_proto_library(
    name = "cli_cc_proto",
    deps = [":cli_proto"],
)

proto_library(
    name = "raft_proto",
#    srcs = [":gen_raft_proto"],
    srcs = ["src/braft/raft.proto"],
    deps = [":enum_proto"],
)

proto_library(
    name = "local_storage_proto",
#    srcs = [":gen_local_storage_proto"],
    srcs = ["src/braft/local_storage.proto"],
    deps = [":raft_proto",
            ":local_file_meta_proto",
    ],
#    proto_source_root = "src",
)

proto_library(
    name = "local_file_meta_proto",
    srcs = ["src/braft/local_file_meta.proto"],
#    proto_source_root = "src",
)

proto_library(
    name = "file_service_proto",
    srcs = ["src/braft/file_service.proto"],
)

proto_library(
    name = "errno_proto",
    srcs = ["src/braft/errno.proto"],
#    proto_source_root = "src",
)

proto_library(
    name = "enum_proto",
    srcs = ["src/braft/enum.proto"],
)

proto_library(
    name = "builtin_service_proto",
    srcs = ["src/braft/builtin_service.proto"],
)

proto_library(
    name = "cli_proto",
    srcs = ["src/braft/cli.proto"],
)
