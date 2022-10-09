licenses(["notice"])  # Apache v2

exports_files(["LICENSE"])

load(":bazel/baikaldb.bzl", "baikaldb_proto_library")

genrule(
    name = "gen_parser",
    srcs = [
        "include/sqlparser/sql_lex.l",
        "include/sqlparser/sql_parse.y",
    ],
    outs = [
        "include/sqlparser/sql_lex.flex.h",
        "include/sqlparser/sql_lex.flex.cc",
        "include/sqlparser/sql_parse.yacc.hh",
        "include/sqlparser/sql_parse.yacc.cc",  
    ],
    cmd = "\n".join([
        "flex $(location include/sqlparser/sql_lex.l)",
        "cat sql_lex.flex.h > $(location include/sqlparser/sql_lex.flex.h)",
        "cat sql_lex.flex.cc > $(location include/sqlparser/sql_lex.flex.cc)",
        "bison $(location include/sqlparser/sql_parse.y)",
        "cat sql_parse.yacc.hh > $(location include/sqlparser/sql_parse.yacc.hh)",
        "cat sql_parse.yacc.cc > $(location include/sqlparser/sql_parse.yacc.cc)",
    ]),
)

COPTS  = [
    "-Iproto",
    "-Iinclude/sqlparser",
    "-Iinclude/physical_plan",
    "-Iinclude/protocol",
    "-Iinclude/common",
    "-Iinclude/engine",
    "-Iinclude/reverse",
    "-Iinclude/reverse/boolean_engine",
    "-Iinclude/exec",
    "-Iinclude/expr",
    "-Iinclude/meta_server",
    "-Iinclude/logical_plan",
    "-Iinclude/raft",
    "-Iinclude/raft_store",
    "-Iinclude/raft_meta",
    "-Iinclude/runtime",
    "-Iinclude/store",
    "-Iinclude/session",
    "-Iinclude/mem_row",
    "-mpclmul",
    "-ggdb",
    "-pipe",
    "-W",
    "-fPIC",
    "-std=c++11", 
    "-O2", 
    "-g", 
    "-fno-omit-frame-pointer", 
    "-Wno-strict-aliasing",
    "-Wno-unused-parameter",
    "-Wno-parentheses",
    "-Wno-deprecated-declarations",
    "-DBAIKAL_TCMALLOC",
    "-UNDEBUG",
]

cc_library(
    name = "common",
    srcs = glob(["src/common/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/common/",
    ],

    deps = [
        "@boost//:lexical_cast",
        "@boost//:algorithm",
        "//external:rapidjson",
        "//external:rocksdb",
        "//external:brpc",
        "//external:butil",
        "//external:bthread",
        "//external:json2pb",
        "//external:braft",
        "//external:arrow",
        "//external:croaring",
        "//external:re2",
        "//external:tcmalloc_and_profiler",
        ":cc_baikaldb_internal_proto",
        ":sqlparser",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "engine",
    srcs = glob(["src/engine/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/engine",
    ],

    deps = [
        "@boost//:lexical_cast",
        "@boost//:algorithm",
        "//external:rapidjson",
        "//external:rocksdb",
        "//external:brpc",
        "//external:butil",
        "//external:bthread",
        "//external:json2pb",
        "//external:braft",
        "//external:arrow",
        "//external:croaring",
        ":cc_baikaldb_internal_proto",
        ":common",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "engine2",
    srcs = glob(["src/engine/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/engine",
    ],

    deps = [
        "@boost//:lexical_cast",
        "@boost//:algorithm",
        "//external:rapidjson",
        "//external:rocksdb",
        "//external:brpc",
        "//external:butil",
        "//external:bthread",
        "//external:json2pb",
        "//external:braft",
        "//external:arrow",
        "//external:croaring",
        ":cc_baikaldb_internal_proto",
        ":common",
        ":reverse",
        ":raft_dummy",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "mem_row",
    srcs = glob(["src/mem_row/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/mem_row",
    ],

    deps = [
        ":cc_baikaldb_internal_proto",
        "//external:bthread",
        "//external:brpc",
        "@boost//:lexical_cast",
        ":common",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "store",
    srcs = glob(["src/store/*.cpp"],
                exclude = [
                    "src/store/main.cpp",
                ]
    ),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/store",
    ],

    deps = [
        "//external:rapidjson",
        "//external:rocksdb",
        "//external:brpc",
        "//external:butil",
        "//external:arrow",
        "//external:croaring",
        "@boost//:lexical_cast",
        "@boost//:filesystem",
        "@boost//:algorithm",
        "//external:braft",
        ":cc_baikaldb_internal_proto",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "expr",
    srcs = glob(["src/expr/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    copts = COPTS,
    includes = [
        "include/expr",
    ],

    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:regex",
        "@boost//:date_time",
        "@boost//:lexical_cast",
        "//external:brpc",
        "//external:bthread",
        "//external:rocksdb",
        "//external:re2",
        "//external:croaring",
        "//external:rapidjson",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "exec",
    srcs = glob(["src/exec/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/exec",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:asio",
        "@boost//:variant",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        "//external:arrow",
        "//external:croaring",
        ":expr",
        ":session",
        ":protocol",
        ":runtime", 
        ":engine", 
        ":physical_plan",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "exec2",
    srcs = glob(["src/exec/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,

    includes = [
        "include/exec",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:asio",
        "@boost//:variant",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        "//external:arrow",
        "//external:croaring",
        ":expr",
        ":session",
        ":engine",
        ":runtime",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "logical_plan",
    srcs = glob(["src/logical_plan/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/logical_plan/",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:asio",
        "@boost//:variant",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        "//external:arrow",
        "//external:croaring",
        "//external:re2",
        ":common",
        ":reverse",
        ":sqlparser",
        ":exec2",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "physical_plan",
    srcs = glob(["src/physical_plan/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/physical_plan/",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:variant",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:arrow",
        "//external:croaring",
        "//external:re2",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "physical_plan2",
    srcs = glob(["src/physical_plan/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/physical_plan/",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:unordered",
        "@boost//:variant",
        "@boost//:thread",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:arrow",
        "//external:croaring",
        ":exec2",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "raft",
    srcs = glob(["src/raft/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/raft/",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "//external:braft",
        "//external:rocksdb",
        "//external:rapidjson",
        "//external:arrow",
        "//external:croaring",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "raft_store",
    srcs = glob(["src/raft_store/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/raft_store",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "//external:braft",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        "//external:arrow",
        "//external:croaring",
        ":common",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "raft_meta",
    srcs = glob(["src/raft_meta/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/raft_meta",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "//external:braft",
        "//external:rocksdb",
        "//external:rapidjson",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "raft_dummy",
    srcs = glob(["src/raft_dummy/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/raft_dummy",
    ],
    deps = [],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "sqlparser",
    srcs = glob(["include/sqlparser/*.cc"]) + [
        ":gen_parser",
    ],

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hh",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/sqlparser",
    ],
    deps = [
        "//external:butil",
        "//external:brpc",
        ":cc_baikaldb_internal_proto",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "meta_server",
    srcs = glob(["src/meta_server/*.cpp"],
                exclude = [
                    "src/meta_server/main.cpp",
                ],
    ),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),

    copts = COPTS,
    includes = [
        "include/meta_server",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "@boost//:algorithm",
        "//external:braft",
        "//external:butil",
        "//external:rocksdb",
        "//external:rapidjson",
        ":engine",
        ":common",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "baikalMeta",
    srcs = ["src/meta_server/main.cpp"],
    includes = [
        "include/meta_server",
        "include/engine",
        "include/common",
    ],
    copts = [
        "-DBAIDU_RPC_ENABLE_CPU_PROFILER",
        "-DBAIDU_RPC_ENABLE_HEAP_PROFILER",
    ],
    deps = [
        ":meta_server",
        ":cc_baikaldb_internal_proto",
        ":common",
        ":engine",
        ":raft",
        ":raft_meta",
        "@boost//:filesystem",
        "//external:tcmalloc_and_profiler",
    ],
    linkstatic = True,
)

cc_binary(
    name = "baikalStore",
    srcs = ["src/store/main.cpp"],
    includes = [
        "include/store",
    ],
    copts = [
        "-DBAIDU_RPC_ENABLE_CPU_PROFILER",
        "-DBAIDU_RPC_ENABLE_HEAP_PROFILER",
    ],
    deps = [
        ":store",
        ":session",
        ":common",
        ":engine",
        ":raft",
        ":protocol",
        ":raft_store",
        ":sqlparser",
        ":reverse",
        ":expr",
        ":exec",
        ":runtime",
        ":physical_plan",
        ":logical_plan",
        ":mem_row",
        "//external:tcmalloc_and_profiler",
    ],
    linkstatic = True,
)

cc_library(
    name = "protocol",
    srcs = glob(["src/protocol/*.cpp"],
                exclude = [
                    "src/protocol/main.cpp",
                ],
    ),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    copts = COPTS,
    includes = [
        "include/protocol",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:asio",
        "@boost//:variant",
        "//external:bvar",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        "//external:re2",
        ":common",
        ":session",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "protocol2",
    srcs = glob(["src/protocol/*.cpp"],
                exclude = [
                    "src/protocol/main.cpp",
                ],
    ),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    copts = COPTS,
    includes = [
        "include/protocol",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:asio",
        "//external:bvar",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        ":common",
        ":session",
        ":exec2",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "session",
    srcs = glob(["src/session/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    copts = COPTS,
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:unordered",
        "@boost//:thread",
        "@boost//:asio",
        "//external:bthread",
        "//external:rocksdb",
        "//external:arrow",
        "//external:croaring",
        ":common",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "baikaldb",
    srcs = ["src/protocol/main.cpp"],
    copts = COPTS + [
        "-DBAIDU_RPC_ENABLE_CPU_PROFILER",
        "-DBAIDU_RPC_ENABLE_HEAP_PROFILER",
    ],
    deps = [
        ":protocol2",
        ":common",
        ":session",
        ":engine2",
        ":store",
        ":sqlparser",
        ":reverse",
        ":expr",
        ":exec2",
        ":raft_dummy",
        ":runtime",
        ":physical_plan2",
        ":logical_plan",
        ":mem_row",
        "//external:tcmalloc_and_profiler",
    ],
    linkstatic = True,
)

cc_library(
    name = "runtime",
    srcs = glob(["src/runtime/*.cpp"]),

    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    copts = COPTS,
    includes = [
        "include/runtime",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:arrow",
        "//external:croaring",
        ":reverse",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "reverse",
    srcs = glob(["src/reverse/*.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
    ]),
    copts = COPTS,
    includes = [
        "include/reverse",
        "include/reverse/boolean_engine",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "//external:brpc",
        "//external:bthread",
        "//external:rocksdb",
        "//external:arrow",
        "//external:croaring",
        "//external:rapidjson",
        ":common",
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "capture",
    srcs = glob(["src/tools/baikal_capturer.cpp"]),
    hdrs = glob([
        "include/**/*.h",
        "include/**/*.hpp",
        "src/tools/*.h",
    ]),
    copts = COPTS,
    includes = [
        "src/tools",
    ],
    deps = [
        ":cc_baikaldb_internal_proto",
        "@boost//:lexical_cast",
        "@boost//:heap",
        "//external:brpc",
        "//external:bthread",
        "//external:rocksdb",
        "//external:croaring",
        "//external:rapidjson",
        ":common",
    ],
    visibility = ["//visibility:public"],
)

baikaldb_proto_library(
    name = "cc_baikaldb_internal_proto",
    srcs = glob([
        "proto/*.proto",
    ]),
    include = "proto",
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "test_date_time",
    srcs = ["test/test_date_time.cpp"],
    copts = ["-Iexternal/gtest/include"],
    deps = [
        ":common",
    ],
)
