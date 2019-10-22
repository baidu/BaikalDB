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
    "-DNEW_PARSER",
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
        ":cc_baikaldb_internal_proto",
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
        ":cc_baikaldb_internal_proto",
        ":common",
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
        ":cc_baikaldb_internal_proto",
        ":common",
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
        "@boost//:lexical_cast",
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
        "@boost//:lexical_cast",
        "@boost//:filesystem",
        "@boost//:algorithm",
        "//external:braft",
        ":cc_baikaldb_internal_proto",
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
        "@boost//:lexical_cast",
        "//external:bthread",
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
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        ":expr",
        ":session",
        ":protocol",
        ":runtime", 
        ":engine", 
        ":physical_plan",
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
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        ":expr",
        ":session",
        ":engine",
        ":runtime",
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
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
        ":sqlparser",
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
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
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
        "@boost//:thread",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        ":exec2",
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
        "//external:rocksdb",
        "//external:rapidjson",
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
    deps = [
        ":meta_server",
        ":cc_baikaldb_internal_proto",
        ":common",
        ":engine",
        ":raft",
        ":raft_meta",
        "@boost//:filesystem",
    ],
    linkstatic = True,
)

cc_binary(
    name = "baikalStore",
    srcs = ["src/store/main.cpp"],
    includes = [
        "include/store",
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
        "//external:bvar",
        "//external:bthread",
        "//external:brpc",
        "//external:rocksdb",
        "//external:rapidjson",
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
    ],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "baikaldb",
    srcs = ["src/protocol/main.cpp"],
    copts = COPTS,
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
        "//external:rocksdb",
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
        "//external:bthread",
        "//external:rocksdb",
        "//external:rapidjson",
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
