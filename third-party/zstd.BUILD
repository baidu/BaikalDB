licenses(["notice"])  # Apache v2 

#@see: https://github.com/cschuet/zstd
package(default_visibility = ["//visibility:public"])

cc_library(
    name = "zstd",
    hdrs = glob(["lib/**/*.h"]),
    srcs = glob(["lib/**/*.c"]),
    copts = [
        "-DZSTD_LEGACY_SUPPORT=5",
        "-DXXH_NAMESPACE=ZSTD_",
    ],
    strip_include_prefix = "lib",
    deps = [
        ":zstd_header",
        ":common",
        ":compress",
        ":decompress",
        ":deprecated",
        ":zdict",
        ":legacy",
    ],
)

cc_library(
    name = "zdict",
    hdrs = glob(["lib/dictBuilder/*.h"]),
    srcs = glob(["lib/dictBuilder/*.c"]),
    copts = [
        "-DZSTD_LEGACY_SUPPORT=5",
        "-DXXH_NAMESPACE=ZSTD_",
    ],
    strip_include_prefix = "lib/dictBuilder",
    deps = [":common"],
)

cc_library(
    name = "legacy",
    hdrs = glob(["lib/legacy/*.h"]),
    srcs = glob(["lib/legacy/*.c"]),
    copts = [
        "-DZSTD_LEGACY_SUPPORT=5",
        "-DXXH_NAMESPACE=ZSTD_",
    ],
    strip_include_prefix = "lib/legacy",
    deps = [":common"],
)
cc_library(
    name = "decompress",
    hdrs = glob(["lib/decompress/*.h"]),
    srcs = glob(["lib/decompress/*.c"]),
    strip_include_prefix = "lib/decompress",
    copts = [
        "-DXXH_NAMESPACE=ZSTD_",
        "-DZSTD_LEGACY_SUPPORT=5",
    ],
    deps = [
        ":common",
        ":legacy",
    ],
)

cc_library(
    name = "deprecated",
    hdrs = glob(["lib/deprecated/*.h"]),
    srcs = glob(["lib/deprecated/*.c"]),
    strip_include_prefix = "lib/deprecated",
    copts = [
        "-DXXH_NAMESPACE=ZSTD_",
        "-DZSTD_LEGACY_SUPPORT=5",
    ],
    deps = [":common"],
)

cc_library(
    name = "compress",
    hdrs = glob(["lib/compress/*.h"]),
    srcs = glob(["lib/compress/*.c"]),
    copts = [
        "-DZSTD_LEGACY_SUPPORT=5",
        "-DXXH_NAMESPACE=ZSTD_",
    ],
    strip_include_prefix = "lib/compress",
    deps = [":common"],
)

cc_library(
    name = "common",
    hdrs = glob(["lib/common/*.h"]),
    srcs = glob(["lib/common/*.c"]),
    strip_include_prefix = "lib/common",
    copts = [
        "-DZSTD_LEGACY_SUPPORT=5",
        "-DXXH_NAMESPACE=ZSTD_",
    ],
    deps = [":zstd_header"],
)

cc_library(
    name = "zstd_header",
    hdrs = ["lib/zstd.h"],
    strip_include_prefix = "lib",
)
cc_library(
    name = "threading",
    hdrs = ["lib/common/threading.h"],
    srcs = ["lib/common/threading.c"],
    linkopts = ["-pthread"],
    copts = ["-DZSTD_MULTITHREAD"],
    deps = [":debug"],
)
