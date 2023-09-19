licenses(["notice"])

cc_library(
    name = "faiss",
    srcs = glob([
        "faiss/*.cpp",
        "faiss/impl/*.cpp",
        "faiss/impl/**/*.cpp",
        "faiss/utils/*.cpp",
        "faiss/utils/**/*.cpp",
        "faiss/invlists/*.cpp",
    ]),
    hdrs = glob([
        "faiss/*.h",
        "faiss/impl/*.h",
        "faiss/impl/**/*.h",
        "faiss/utils/*.h",
        "faiss/utils/**/*.h",
        "faiss/invlists/*.h",
    ]),
    includes = [
        ".",
        "include",
        "faiss",
    ],
    defines = [
        "OS_LINUX",
        "FINTEGER=int"
    ],
    copts = [
        "-Wno-unused-parameter",
        "-Wno-sign-compare",
        "-Wno-unused-variable",
        "-Wno-unused-function",
        "-Wno-type-limits",
        "-Wno-implicit-fallthrough",
        "-Wno-maybe-uninitialized",
        "-pipe",
        "-fPIC",
        "-fopenmp",
        "-mavx2",
        "-mfma",
        "-mf16c",
        "-mpopcnt",
        "-O2",
    ],
    linkopts = [
        "-fopenmp",
        "-ldl",
        "-lpthread",
    ],
    deps = [
        "//external:openblas",
    ],
    visibility = ["//visibility:public"],
)

