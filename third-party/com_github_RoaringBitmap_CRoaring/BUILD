licenses(["notice"])

cc_library(
    name = "croaring",
    srcs = [
        "src/array_util.c",
        "src/bitset_util.c",
        "src/containers/array.c",
        "src/containers/bitset.c",
        "src/containers/containers.c",
        "src/containers/convert.c",
        "src/containers/mixed_intersection.c",
        "src/containers/mixed_union.c",
        "src/containers/mixed_equal.c",
        "src/containers/mixed_subset.c",
        "src/containers/mixed_negation.c",
        "src/containers/mixed_xor.c",
        "src/containers/mixed_andnot.c",
        "src/containers/run.c",
        "src/roaring.c",
        "src/roaring_priority_queue.c",
        "src/roaring_array.c",
    ],
    hdrs = glob([
        "include/roaring/**/*.h",
        "include/roaring/*.h",
        "cpp/*.hh",
    ]),
    includes = [
        ".",
        "include",
        "cpp",
    ],
    defines = [
        "OS_LINUX",
    ],
    copts = [
        "-fno-omit-frame-pointer",
        "-momit-leaf-frame-pointer",
        "-msse",
        "-msse4.2",
        "-Werror",
        "-mpclmul",
        "-O2",
        "-std=c99",
    ],
    visibility = ["//visibility:public"],
)

