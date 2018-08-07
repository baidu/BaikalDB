licenses(["notice"])

# Modified from https://github.com/mzhaom/trunk/blob/master/third_party/snappy/BUILD
#               https://github.com/smyte/smyte-db/blob/master/third_party/snappy.BUILD

genrule(
    name = "snappy_stubs_public_h",
    srcs = [
        "snappy-stubs-public.h.in",
    ],
    outs = [
        "snappy-stubs-public.h",
    ],
    cmd = "sed 's/@ac_cv_have_stdint_h@/1/g' $(<) | " +
        "sed 's/@ac_cv_have_stddef_h@/1/g' | " +
        "sed 's/@ac_cv_have_sys_uio_h@/1/g' | " +
        "sed 's/@SNAPPY_MAJOR@/1/g' | " +
        "sed 's/@SNAPPY_MINOR@/1/g' | " +
        "sed 's/@SNAPPY_PATCHLEVEL@/3/g' >$(@)",
)

cc_library(
    name = "snappy",
    srcs = [
        "snappy-c.cc",
        "snappy-internal.h",
        "snappy-sinksource.cc",
        "snappy-stubs-internal.cc",
        "snappy-stubs-internal.h",
        "snappy.cc",
    ],
    hdrs = [
        ":snappy_stubs_public_h",
        "snappy.h",
        "snappy-c.h",
        "snappy-sinksource.h",
    ],
    includes = [
        "."
    ],
    copts = [
        "-DHAVE_CONFIG_H",
        "-Wno-sign-compare",
    ],
    deps = [
        "//external:snappy_config",
    ],
    visibility = ["//visibility:public"],
)