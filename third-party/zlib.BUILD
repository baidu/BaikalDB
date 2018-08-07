licenses(["notice"])  # BSD/MIT-like license (for zlib)

# Modified from https://github.com/tensorflow/tensorflow/blob/master/zlib.BUILD
#               https://github.com/smyte/smyte-db/edit/master/third_party/zlib.BUILD

cc_library(
    name = "zlib",
    srcs = [
        "adler32.c",
        "compress.c",
        "crc32.c",
        "crc32.h",
        "deflate.c",
        "deflate.h",
        "gzclose.c",
        "gzguts.h",
        "gzlib.c",
        "gzread.c",
        "gzwrite.c",
        "infback.c",
        "inffast.c",
        "inffast.h",
        "inffixed.h",
        "inflate.c",
        "inflate.h",
        "inftrees.c",
        "inftrees.h",
        "trees.c",
        "trees.h",
        "uncompr.c",
        "zconf.h",
        "zutil.c",
        "zutil.h",
    ],
    hdrs = [
        "zlib.h",
    ],
    includes = [
        ".",
    ],
    copts = [
      "-D_LARGEFILE64_SOURCE=1",
    ],
    visibility = ["//visibility:public"],
)