cc_library(
    name = "lz4",
    # We include the lz4.c as a header as lz4hc.c actually does #include "lz4.c".
    hdrs = glob(["lib/*.h"]) + ["lib/lz4.c"],
    srcs = glob(["lib/*.c"]),
    visibility = ["//visibility:public"],
    strip_include_prefix = "lib",
)
