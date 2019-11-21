licenses(["notice"])  # Apache v2
# @see: https://github.com/bazelbuild/bazel/issues/818
# @see: https://github.com/envoyproxy/envoy/issues/1069
genrule(
    name = "build",
    srcs = glob(["**/*"]),
    outs = [
        "src/gperftools/tcmalloc.h",
        ".libs/libtcmalloc_and_profiler.a",
    ],
    cmd = "\n".join([
        "gperf=\"external/com_github_gperftools_gperftools\"",
        "outs=($(OUTS))",
        "cp -r $${gperf} build",
        "(cd build" +
        " && ./autogen.sh >/dev/null 2>&1" +
        " && ./configure -q --enable-frame-pointers --disable-libunwind" +
        " && make --silent libtcmalloc_and_profiler.la)",
        "cp build/src/gperftools/tcmalloc.h $(location src/gperftools/tcmalloc.h)",
        "cp build/.libs/libtcmalloc_and_profiler.a  $(location .libs/libtcmalloc_and_profiler.a)",
    ]),
)

cc_library(
    name = "tcmalloc_and_profiler",
    srcs = [".libs/libtcmalloc_and_profiler.a"],
    hdrs = glob(["src/gperftools/*.h"]) +
                ["src/gperftools/tcmalloc.h"], 
    strip_include_prefix = "src",                                                                      
    linkopts = ["-pthread",],
    linkstatic = True,
    visibility = ["//visibility:public"],
)

