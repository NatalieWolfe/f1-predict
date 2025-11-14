load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

cmake(
    name = "lightgbm",
    build_args = ["--parallel=8"],
    cache_entries = {
        "BUILD_CLI": "ON",
        "BUILD_STATIC_LIB": "ON",
        "USE_MPI": "OFF",
        "USE_GPU": "OFF",
        "USE_OPENMP": "ON",
    },
    lib_source = "@lightgbm//:srcs",
    out_binaries = ["lightgbm"],
    out_static_libs = ["lib_lightgbm.a"],
    targets = [
        "_lightgbm",
        "lightgbm",
    ],
)

filegroup(
    name = "binary",
    srcs = [":lightgbm"],
    output_group = "lightgbm",
    visibility = ["//visibility:public"],
)
