load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "autocomplete",
    srcs = ["autocomplete.cc"],
    hdrs = ["autocomplete.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":colorize",
        "//strings:levenshtein",
        "@abseil-cpp//absl/strings",
    ],
)

cc_library(
    name = "colorize",
    srcs = ["colorize.cc"],
    hdrs = ["colorize.h"],
    deps = ["@abseil-cpp//absl/strings"],
)
