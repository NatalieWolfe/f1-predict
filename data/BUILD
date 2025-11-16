load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
load("//model:prediction.bzl", "genprediction")

_RESULTS_RACES = [path.split("/") for path in set([
    "/".join(path.split("/")[1:-1])
    for path in glob(["results/**/*.textproto"])
])]

filegroup(
    name = "results",
    srcs = glob(["results/**/*.textproto"]),
    data = glob(["results/**/*.textproto"]),
    visibility = ["//model:__subpackages__"],
)

[
    filegroup(
        name = "%s_%s_results" % (year, circuit),
        srcs = glob(["results/%s/%s/*.textproto" % (year, circuit)]),
        data = glob(["results/%s/%s/*.textproto" % (year, circuit)]),
        visibility = ["//model:__subpackages__"],
    )
    for (year, circuit) in _RESULTS_RACES
]

[
    genprediction(
        name = "%s_%s_prediction" % (year, circuit),
        circuit = circuit,
        year = year,
    )
    for (year, circuit) in _RESULTS_RACES
]

proto_library(
    name = "constants_proto",
    srcs = ["constants.proto"],
)

cc_proto_library(
    name = "constants_cc_proto",
    visibility = ["//model:__subpackages__"],
    deps = [":constants_proto"],
)

cc_library(
    name = "constants_maps",
    srcs = ["constants_maps.cc"],
    hdrs = ["constants_maps.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":constants_cc_proto",
        "//strings:parse",
    ],
)

cc_library(
    name = "csv",
    srcs = ["csv.cc"],
    hdrs = ["csv.h"],
    visibility = ["//visibility:public"],
    deps = ["//strings:trim"],
)

proto_library(
    name = "race_results_proto",
    srcs = ["race_results.proto"],
    deps = [
        ":constants_proto",
        "@protobuf//:duration_proto",
    ],
)

cc_proto_library(
    name = "race_results_cc_proto",
    visibility = ["//model:__subpackages__"],
    deps = [":race_results_proto"],
)

cc_binary(
    name = "importer",
    srcs = ["importer.cc"],
    deps = [
        ":constants_cc_proto",
        ":constants_maps",
        ":csv",
        ":proto_utils",
        ":race_results_cc_proto",
        "//strings:parse",
        "//strings:trim",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
    ],
)

cc_binary(
    name = "insert",
    srcs = ["insert.cc"],
    deps = [
        ":constants_cc_proto",
        ":proto_utils",
        ":race_results_cc_proto",
        "//cli:autocomplete",
        "//strings:parse",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
        "@abseil-cpp//absl/strings",
        "@protobuf",
    ],
)

cc_binary(
    name = "kaggle_importer",
    srcs = ["kaggle_importer.cc"],
    deps = [
        ":constants_cc_proto",
        ":constants_maps",
        ":csv",
        ":proto_utils",
        ":race_results_cc_proto",
        "//strings:parse",
        "//strings:trim",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
        "@protobuf",
        "@protobuf//:duration_cc_proto",
    ],
)

cc_library(
    name = "proto_utils",
    srcs = ["proto_utils.cc"],
    hdrs = ["proto_utils.h"],
    visibility = ["//model:__subpackages__"],
    deps = [
        ":race_results_cc_proto",
        "@protobuf",
        "@protobuf//:duration_cc_proto",
        "@protobuf//:time_util",
    ],
)
