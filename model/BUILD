load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

genrule(
    name = "training_data",
    srcs = ["//data:results"],
    outs = [
        "training.csv",
        "tests.csv",
    ],
    cmd = (
        "OUT_ARRAY=($(OUTS))\n" +
        "TRAINING_FILE=$${OUT_ARRAY[0]}\n" +
        "TESTS_FILE=$${OUT_ARRAY[1]}\n" +
        "$(location :generate_training_files)" +
        "  --training_file=$${TRAINING_FILE}" +
        "  --tests_file=$${TESTS_FILE}" +
        "  --results_dir=data/results"
    ),
    tools = [":generate_training_files"],
)

genrule(
    name = "f1_lambdarank_model",
    srcs = [
        ":training.csv",
        ":tests.csv",
        ":training.conf",
    ],
    outs = ["f1_lambdarank_model.txt"],
    cmd = (
        "$(location //third_party/lightgbm:binary)" +
        "  config=$(location :training.conf)" +
        "  output_model=$(OUTS)" +
        "  train=$(location :training.csv)" +
        "  valid=$(location :tests.csv)" +
        "  2>&1" +
        "  | tail"
    ),
    tools = ["//third_party/lightgbm:binary"],
)

cc_library(
    name = "data_aggregates",
    hdrs = ["data_aggregates.h"],
    deps = ["//data:constants_cc_proto"],
)

cc_binary(
    name = "generate_training_files",
    srcs = ["generate_training_files.cc"],
    deps = [
        ":data_aggregates",
        ":writer",
        "//data:constants_cc_proto",
        "//data:proto_utils",
        "//data:race_results_cc_proto",
        "@abseil-cpp//absl/flags:flag",
        "@abseil-cpp//absl/flags:parse",
        "@abseil-cpp//absl/random",
    ],
)

cc_library(
    name = "writer",
    srcs = ["writer.cc"],
    hdrs = ["writer.h"],
    deps = [
        ":data_aggregates",
        "//data:constants_cc_proto",
        "//data:proto_utils",
        "//data:race_results_cc_proto",
        "@abseil-cpp//absl/strings",
        "@protobuf//:duration_cc_proto",
    ],
)
