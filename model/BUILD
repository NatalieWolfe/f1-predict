load("@rules_cc//cc:defs.bzl", "cc_binary")

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
        "  $(SRCS)"
    ),
    tools = [":generate_training_files"],
)

cc_binary(
    name = "generate_training_files",
    srcs = ["generate_training_files.cc"],
    deps = [
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
        "//data:constants_cc_proto",
        "//data:proto_utils",
        "//data:race_results_cc_proto",
        "@abseil-cpp//absl/strings",
        "@protobuf//:duration_cc_proto",
    ],
)
