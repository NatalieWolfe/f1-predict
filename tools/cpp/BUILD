package(default_visibility = ["//visibility:public"])

cc_toolchain_suite(
    name = "gcc_toolchain_suite",
    toolchains = {
        "k8|linux": ":gcc_14_toolchain",
        "k8|linux|gcc": ":gcc_14_toolchain",
        "x86_64|linux": ":gcc_14_toolchain",
        "x86_64|linux|gcc": ":gcc_14_toolchain",
    },
)

cc_toolchain(
    name = "gcc_14_toolchain",
    all_files = ":empty",
    compiler_files = ":empty",
    dwp_files = ":empty",
    linker_files = ":empty",
    objcopy_files = ":empty",
    strip_files = ":empty",
    supports_param_files = True,
    toolchain_config = ":gcc_14_crosstool",
    toolchain_identifier = "gcc-14-linux",
)

filegroup(
    name = "gcc_14_crosstool",
    srcs = ["CROSSTOOL"],
)

filegroup(
    name = "empty",
    srcs = [],
)
