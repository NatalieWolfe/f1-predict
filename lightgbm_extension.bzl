"""A bzlmod extension adding the LightGBM GitHub repository."""

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

_BUILD_FILE_CONTENT = """
filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
"""

def _lightgbm_extension_impl(_ctx):
    git_repository(
        name = "lightgbm",
        build_file_content = _BUILD_FILE_CONTENT,
        init_submodules = True,
        recursive_init_submodules = True,
        remote = "https://github.com/microsoft/LightGBM",
        # strip_prefix,
        # tag = "v4.6.0",
        commit = "d02a01ac6f51d36c9e62388243bcb75c3b1b1774",
    )

lightgbm_extension = module_extension(
    implementation = _lightgbm_extension_impl,
)
