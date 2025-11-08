""""""

load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load(
    "@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)

def _impl(ctx):
    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        toolchain_identifier = "k8-toolchain",
        host_system_name = "local",
        target_system_name = "local",
        target_cpu = "k8",
        target_libc = "unknown",
        compiler = "gcc-14",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        tool_paths = [
            tool_path(
                name = "gcc",
                path = "/usr/bin/gcc-14",
            ),
            tool_path(
                name = "ld",
                path = "/usr/bin/ld",
            ),
            tool_path(
                name = "ar",
                path = "/usr/bin/ar",
            ),
            tool_path(
                name = "cpp",
                path = "/usr/bin/cpp-14",
            ),
            tool_path(
                name = "gcov",
                path = "/usr/bin/gcov-14",
            ),
            tool_path(
                name = "nm",
                path = "/usr/bin/nm",
            ),
            tool_path(
                name = "objdump",
                path = "/usr/bin/objdump",
            ),
            tool_path(
                name = "strip",
                path = "/usr/bin/strip",
            ),
        ],
        # compiler_built_in_include_directories = [
        #     "/usr/lib/gcc/x86_64-linux-gnu/14/include",
        # ],
        cxx_builtin_include_directories = [
            "/usr/lib/gcc/x86_64-linux-gnu/14/include",
            "/usr/include/c++/14",
            "/usr/include/x86_64-linux-gnu/c++/14",
            "/usr/include/c++/14/backward",
            "/usr/local/include",
            "/usr/include/x86_64-linux-gnu",
            "/usr/include",
        ],
        features = [
            feature(
                name = "default_compiler_flags",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = [ACTION_NAMES.cpp_compile],
                        flag_groups = ([flag_group(flags = ["-std=c++23"])]),
                    ),
                ],
            ),
            feature(
                name = "default_linker_flags",
                enabled = True,
                flag_sets = [
                    flag_set(
                        actions = [
                            ACTION_NAMES.cpp_link_executable,
                            ACTION_NAMES.cpp_link_dynamic_library,
                            ACTION_NAMES.cpp_link_nodeps_dynamic_library,
                        ],
                        flag_groups = ([
                            flag_group(
                                flags = [
                                    "-lstdc++",
                                    "-std=c++23",
                                ],
                            ),
                        ]),
                    ),
                ],
            ),
        ],
    )

cc_toolchain_config = rule(
    implementation = _impl,
    attrs = {},
    provides = [CcToolchainConfigInfo],
)
