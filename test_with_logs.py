#!/usr/bin/env python3

import os
import subprocess
import shutil

cargo_bin = shutil.which("cargo")


def list_tests():
    output = subprocess.check_output(
        [cargo_bin, "test", "--", "--list"], stderr=subprocess.DEVNULL
    )
    for line in output.decode().splitlines():
        if not line:
            return
        yield line.removesuffix(": test")


def run_test_with_logging(name):
    env = {}
    env.update(os.environ)
    env["RUST_LOG"] = "trace"
    subprocess.check_call(
        [cargo_bin, "test", name, "--", "--nocapture", "--exact"],
        env=env,
    )


for test in list_tests():
    run_test_with_logging(test)
