#!/usr/bin/env python3
# Copyright 2026 nev
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Measure native C Image publication to a generated Python Image."""

import argparse
import ctypes
import json
import os
import statistics
import time

from sensor_msgs.msg import Image


def main():
    """Run a single route, verifying all payloads and retained Python objects."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--library', required=True)
    parser.add_argument('--samples', type=int, default=200)
    parser.add_argument('--bytes', type=int, default=1024 * 1024)
    parser.add_argument('--functional', action='store_true')
    args = parser.parse_args()
    start_load = os.getloadavg()[0]
    if not args.functional:
        assert start_load < 2, start_load
    Image.__class__.__import_type_support__()
    capsule_pointer = ctypes.pythonapi.PyCapsule_GetPointer
    capsule_pointer.restype = ctypes.c_void_p
    capsule_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
    converter = ctypes.PYFUNCTYPE(ctypes.py_object, ctypes.c_void_p)(
        capsule_pointer(Image.__class__._CONVERT_TO_PY, None))
    lib = ctypes.CDLL(args.library)
    lib.wp4_publish.argtypes = [ctypes.c_size_t, ctypes.c_uint32]
    lib.wp4_publish.restype = ctypes.c_int64
    lib.wp4_take.restype = ctypes.c_void_p
    assert lib.wp4_init() == 0
    reset = ctypes.CDLL(None).memcount_reset
    latencies = []
    native_latencies = []
    retained = None
    for i in range(20 + args.samples):
        if i == 20:
            reset()
        published = lib.wp4_publish(args.bytes, i)
        assert published > 0
        pointer = lib.wp4_take()
        assert pointer, i
        taken = time.monotonic_ns()
        message = converter(pointer)
        elapsed = time.monotonic_ns() - published
        assert message.height == i
        assert len(message.data) == args.bytes
        assert message.data.count(42) == args.bytes
        lib.wp4_release()
        if i == 19:
            retained = message
        if i >= 20:
            latencies.append(elapsed / 1000)
            native_latencies.append((taken - published) / 1000)
    assert retained.height == 19 and retained.data.count(42) == args.bytes
    end_load = os.getloadavg()[0]
    if not args.functional:
        assert end_load < 2, end_load
    print(json.dumps({
        'samples': args.samples, 'bytes': args.bytes,
        'load_start': start_load, 'load_end': end_load,
        'timing_valid': not args.functional,
        'p50_us': None if args.functional else statistics.median(latencies),
        'p99_us': None if args.functional else sorted(latencies)[
            int(len(latencies) * .99) - 1],
        'native_p50_us': None if args.functional else statistics.median(native_latencies),
        'native_p99_us': None if args.functional else sorted(native_latencies)[
            int(len(native_latencies) * .99) - 1]}))
    lib.wp4_fini()


if __name__ == '__main__':
    main()
