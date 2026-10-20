# Owned C-message experiment

This is a design probe, not transparent IPC support for rclcpp/rclpy/rclrs. The
native publisher is C++ code calling rcl with a generated C `sensor_msgs/Image`.
The Python consumer calls the existing generated C-to-Python converter. Both
endpoints belong to one native rcl context. It is not an rclcpp Publisher or an
rclpy Node integration; those wrappers and their mixed-container context borrowing
are designed in [design.md](DESIGN.md).

## Reproduction and build

Run from the wp4 workspace. All colcon builds use its build/install/log directories;
no files in the main build/install are modified. `env.sh` can inject package-ignore
or build defaults, so clear `COLCON_DEFAULTS_FILE` explicitly.

The original reproduction was built with `WP4_IPC=OFF`, before editing rcl. It used
the unmodified installed rcl. That probe and its librcl were frozen in
`results/baseline/lib`; `results/baseline-functional.json` records the first
successful reproduction and `baseline-functional-copies.*` records the two CDR
copies plus Python conversion. The first attempted timing run was refused because
load exceeded 2. Functional runs at high load do not report latencies.

To rebuild that RMW-route probe from the final source, use a separate baseline
build/install and source only the main underlay. `WP4_IPC=OFF` excludes all new APIs:

```bash
source install/setup.bash
export COLCON_DEFAULTS_FILE=/dev/null MAKEFLAGS='-j2 -l2'
colcon --log-base "$PWD/log-baseline" build --symlink-install \
  --base-paths ../wt/wp4_rcl/rcl_ipc_probe \
  --build-base "$PWD/build-baseline" --install-base "$PWD/install-baseline" \
  --packages-select rcl_ipc_probe \
  --cmake-args -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON -DWP4_IPC=OFF
```

Build the prototype with:

```bash
source install/setup.bash
export COLCON_DEFAULTS_FILE=/dev/null MAKEFLAGS='-j2 -l2'
colcon --log-base "$PWD/log" build --symlink-install \
  --base-paths ../wt/wp4_rcl --build-base "$PWD/build" --install-base "$PWD/install" \
  --packages-select rcl rcl_executor rcl_ipc_probe \
  --cmake-clean-cache \
  --cmake-args -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_TESTING=ON -DWP4_IPC=ON
source "$PWD/install/setup.bash"
```

Clear stale CMake caches when changing between the underlay and overlay. In
particular `rcl_DIR` for the IPC probe must resolve to `wp4/install/rcl`, not to the
main installation. The two probe binaries use the same source except for the
compile-time route selection.

## Measurements

`bash prototype/measure.sh final` alternates baseline/prototype three times using
the frozen baseline binaries and the completed prototype. It sets:

```bash
ROS_DOMAIN_ID=175
RMW_IMPLEMENTATION=rmw_fastrtps_cpp
FASTDDS_BUILTIN_TRANSPORTS=SHM
ROS_LOG_DIR="$PWD/roslog"
ROS_SHM_LANE=0
```

Fast DDS SHM is used because sandbox interface enumeration can fail. Even the
baseline has zero socket bytes; this experiment removes CDR copies within a
process, not socket transport. `ROS_SHM_LANE=0` explicitly excludes the parked lane.

The driver refuses timing unless the one-minute load is below 2 at both ends.
Each run has 20 warmups and 200 measured 1 MiB messages. libmemcount is reset after
warmup and counts payload-sized memcpy/memmove calls. All bytes and sequence
numbers are checked, and a Python object retained from warmup is checked after
all native message storage has been released.

Start: native monotonic timestamp after allocation and payload fill, just before
publication. End: generated Python conversion has returned. Thus allocation/fill,
payload verification and Python user callback work are excluded. `native_*` ends
when ctypes has obtained the C pointer, before Python conversion; it includes FFI
calls and executor wait/claim. The caller publishes then takes on one thread, so
these are synchronous handoff numbers, not independent-thread scheduling latency.
The GTest separately checks waking a sleeping executor on another thread.

## Validation

```bash
export ROS_DOMAIN_ID=175 RMW_IMPLEMENTATION=rmw_fastrtps_cpp
export FASTDDS_BUILTIN_TRANSPORTS=SHM ROS_LOG_DIR="$PWD/roslog" ROS_SHM_LANE=0
colcon --log-base "$PWD/log" test --base-paths ../wt/wp4_rcl \
  --build-base "$PWD/build" --install-base "$PWD/install" \
  --packages-select rcl_ipc_probe rcl_executor --return-code-on-test-failure
colcon --log-base "$PWD/log" test --base-paths ../wt/wp4_rcl \
  --build-base "$PWD/build" --install-base "$PWD/install" \
  --packages-select rcl --return-code-on-test-failure \
  --ctest-args -R 'test_(publisher|subscription|get_actual_qos)__rmw_fastrtps_cpp$'
python3 prototype/abi.py
```

`abi.py` compares public sizes/alignments/offsets with headers archived from the
baseline revision. `results/abi-symbols.txt` compares defined dynamic symbols from
the frozen and new librcl. The old probe is also run against the new librcl; its
resolved library path is in `results/abi-old-binary-ldd.txt`.

The type-hash negative test uses the private subscription impl to inject a mismatch:
Fast DDS rejects conflicting semantic types on the same topic before that rcl
comparison can otherwise be reached. The readiness tests handle the executor's
documented INTERRUPTED result and retry collection. These are harness requirements,
not changes to RMW or the common executor.

## Limits

The queue is an explicit owned C-pointer handoff, KEEP_LAST(1), volatile, reliable,
with one pair and no concurrent destruction. Endpoints remain ordinary RMW
endpoints; the experiment deliberately calls the local route instead of
`rcl_publish` for this pair. It must not be used as an automatic publisher fast
path when external or IPC-disabled readers exist. No hybrid suppression, C++
struct adapter, fanout, services or generic-message implementation is included.
The [design](DESIGN.md) specifies those contracts and production gates.
