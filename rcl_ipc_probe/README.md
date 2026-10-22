# rcl broker tests and owned C-message probe

The package name is historical. The context-owned rcl broker that started as the
probe here now ships in rcl itself; this package holds its tests and the original
probe. The design record is [DESIGN.md](DESIGN.md).

## The broker

`RCL_INTRA_PROCESS=1` (default off, read when a context is initialized) makes rcl
deliver topic, service and action messages between endpoints of the same rcl context
without serialization. The broker is created only when the active RMW exports
`rmw_get_intra_process_api_v1` (see `rmw/intra_process.h`); in this workspace that
is `rmw_zenoh_cpp`. With any other RMW, or with the variable unset, rcl behaves as
in Jazzy.

| Source | Content |
| --- | --- |
| `rcl/include/rcl/intra_process.h` | Owned and adapted publish, copy-free takes, per-endpoint opt-out, serialized ingress, service and client buffers |
| `rcl/src/rcl/intra_process.cpp` | Topic broker: matching, per-reader queues, conversions, executor and wait-set readiness |
| `rcl/src/rcl/intra_process_service.cpp` | Service and client request and response routing |
| `rcl/include/rcl/ipc_prototype.h`, `rcl/src/rcl/ipc_prototype.cpp` | The first single-pair prototype API, kept for the probe below |

Only endpoints with KEEP_LAST (depth > 0), VOLATILE, no deadline, lifespan or
liveliness lease, and liveliness other than MANUAL_BY_TOPIC join the broker.
Subscriptions with a content filter or `ignore_local_publications` stay on the RMW
path. Readers in another context or process always receive through the RMW; the
publication tells the RMW which readers the broker already served so that each
reader gets a message once.

## Contents

| File | Built | Purpose |
| --- | --- | --- |
| `test_broker.cpp` | `BUILD_TESTING=ON` | Topic broker: depth and writer order, typed and serialized takes, conversions, QoS fallback, separate contexts, readiness, adapted publications |
| `test_services.cpp` | `BUILD_TESTING=ON` | Service broker: deferred and out-of-order responses, endpoint destruction, owned requests and borrowed responses |
| `probe.cpp`, `bench.py` | always | Native C++ publisher of a generated C `sensor_msgs/Image` read by the generated Python converter. With `-DWP4_IPC=ON` the image goes through `rcl/ipc_prototype.h`, otherwise through `rcl_publish` and the RMW. `bench.py --library <path>` loads either build |
| `test_ipc.cpp` | `BUILD_TESTING=ON` and `-DWP4_IPC=ON` | Tests of the prototype API |

## Build and test

```bash
colcon build --symlink-install --packages-select rcl_ipc_probe \
  --cmake-args -DBUILD_TESTING=ON
RMW_IMPLEMENTATION=rmw_zenoh_cpp colcon test --packages-select rcl_ipc_probe \
  --return-code-on-test-failure
```

The broker tests set `RCL_INTRA_PROCESS=1` themselves and need an RMW that
implements the routing extension. Run a Zenoh router for them as for any other
`rmw_zenoh_cpp` test.

## The prototype

The prototype answered one question before the broker existed: can a generated C
message cross from native C++ to Python inside one rcl context without
serialization, keep correct ownership, and wake the common executor? It is a single
pair with KEEP_LAST(1), volatile and reliable endpoints, an explicit owned C-pointer
handoff, and no fanout, services or RMW suppression. It does not intercept
`rcl_publish`. The Python side still converts once into an independent mutable
message. Do not use `rcl/ipc_prototype.h` in new code; use `rcl/intra_process.h`
through rclcpp or rclpy.
