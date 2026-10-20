# wp4 result

The English [design](DESIGN.md) covers context matching, language representation
adapters, immutable/unique ownership, QoS, ordering, duplicate suppression, IPM
migration, services, serialized/generic subscriptions, Jazzy ABI and production tests.
The prototype is restricted to an explicit owned generated-C `Image` handoff from
native C++ code to Python, notified through `rcl_executor_add_source`.
It is not transparent rclcpp/rclpy IPC or a mixed component container.

## Reproduction

Before modifying rcl, the unmodified RMW path delivered 200 correct 1 MiB Images
but incurred 400 payload-sized Fast CDR copies and 200 Python conversion copies.
The initial successful reproduction is `baseline-functional.json` and its
`baseline-functional-copies.*` file. Load was 5.92 -> 5.61; no timing was reported.
The original probe and librcl were frozen before the prototype changes.

Afterward the same driver delivered every message and preserved a retained Python
message, with only the 200 Python conversion copies. No Fast CDR copies occurred.
The native queue transfers the original pointer; the generated Python converter
creates an independent mutable Python message. Zero-copy native handoff is not
zero-copy delivery into the legacy Python object API.

## Final alternating measurements

Domain 175, Fast DDS SHM, parked lane disabled, RelWithDebInfo. Three alternating
baseline/prototype pairs, 20 warmups and 200 measured 1 MiB messages per run.
Every run passed sequence, full-payload and retained-object checks. All start/end
one-minute loads were below 2. Values are microseconds.

| Run | Route | Load start/end | Python object p50 | Python object p99 | C-pointer p50 | C-pointer p99 | Payload copies/message |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | baseline | 1.598 / 1.598 | 126.754 | 294.696 | 74.068 | 89.658 | 3 |
| 1 | prototype | 1.598 / 1.630 | 42.871 | 268.045 | 0.872 | 2.615 | 1 |
| 2 | baseline | 1.630 / 1.630 | 129.487 | 312.519 | 75.522 | 85.663 | 3 |
| 2 | prototype | 1.630 / 1.630 | 42.152 | 225.953 | 0.866 | 1.232 | 1 |
| 3 | baseline | 1.630 / 1.630 | 125.644 | 324.088 | 64.927 | 93.186 | 3 |
| 3 | prototype | 1.630 / 1.630 | 42.395 | 229.696 | 0.961 | 1.392 | 1 |

Median of the three per-run end-to-end medians: **126.754 -> 42.395 us**,
about 2.99x faster for this slice. This is not a pooled percentile. The C-pointer
median-of-medians is 74.068 -> 0.872 us and includes FFI overhead. Publication and
take run synchronously on the Python caller thread; the separate sleeping-executor
GTest validates wakeup, not its latency.

For every measured baseline run libmemcount reports 419,430,400 bytes / 400 calls
in `libfastcdr.so.2` and 209,715,200 bytes / 200 calls in `python3`. For every
prototype run it reports only the latter. Both report zero socket bytes. This
counts large memcpy/memmove calls, not allocations, inline field copies, memset
initialization or all possible backend memory traffic. Allocation/fill and payload
validation are outside the latency interval; generated Python conversion is inside.
Raw files: `final-{baseline,prototype}-{1,2,3}.json` and matching `*-copies.*`.
Earlier valid measurements at loads 0.56-0.60 are retained separately.

## Tests and ABI

* Nine new GTests pass: original pointer handoff and empty take, replacement/destruction
  release once, owner surviving endpoint destruction, context/topic/type rejection,
  unsupported depth, sleeping executor wake/rearm, and callback reentrancy outside locks.
* Existing common executor: 15 scheduler GTests, one trace GTest and its C API executable pass.
* Existing rcl Fast DDS regressions: 18 publisher, 27 subscription and six actual-QoS
  GTests pass. Overall colcon test-result: **83 tests, zero failures/errors/skips**
  (includes CTest wrapper entries; 76 are GTest cases).
* Changed/new code passes uncrustify, cpplint, flake8 and pep257.
* `nm -D --defined-only`: no removed symbols; exactly four new
  `rcl_ipc_prototype_{create,publish,take,destroy}` symbols.
* Public sizes/alignments/offsets agree with baseline headers, including
  publisher/subscription handles at 8 bytes, context at 24, node at 16,
  publisher options at 152, subscription options at 160 and wait set at 104.
  Only the private subscription impl grows by a context pointer.
* The original pre-change probe also passes 20 measured-size functional deliveries
  against the new librcl, including payload/retention checks; ldd confirms the overlay
  library. No rclcpp, rclpy or rmw source/class layout was changed.

No full RMW matrix, sanitizer stress, QoS completeness, services, representation
cache fanout or mixed-container implementation is claimed. Those are explicit
production gates in the design. Build and test logs remain in this directory and
`../log`; [reproduction commands](README.md) describe the isolated setup.
