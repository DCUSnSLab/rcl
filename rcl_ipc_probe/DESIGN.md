# Context-scoped intra-process delivery at rcl

Status: design and an intentionally restricted prototype, not a production transport.
Target: this workspace's Jazzy-based `overhaul` branch. Domain used for all probes: 175.

## Decision and scope

Put endpoint matching, routing, history, message ownership and readiness in rcl. Keep
language-specific representations and callback dispatch in client libraries. Use a
context-owned broker, not a process-global topic registry. Two contexts in one process
are as separate as two processes; sharing a ROS domain does not join their brokers.
The broker supports multiple publishers per topic. There is no one-writer restriction.

An intra-process delivery does not call rmw serialize/deserialize. A publication may
still require serialization for remote or IPC-disabled recipients, or for a callback
whose contract explicitly requests serialized bytes. Those are different consumers
of the same publication. Arbitrary Python objects and C++ messages are not layout
compatible, so universal zero-copy with existing typed APIs is not a credible promise.

The production design requires an optional RMW routing capability described below.
It is additive and feature-negotiated. Without it, preserve the existing RMW path
for pairs for which exactly-once hybrid routing cannot be established. Do not ship
an optimistic graph-count heuristic as a correctness mechanism.

The implemented experiment validates a smaller question: can a generated C message
cross a C++/Python boundary without serialization, retain its correct ownership, and
wake the common executor? It deliberately does not implement transparent routing.

## Evidence and prior work

The complete issue bodies, discussion comments, relevant review comments and PR
patches are archived in [research](/tmp/claude-1000/-home-nev-flux-devel/2098f1b0-2fbe-4a6c-b7c7-9521268b2bb3/scratchpad/codex/wp4/research/). Retrieved with `gh api`, including
paginated comments and PR files. These are historical proposals, not merged APIs.

| Source | Consequence for this design |
| --- | --- |
| [rclcpp#1750](https://github.com/ros2/rclcpp/issues/1750) | Moving down a layer exposes representation, custom allocator, reference counting and QoS work. A DDS notification channel would restore much of the latency being removed. |
| [design#251](https://github.com/ros2/design/issues/251) | All-language IPC and a loan-style API are separate requirements; moving code below C++ alone does not make arbitrary language objects shareable. RMW implementations still own network transport. |
| [design#327](https://github.com/ros2/design/pull/327), [rclcpp#1847](https://github.com/ros2/rclcpp/pull/1847) | Request and response queues can reuse IPC infrastructure. That proposal assumes one local service per name and supports KEEP_LAST/volatile; its open discussion repeatedly requests review. It is not evidence that multiple-service arbitration is solved. |
| [rclcpp#2202](https://github.com/ros2/rclcpp/issues/2202) | The usual double delivery problem includes work done before a duplicate is discarded, even when users see one callback. |
| [rclcpp#2203](https://github.com/ros2/rclcpp/issues/2203) | A middleware loan can only serve both routes if its owner stays alive through both. IPC and loaned publication cannot each consume the sole owner. |
| [rclcpp#2338](https://github.com/ros2/rclcpp/pull/2338), [rcl#1111](https://github.com/ros2/rcl/pull/1111), [rmw#358](https://github.com/ros2/rmw/pull/358) | The proposed non-local count defines local as the same context. Reviews ask for that definition. Counts alone do not suppress duplicate delivery, as #2338's test report explicitly corrects. Same-context IPC-disabled readers still require RMW. |
| [rclcpp#1931](https://github.com/ros2/rclcpp/pull/1931), [rclcpp#3083](https://github.com/ros2/rclcpp/pull/3083) | Serialized ownership and generic subscriptions need explicit representation contracts. #1931 remained in review/rebase/test work; #3083 is a draft and discusses serialization once for multiple generic readers. Serialization is still required by a recorder's byte API. |
| [rclpy#599](https://github.com/ros2/rclpy/pull/599) | A Python component manager is not a mixed-language container. The discussion explicitly says mixed C++/Python components are unsupported. Sharing a context needs an explicit embedding API and lifetime contract. |

### Local code read

* `rclcpp/include/rclcpp/experimental/intra_process_manager.hpp` and
  `rclcpp/src/rclcpp/experimental/intra_process_manager.cpp`: a context subobject
  matches by topic/QoS and partitions recipients into shared and owning consumers.
  `do_intra_process_publish` promotes to shared ownership or copies for owning
  consumers and moves the original to one. It retains publisher history for
  transient-local delivery.
* `experimental/{subscription_intra_process*,ros_message_intra_process_buffer.hpp}`
  and `experimental/buffers/{intra_process_buffer,ring_buffer_implementation}.hpp`:
  per-subscription buffers, shared/unique representations, mutex-protected FIFO,
  overwrite-oldest KEEP_LAST behavior, guard-condition readiness. Existing RMW and
  IPC queues have independent capacity; combining them can double effective depth.
* `subscription_base.cpp::take_type_erased` and
  `matches_any_intra_process_publishers`: the ordinary path can deserialize before
  identifying an IPC publisher. Filtering only by a currently registered publisher
  is also insufficient during route changes.
* `rcl_executor/include/rcl_executor/executor.h` and `src/executor.cpp`:
  `add_source` registers a persistent external WORK item, `notify` adds readiness
  credits, `next` claims a callback group, and `complete` releases that claim.
  Removal cancels queued work; already claimed work must finish. The source API is
  sufficient for the prototype. No new waker thread or DDS notification topic is needed.
* Parked shared-memory design, section 3:
  flat frames, segment epochs, flux ownership and executor notifications are useful
  experience, but this IPC design uses ordinary pointers inside a process. Do not
  revive its single-writer restriction, node-name endpoint matching, count-based
  route decisions, timestamp merge or one-second ordering escape hatch. That record
  documents Cyclone publisher-GID/received-handle mismatch and an actual order
  inversion at route changes. Both are reasons to require a routing protocol.

## Representations and conversion

A type identity is the ROS interface's type hash and encoding/version, not its
C++ RTTI, topic name, message size, or a `void *`. Representation identity is a
separate key: generated C, a particular generated C++ ABI/allocator specialization,
a serialized format, or a registered custom adapter. A type hash proves semantic
type equality; it never proves C/C++ layout equality.

Each publication owns an immutable envelope with:

* context generation, publisher instance ID, monotonic publisher sequence, route epoch;
* source timestamp and local monotonic acceptance timestamp;
* semantic type key, original representation key and owner;
* lazily populated per-representation cache, with a once-only conversion state;
* references from subscriber queues, writer history and any outstanding callbacks.

An owner is an opaque reference-counted C ABI handle. Its operations are retain,
release, get-const-data and, where supported, detach-exclusive. The deleter context
holds the originating allocator, typesupport library and adapter/module lifetime.
Do not pass `std::shared_ptr` or `PyObject` ownership through the public C ABI.
A C++ implementation of the hidden control block is fine.

| Producer / requested representation | Work |
| --- | --- |
| C++ -> same generated C++ ABI | Share const original; no message conversion. A const-reference publish must first copy into owned storage because the caller can mutate it after return. |
| C -> C, rclrs using generated C backing | Share C owner; no conversion. Existing `rcl_take(out)` still deep-copies into caller storage. |
| C++ -> C / Python | Convert fields directly from C++ to one cached C owner. Python's existing generated `convert_to_py` creates a normal Python object once **per mutable Python callback recipient**, not once globally. |
| Python -> C / C++ | Existing `convert_from_py` snapshots a C owner once at publish. C++ recipients share one directly converted C++ owner per ABI/allocator key. Python objects are not retained as the canonical transport owner. |
| Rust native wrapper -> C | If the wrapper owns a generated C message, transfer that owner; otherwise a registered adapter constructs C once. Ordinary `rcl_publish` remains a borrowed-pointer API. |
| typed -> serialized / generic | Serialize once per requested wire format and cache immutable bytes. No DDS traversal. |
| serialized -> typed | Retain bytes and deserialize once into the target representation; a C bridge may then supply another language representation. |

“One conversion” means at most one construction of a given reusable immutable
representation per publication. It does not mean Python -> C++ can avoid both
Python -> C and C -> C++, or that mutable Python callbacks can share one Python
object safely. Cache conversions across fanout; never convert C++ -> CDR -> C
just to cross languages. Build direct field-wise C/C++ adapters with rosidl
introspection, including strings, nested sequences, bounds and wide strings;
generated adapters can replace them later without changing the owner ABI.

Introspection is not a license to memcpy a C++ struct. Allocator-aware strings and
vectors need element operations. Unknown/custom type adapters stay on their
native C++ fast path, or use their explicit ROS-message adapter before crossing
languages. If no safe adapter exists, that pair remains on RMW. TypeDescription
alone supplies a schema, not executable conversion functions.

Legacy rclpy publish already supplies a C struct; move the scope's owned C storage
into the new owned-publish API instead of destroying it after `rcl_publish`.
For legacy `rcl_publish`, clone the borrowed C/C++ representation before returning.
Python subscriptions convert while holding their interpreter's GIL, then release
the C owner. C owner destruction needs no GIL. Different interpreters do not share
`PyObject *`; an interpreter cannot finalize while its dispatched callback is live.

An optional immutable Python view may retain a C owner and expose read-only memory,
with that owner as its buffer base. This is a new API with explicit lifetime and
mutation rules. Existing generated Python messages remain independent mutable
objects. GPU/DLPack and flat SHM frames are future adapter types, not prerequisites.

## Ownership, loans and shutdown

Shared callbacks receive const access, and can retain an owner beyond callback
return. Queue/history eviction drops only its reference, not a borrow held by a
user. The final release invokes exactly one originating deleter outside broker
and queue locks. Keep adapters loaded until their last owner dies.

An exclusive callback gets the original allocation only if the representation is
compatible, the producer transferred ownership, and there are no other owners:
no reader, replay history, conversion task or in-flight RMW serialization. A sole
subscriber does not imply sole ownership. Otherwise clone for that mutable
recipient. Multiple exclusive consumers require independent copies; choose one
eligible recipient to receive the original. This reproduces rclcpp's useful
shared/unique partition without claiming mutable cross-language aliasing.

Normal RMW publication borrows const storage through return. Hold its owner until
then. A loan whose publish operation consumes/reclaims middleware storage cannot
also be retained by IPC callbacks. Use a provider retain/release loan capability
where supported, otherwise copy for IPC or select RMW for that publication before
admission. Never delay returning a middleware loan beyond its supported lifetime.

Publisher destruction removes future matches but queued owners remain consumable.
Subscription destruction closes admission, unregisters readiness, waits for claims
or detaches their retained state, and drops queued references. Context shutdown
closes admission and wakes waiters. Last message release may occur after shutdown;
allocator and adapter resources live in the owner, not a finalized endpoint.
Context destruction waits for broker operations, not arbitrarily long user borrows.
Use a context instance generation to prevent pointer-address reuse from matching old
owners to a newly initialized context. Inherited post-fork broker handles are invalid.

## Matching, routing and duplicate suppression

Matching uses context instance, fully resolved/remapped topic, semantic type,
offered/requested QoS compatibility and supported representation adapters.
Endpoint IDs include a generation; node name and PID are not identities.
IPC is enabled per endpoint. An IPC-disabled reader in the same context is an RMW
recipient. Readers in another context, process or host are also RMW recipients.

For a publication P, take a broker match snapshot. Partition compatible readers
into admitted IPC bindings I(P) and RMW readers D(P). Preserve a real RMW endpoint
for IPC readers so a remote or IPC-disabled publisher can still reach them.
Discovery graph counts are advisory statistics, never the partition's authority.

### Required optional RMW capability

Define a versioned, size-tagged extension operations table, negotiated through an
optional provider registration/lookup. Old `rmw_*_t`, options and `rmw_message_info_t`
layouts remain unchanged. A provider can decline the capability. Do not add a hard
unresolved dependency on new symbols to an old Jazzy RMW plugin.

The extension must support all of:

1. Stable context-scoped endpoint identity and writer sequence mapping, even if a
   backend's public GID is an instance handle rather than an end-to-end wire ID.
2. A fenced writer/reader binding with route epoch and sequence cutover. For this
   exact pair, RMW suppresses only samples committed to IPC; all other writers
   still reach that reader. Suppression happens before local deserialization and,
   where possible, before writer fanout. Participant-wide ignore is insufficient.
3. An atomic publication routing decision against that writer's current match set:
   publish to all uncovered RMW matches, or skip serialization when there are none.
   A newly discovered endpoint is handled according to the same middleware matching
   linearization as an ordinary publish; rcl does not guess from a stale count.
4. Local admission and RMW suppression commit as one route decision. Prepare
   allocations/conversions/reservations first; failure leaves the pair on RMW.
   Once IPC commits, RMW failure for a remote reader does not roll back or resend
   the local delivery. Report publication failure with possible partial delivery,
   as the existing API cannot promise distributed atomic delivery.

This is more than the count API proposed by rmw#358. A provider may implement it
with a writer routing lock and local reader filters. The broker callback invoked
under that protocol must only commit prepared queue state, with a documented lock
order; it cannot allocate, run language code, or reenter RMW. Notify executors after
leaving routing locks. This protocol and its backend feasibility are production
gates; the present experiment does not validate them.

A fixed IPC binding remains active across a remote reader joining: remote samples
go through RMW, local samples keep the IPC route. No global topic mode flip.
Registration activates only after queues and readiness are usable and the RMW
cutover fence is installed. Removal first fences future IPC admissions, then drains
already accepted samples and their suppression epoch. Retain tombstones until the
provider confirms no delayed samples from that epoch remain. Do not use timeouts
or the current publisher registry to guess whether a sample was locally delivered.

If a provider has reliable stable writer+sequence metadata but only receive-side
filtering, a compatibility implementation may drop exact admitted sample IDs before
conversion. It still incurs serialization/transport overhead and must be described
as such. If only post-deserialization filtering is available, it avoids a second
callback but does not satisfy the no-RMW-work performance goal. Without safe
identity/cutover metadata use RMW for the pair, including typed, serialized and
loaned takes. Never blanket-drop all samples from a locally registered publisher:
that loses valid fallback samples.

### Reachability cases

| Publisher / reader | Route |
| --- | --- |
| both enabled, same context, compatible adapters/QoS, committed binding | IPC exactly once |
| reader disabled, same context | RMW, even if non-local count is zero |
| publisher disabled, enabled reader | RMW |
| another context in same process | RMW |
| remote reader plus local enabled reader | RMW to remote, IPC to local, pair-specific suppression |
| unsupported QoS/adapter/provider | RMW for that pair; no speculative local copy |

## History, reliability and ordering

Resolve SYSTEM_DEFAULT/BEST_AVAILABLE through actual negotiated QoS before matching.
Use the same requested/offered compatibility predicates as RMW. Unsupported policy
combinations fall back before a binding commits; do not silently weaken QoS.

* KEEP_LAST(N): a reader owns one logical bounded admission queue shared by both
  routes. It evicts the oldest not-yet-dispatched sample according to admission
  order, releasing that queue reference. Writer history and user borrows are
  separate memory budgets. Depth is not an upper bound on all retained allocations.
  Do not keep N IPC plus N RMW samples and advertise a depth of N. RMW cache is a
  transport staging area; account it explicitly until a provider supports shared
  admission/resource limits. A strict single-depth contract requires that extension.
* KEEP_ALL: use a resource-limited queue with configurable sample/byte budgets and
  explicit backpressure, not an overwriting ring or unlimited heap. Reliable
  admission can block up to the publisher's allowed blocking timeout and return a
  resource failure. Reserve capacity before committing any IPC route. Do not wait
  while holding callback-group claims or broker locks; self-deadlock is possible
  if publication and consumption share one mutually exclusive group. Reject such
  configurations or use RMW until bounded blocking behavior is implemented.
* Reliability: local reliable delivery means an admitted sample remains available
  subject to history, lifespan and endpoint teardown, not that the callback has
  run. KEEP_LAST replacement is still allowed. Best-effort may reject at resource
  exhaustion with an observable drop counter. Lack of IPC storage must not cause
  an uncoordinated per-sample RMW fallback under an active suppression binding.
* Transient local: publisher-owned immutable history, bounded by writer QoS and
  resource limits. On reader match, snapshot compatible retained samples and
  install the live cursor under one writer sequence fence, then apply reader
  depth/lifespan. This avoids holes or replay/live duplicates. Retain history even
  with no current readers. Reader durability compatibility follows RMW rules.
  Replay must also obey the provider's sequence cutover to avoid a second RMW
  replay. History disappears with the writer, as transient-local requires.
* Deadline, lifespan and liveliness: implement corresponding event accounting in
  the broker, including sample expiration using monotonic timers and source-time
  rules where required. Local reliable ACK bookkeeping must integrate
  `wait_for_all_acked` (admission/rejection, not callback completion). Until each
  policy and event has parity, leave affected pairs on RMW. Do not pretend DDS
  can monitor samples it never sees. Reliability alone supplies none of these.

Ordering is per publisher, not a global timestamp order across all publishers on a
topic. Serialize concurrent publications at the writer's admission point and assign
a monotonic sequence. Each reader emits that writer's admitted sequence order,
including replay; dropped history entries create gaps, not a reason to wait forever.
Mixed remote writers enter the same reader queue in local admission order. ROS does
not supply a total cross-writer order; do not invent one by comparing unsynchronized
source clocks.

A binding cutover records the last RMW sequence for that writer/reader pair. The
provider must drain or explicitly retire earlier samples before exposing later IPC
samples. Retirement for best-effort loss or history eviction is explicit metadata,
not a guessed timeout. The same fence applies in reverse. If a provider cannot
establish such a fence, keep the pair's route fixed for its lifetime or stay on RMW.
Never merge based on wall-clock timestamp or release a reliable ordering fence
because one second elapsed. Serialized/typed takes consume the same cursor.

## Executors and readiness

The broker owns queues; the executor owns scheduling and callback-group claims.
Publication only enqueues and notifies. It never invokes a user's subscription
callback inline. A language adapter registers the subscription's external source
with `rcl_executor_add_source`, and the queue calls `rcl_executor_notify` on becoming
ready. Dispatch takes one sample, converts if needed, runs the language callback,
then completes its token; Python coroutine callbacks complete only when their
existing semantics allow the callback group to be released.

Use level readiness with an outstanding-notification/generation flag. If more data
remains after a take, rearm one credit. Overwrite of a full queue must not add an
extra credit. Recheck after disarming under the queue lock to prevent a lost wake.
Stale claimed notifications may produce an empty take and must still complete.
Do not combine an unbounded credit count with a depth-limited queue.

Keep registration state alive through in-flight notifier/dispatch calls; remove
before freeing language callback storage. The queue must not call executor code
while holding its mutex. Preserve callback group exclusion, priorities and fairness
through the common scheduler, not through per-language worker threads.

For legacy `rcl_wait`, add an internal guard condition and pre-wait queue readiness
check through the wait-set's private implementation. Mark the existing subscription
slot ready for either route. Compose the existing on-new-message callback slot
internally instead of installing two competing callbacks. Events executors use the
same broker readiness source. No DDS shadow topic or helper thread is needed for
same-process notification.

## rclcpp migration and mixed-language composition

Bridge the existing `IntraProcessManager` API to the rcl broker, then retire its
routing/history internals after compatibility tests. Do not run two managers for
one endpoint. Existing public C++ class layouts and exported methods remain.

* Keep the context IPM subobject as an ABI facade. Map legacy numeric IDs to opaque
  broker registrations in private state or a sidecar when an exposed class layout
  cannot grow.
* Preserve template entry points and shared/unique callback selection. Wrap a
  native C++ shared/unique owner with allocator-aware retain/deleter callbacks.
  This retains the current homogeneous C++ zero-copy path without converting all
  messages to C. Adapter conversions are lazy for foreign recipients only.
* Replace `SubscriptionIntraProcess` buffers with broker-backed waitable adapters;
  avoid double notifications/independent history. Retain `get_intra_process_waitable`
  behavior for old executors. A rebuilt template user selects the new path. Old
  compiled C++ IPC code remains in legacy mode; do not silently enroll that same
  endpoint into a second broker. Full mixed-language IPC requires rebuilt bindings.
* rclpy uses owned generated C publish storage and an owned take before its usual
  Python conversion. rclrs wraps the C owner in an Arc/Drop guard and exposes
  immutable borrows with lifetimes; it cannot construct an owning Box from a
  foreign allocator. Legacy take-to-output remains valid but copies.

A mixed container must explicitly lend one live `rcl_context_t` to both client
libraries through new borrowed-context APIs. Refcount container lifetime, have one
shutdown authority, and prevent either wrapper from finalizing a borrowed context.
Initialize the interpreter/GIL before loading Python components and join their work
before interpreter finalization. Matching ROS_DOMAIN_ID or loading Python in a
C++ process does not automatically share a context. rclpy#599 alone provides no
such contract. The prototype embeds a Python consumer of a native rcl context;
it does not claim to supply a mixed rclcpp/rclpy component container.

## Services, clients and actions

Reuse owners, conversion caches and external readiness sources for two directed
queues: service requests and client responses. Correlate with context generation,
client instance generation and monotonically increasing request sequence. Store
that route in an immutable request token; the response uses the original token,
not a fresh name lookup. Deferred responses retain this token and the request owner.
Map to/from `rmw_request_id_t` internally while keeping its public layout unchanged;
use a provider-owned writer identity mapping, not a fabricated network GUID.

If exactly one compatible service is selected under a stable service binding,
route the request once locally and pin its response route. A service may receive
remote requests at the same time; their tokens select RMW responses. Never send
both local and RMW copies of one request: user side effects cannot be deduplicated
after callback execution. Client cancellation/timeout drops interest, but a late
response is safely discarded using the request generation and cannot hit a reused
client. Service destruction cancels queued requests and makes outstanding route
tokens fail cleanly; it does not free storage under an executing callback.

Multiple servers sharing a name have middleware-dependent selection behavior.
Do not assume the sole local server is the sole matched server. If the provider
cannot negotiate a unique recipient consistently with its service semantics,
leave that client's requests on RMW. The one-local-service shortcut in design#327
is insufficient when a remote service also exists. A future explicit server-ID
selection API can make this deterministic. Response failure must not trigger
request retransmission after a service has already executed it.

Respect service/client QoS histories and request/response byte budgets separately.
An executor credit represents available request/response data, not completion of a
future. Actions then benefit through their underlying services plus feedback/status
pub/sub; cancellation and goal state remain action-layer responsibilities.

## Serialized, generic and dynamic data

A generic subscription requests an encoded byte representation by type hash and
serialization-format identifier. It uses the same broker match, history and sample
identity as a typed subscription. A generic publisher owns its serialized buffer;
IPC generic subscribers can retain it directly. Existing API copies into a caller's
`rcl_serialized_message_t` when required, whereas an owned-byte API avoids that copy.

For a typed publisher, create one serialized cache only if a consumer needs bytes.
Keep actual serialization on a designated conversion worker or first requesting
take to avoid forcing the typed publisher to pay bagging cost; order/capacity
reservations cover the pending conversion. The worker's scheduling policy is
explicit so bag conversion cannot stall unrelated real-time groups. The first
implementation may serialize at publish, but must measure that interference.
A remote publication can reuse the same serialized cache only if the provider
accepts that exact format and retains it correctly; existing rmw_publish does not
promise this. Otherwise local generic serialization and remote serialization may
both occur. Do not advertise a universal single serialization across RMWs.

Serialized -> typed requires real deserialization; a local byte hop removes no
such semantic requirement. Runtime dynamic types supply registered conversion ops
and a pinned dynamic typesupport owner. Unknown formats, hashes, encodings or
unavailable operations stay on RMW; no reinterpret-casting bytes into typed data.
The local path can call an RMW codec function without traversing RMW transport;
this distinction must be explicit in copy accounting for generic callbacks.

## ABI and rollout

No existing public rcl publisher/subscription/context/options struct grows. Store
broker state in `rcl_context_impl_t` and endpoint registration pointers in private
publisher/subscription/service/client implementations. The prototype adds only a
context pointer to the private subscription impl and an opaque queue API; it does
not change ordinary publish/take behavior. New public options use a versioned
size-tagged separate configuration API after endpoint initialization, before
activation. New owners and route tokens are opaque handles.

No removed exported symbols or changed signatures. Keep existing C++ class layouts,
vtable slots and template interoperability; use private impls/sidecars for the
facade. Python and Rust binding changes are opt-in/rebuild requirements, not a
claim that an old binary will suddenly acquire ownership-aware transport. Do not
replace pointers in user message structs or change generated rosidl ABI.

Roll out disabled by default: owner/adapters, broker/readiness, RMW capability and
hybrid correctness, language facades, then wider QoS and services. Enable a pair
only when every needed policy/capability is present. Emit a debug reason when a
pair falls back. Keep counters for admissions, drops, conversions, duplicate
suppression, retained bytes and fallback reasons.

## Prototype and results

See [prototype instructions](README.md) and [measured results](REPORT.md).
Source is in the wp4 rcl worktree: `rcl/include/rcl/ipc_prototype.h`,
`rcl/src/rcl/ipc_prototype.cpp` and the `rcl_ipc_probe` package.

The explicit single-pair API checks same context, topic and nonzero type hash,
and requires KEEP_LAST(1), volatile, reliable endpoints. Both sides promise the
generated C representation. It transfers one owned message, releases an unread
replacement, and notifies on empty-to-nonempty outside its lock. A Python ctypes
consumer waits via the existing common executor, takes the pointer and invokes the
existing generated `sensor_msgs` C-to-Python converter with its GIL held.

The restriction is deliberate: rcl's opaque pointer carries no C/C++ representation
identity today. The experiment supplies that contract explicitly instead of
pretending that any `rcl_publish(void *)` can be intercepted safely. It validates
one C-owner handoff and one Python conversion; it does not validate C++ struct
conversion, shared-owner fanout, RMW suppression, remote reachability, automatic
hooks, service selection, legacy waitsets or a real rclpy Node wrapper. It is an
experimental additive API, not a production ABI commitment.

## Test plan and production gates

| Area | Required tests |
| --- | --- |
| Representation | C/C++, Python/C/C++, rclrs/C; same type hash with incompatible representation; nested/string/bounded/wide/fixed arrays, custom allocators, custom TypeAdapter; cache construction once under concurrent takes and conversion failure. |
| Lifetime | Pointer identity for unique transfer; shared const fanout; multiple exclusive clones; retained callback after publisher/context shutdown; deferred response; module pinning; final deleter once, outside locks; ASan/TSan stress, allocation failure. |
| Reachability | Local enabled + local disabled + second context + remote readers on one writer; remote and disabled publishers into an enabled reader; multiple writers; remapping; different domains; matching/unmatching during active publication. Assert exact sample IDs and callback counts. |
| Suppression | Typed, serialized, sequence and loaned take all consume the same eligibility ledger; delay RMW delivery across activate/deactivate; history/replay duplicates; endpoint-address reuse; backend identities which differ from public GIDs. Trace that covered readers do no deserialize work. |
| Ordering | Monotonic per-writer sequence across both route cutovers, remote join/leave, delayed reliable deliveries, best-effort loss retirement, history eviction and transient replay. No assertion of cross-writer wall-clock order. |
| QoS | Both reliability modes, incompatible pair, KEEP_LAST depths per reader/writer, KEEP_ALL byte/sample limits and timeouts, transient-local late join/replay, lifespan/deadline/liveliness events, wait_for_all_acked, combined queue capacity and blocked publication in exclusive groups. |
| Executor | Publish before wait, publish during sleep, overwrite while ready, take/disarm/publish race, multi-thread claims, group exclusion/priorities, coroutine completion, remove while callback claimed, callback reentrancy, shutdown, legacy wait-set and event executor parity. |
| Services | Multiple clients, deferred and out-of-order responses, cancellation/timeouts, service removal/recreation, local plus remote services, remote client to local server, request executed once despite errors; actions parameter services as integration cases. |
| ABI | `nm -D --defined-only` before/after, sizeof/alignof/offsetof existing public handles/options and representative C++ classes, run old compiled baseline against new rcl; add only new symbols. |
| Performance | Reproduce unmodified path first; alternate frozen baseline/prototype runs, same environment and payload checks, libmemcount reset after warmup, end-to-end and native-hop timing separately, load <2 at start/end, payload sizes, fanout/mixed readers, allocations and retained bytes. |

The current tests cover only the implemented slice. Production acceptance requires
all relevant rows above and at least two RMW providers, including an identity model
unlike Fast DDS. The routing extension, direct C++/C adapter coverage and ABI-safe
language facade migration are the largest remaining engineering tasks.
