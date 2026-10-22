// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RCL__INTRA_PROCESS_H_
#define RCL__INTRA_PROCESS_H_
#include "rcl/client.h"
#include "rcl/publisher.h"
#include "rcl/service.h"
#include "rcl/subscription.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct rcl_ipc_message_s rcl_ipc_message_t;
/// Consumes owner on every return path. Release must be callable without a language runtime.
RCL_PUBLIC rcl_ret_t rcl_publish_ipc_owned(
  const rcl_publisher_t * publisher,
  const void * data, void (*release)(void *), void * owner);
/// Publish an owner that holds data, an initialized ROS message that fill(owner) completes.
/**
 * Broker readers that pass the same key to rcl_take_ipc_adapted read the owner and fill never
 * runs for them; fill runs once, on the publishing thread when the rmw serializes data for
 * remote readers, otherwise for the first reader of the ROS form. fill returns false on failure.
 * Without the broker, data is filled and published at once. Consumes owner on every return path.
 */
RCL_PUBLIC rcl_ret_t rcl_publish_ipc_adapted(
  const rcl_publisher_t * publisher, void * data, const char * key,
  bool (*fill)(void *), void (*release)(void *), void * owner);
RCL_PUBLIC void rcl_publisher_disable_ipc(rcl_publisher_t * publisher);
RCL_PUBLIC bool rcl_publisher_is_ipc_enabled(const rcl_publisher_t * publisher);
RCL_PUBLIC bool rcl_subscription_is_ipc_enabled(const rcl_subscription_t * subscription);
/// A retained immutable representation; release can occur after endpoint destruction.
RCL_PUBLIC rcl_ret_t rcl_take_ipc_message(
  const rcl_subscription_t * subscription,
  rcl_ipc_message_t ** message, const void ** data, rmw_message_info_t * info);
RCL_PUBLIC void rcl_ipc_message_release(rcl_ipc_message_t * message);
/// rcl_take_ipc_message that sets *adapted, and not *data, for a publication with this key.
RCL_PUBLIC rcl_ret_t rcl_take_ipc_adapted(
  const rcl_subscription_t * subscription, const char * key,
  rcl_ipc_message_t ** message, const void ** adapted, const void ** data,
  rmw_message_info_t * info);
RCL_PUBLIC void rcl_subscription_disable_ipc(rcl_subscription_t * subscription);
/// Preserve encoded ingress for generic and raw subscriptions without changing their cursor.
RCL_PUBLIC void rcl_subscription_set_serialized_ipc(rcl_subscription_t * subscription);
/// Take a broker request without copying it; the caller owns it until release.
RCL_PUBLIC rcl_ret_t rcl_take_ipc_request(
  const rcl_service_t * service, rmw_service_info_t * request_header,
  rcl_ipc_message_t ** message, void ** data);
/// Take a broker response without copying it; the caller owns it until release.
RCL_PUBLIC rcl_ret_t rcl_take_ipc_response(
  const rcl_client_t * client, rmw_service_info_t * request_header,
  rcl_ipc_message_t ** message, void ** data);
/// A default response from the requesting client's broker buffers, or RCL_RET_UNSUPPORTED.
RCL_PUBLIC rcl_ret_t rcl_borrow_ipc_response(
  const rcl_service_t * service, const rmw_request_id_t * request_header,
  rcl_ipc_message_t ** message, void ** data);
/// Send a borrowed response without copying it; the caller must not modify it afterwards.
RCL_PUBLIC rcl_ret_t rcl_send_ipc_response(
  const rcl_service_t * service, rmw_request_id_t * request_header,
  rcl_ipc_message_t * message);
/// Tracing anchors: exported functions that do nothing, for uprobes. Bump the contract symbol
/// whenever an anchor's arguments change.
RCL_PUBLIC int rcl_ipc_trace_contract_1(void);
/// The broker queued message for readers of its own; source_timestamp is their message info's.
RCL_PUBLIC void rcl_ipc_trace_publish(
  const rcl_publisher_t * publisher, const void * message, int64_t source_timestamp,
  size_t readers);
/// A reader took a broker message: message is retained until rcl_ipc_message_release, NULL when
/// the take returned a copy. local is false for a remote message that the rmw queued.
RCL_PUBLIC void rcl_ipc_trace_take(
  const rcl_subscription_t * subscription, const rcl_ipc_message_t * message, const void * data,
  int64_t source_timestamp, bool local);
RCL_PUBLIC void rcl_ipc_trace_release(const rcl_ipc_message_t * message);
#ifdef __cplusplus
}
#endif
#endif  // RCL__INTRA_PROCESS_H_
