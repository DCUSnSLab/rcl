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
#include "rcl/publisher.h"
#include "rcl/subscription.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct rcl_ipc_message_s rcl_ipc_message_t;
/// Consumes owner on every return path. Release must be callable without a language runtime.
RCL_PUBLIC rcl_ret_t rcl_publish_ipc_owned(
  const rcl_publisher_t * publisher,
  const void * data, void (*release)(void *), void * owner);
RCL_PUBLIC void rcl_publisher_disable_ipc(rcl_publisher_t * publisher);
RCL_PUBLIC bool rcl_publisher_is_ipc_enabled(const rcl_publisher_t * publisher);
RCL_PUBLIC bool rcl_subscription_is_ipc_enabled(const rcl_subscription_t * subscription);
/// A retained immutable representation; release can occur after endpoint destruction.
RCL_PUBLIC rcl_ret_t rcl_take_ipc_message(
  const rcl_subscription_t * subscription,
  rcl_ipc_message_t ** message, const void ** data, rmw_message_info_t * info);
RCL_PUBLIC void rcl_ipc_message_release(rcl_ipc_message_t * message);
RCL_PUBLIC void rcl_subscription_disable_ipc(rcl_subscription_t * subscription);
/// Preserve encoded ingress for generic and raw subscriptions without changing their cursor.
RCL_PUBLIC void rcl_subscription_set_serialized_ipc(rcl_subscription_t * subscription);
#ifdef __cplusplus
}
#endif
#endif  // RCL__INTRA_PROCESS_H_
