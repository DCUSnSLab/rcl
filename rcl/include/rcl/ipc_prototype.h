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

#ifndef RCL__IPC_PROTOTYPE_H_
#define RCL__IPC_PROTOTYPE_H_

#include "rcl/publisher.h"
#include "rcl/subscription.h"
#include "rcl/visibility_control.h"

#ifdef __cplusplus
extern "C" {
#endif

/// Experimental single-pair, KEEP_LAST(1), volatile, reliable C-message handoff.
/** Both endpoints must use the same generated C representation. This API does
 * not intercept ordinary publish/take, perform discovery, or suppress DDS copies.
 * The caller chooses this route instead of rmw publication for this pair.
 * Endpoints must be valid at creation. Quiesce producers/consumers before destroy;
 * destroy the queue before finalizing the context. Callbacks must not throw.
 */
typedef struct rcl_ipc_prototype_s rcl_ipc_prototype_t;
typedef void (*rcl_ipc_prototype_release_t)(void * message);
typedef void (*rcl_ipc_prototype_ready_t)(void * user_data);

/// Create a queue; output must initially be NULL. Ready runs outside the queue lock.
RCL_PUBLIC rcl_ret_t
rcl_ipc_prototype_create(
  const rcl_publisher_t * publisher, const rcl_subscription_t * subscription,
  rcl_ipc_prototype_release_t release, rcl_ipc_prototype_ready_t ready,
  void * user_data, rcl_ipc_prototype_t ** queue);

/// On success consumes message; overwriting an unread message releases that owner.
RCL_PUBLIC rcl_ret_t
rcl_ipc_prototype_publish(rcl_ipc_prototype_t * queue, void * message);

/// Transfer the owner to the caller; NULL means empty. Caller uses its release function.
RCL_PUBLIC void *
rcl_ipc_prototype_take(rcl_ipc_prototype_t * queue);

/// Release the unread owner, if any, and destroy the queue.
RCL_PUBLIC void
rcl_ipc_prototype_destroy(rcl_ipc_prototype_t * queue);

#ifdef __cplusplus
}
#endif
#endif  // RCL__IPC_PROTOTYPE_H_
