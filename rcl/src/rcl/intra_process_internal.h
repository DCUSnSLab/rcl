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

#ifndef RCL__INTRA_PROCESS_INTERNAL_H_
#define RCL__INTRA_PROCESS_INTERNAL_H_
#include "rcl/intra_process.h"
#ifdef __cplusplus
extern "C" {
#endif
void rcl_ipc_context_init(rcl_context_t * context);
void rcl_ipc_context_fini(rcl_context_t * context);
void rcl_ipc_publisher_init(rcl_publisher_t *, const rosidl_message_type_support_t *);
void rcl_ipc_publisher_fini(rcl_publisher_t *);
void rcl_ipc_subscription_init(rcl_subscription_t *, const rosidl_message_type_support_t *);
rcl_ret_t rcl_ipc_publish(const rcl_publisher_t *, const void *);
rcl_ret_t rcl_ipc_take_copy(const rcl_subscription_t *, void *, rmw_message_info_t *);
rcl_ret_t rcl_ipc_take_serialized_copy(
  const rcl_subscription_t *, rcl_serialized_message_t *, rmw_message_info_t *);
#ifdef __cplusplus
}
#endif
#endif  // RCL__INTRA_PROCESS_INTERNAL_H_
