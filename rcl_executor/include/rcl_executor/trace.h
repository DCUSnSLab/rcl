// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// @file

#ifndef RCL_EXECUTOR__TRACE_H_
#define RCL_EXECUTOR__TRACE_H_

#include "rcl_executor/executor.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum rcl_executor_trace_client_e
{
  RCL_EXECUTOR_TRACE_OTHER,
  RCL_EXECUTOR_TRACE_RCLCPP,
  RCL_EXECUTOR_TRACE_RCLPY,
  RCL_EXECUTOR_TRACE_RCLRS
} rcl_executor_trace_client_t;

// Bump the contract symbol whenever any anchor arguments change.
RCL_EXECUTOR_PUBLIC
int rcl_executor_trace_contract_1(void);
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_set_trace_client(
  rcl_executor_t * executor, rcl_executor_trace_client_t client);
// The item is valid during the anchor call. Preserve old anchors and version changed arguments.
RCL_EXECUTOR_PUBLIC
void rcl_executor_trace_claim(
  const rcl_executor_t * executor, const rcl_executor_item_t * item,
  uint64_t group, rcl_executor_trace_client_t client);

// These exported, never-inlined functions exist for uprobes.
RCL_EXECUTOR_PUBLIC
void rcl_executor_trace_wait_enter(const rcl_executor_t * executor, int64_t timeout_ns);
RCL_EXECUTOR_PUBLIC
void rcl_executor_trace_wait_exit(const rcl_executor_t * executor, rcl_executor_result_t result);
RCL_EXECUTOR_PUBLIC
void rcl_executor_trace_item_claimed(
  const rcl_executor_t * executor, rcl_executor_id_t entity,
  rcl_executor_entity_type_t type, uint64_t group);
RCL_EXECUTOR_PUBLIC
void rcl_executor_trace_item_completed(const rcl_executor_t * executor, rcl_executor_id_t entity);

#ifdef __cplusplus
}
#endif
#endif  // RCL_EXECUTOR__TRACE_H_
