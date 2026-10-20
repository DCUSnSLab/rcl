// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include "rcl_executor/trace.h"

#if defined(__GNUC__) && !defined(__clang__)
#define TRACE_NOINLINE __attribute__((noinline, noipa))
#else
#define TRACE_NOINLINE __attribute__((noinline))
#endif

extern "C"
{
TRACE_NOINLINE void rcl_executor_trace_wait_enter(
  const rcl_executor_t * executor, int64_t timeout_ns)
{
  asm volatile("" : : "g" (executor), "g" (timeout_ns) : "memory");
}
TRACE_NOINLINE void rcl_executor_trace_wait_exit(
  const rcl_executor_t * executor, rcl_executor_result_t result)
{
  asm volatile("" : : "g" (executor), "g" (result) : "memory");
}
TRACE_NOINLINE void rcl_executor_trace_item_claimed(
  const rcl_executor_t * executor, rcl_executor_id_t entity,
  rcl_executor_entity_type_t type, uint64_t group)
{
  asm volatile("" : : "g" (executor), "g" (entity), "g" (type), "g" (group) : "memory");
}
TRACE_NOINLINE void rcl_executor_trace_item_completed(
  const rcl_executor_t * executor, rcl_executor_id_t entity)
{
  asm volatile("" : : "g" (executor), "g" (entity) : "memory");
}
}  // extern "C"
