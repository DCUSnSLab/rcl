// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#ifndef RCL_EXECUTOR__EXECUTOR_H_
#define RCL_EXECUTOR__EXECUTOR_H_

#include <stddef.h>
#include <stdint.h>
#include <rcl/timer.h>

#if defined(_WIN32)
#ifdef RCL_EXECUTOR_BUILDING_LIBRARY
#define RCL_EXECUTOR_PUBLIC __declspec(dllexport)
#else
#define RCL_EXECUTOR_PUBLIC __declspec(dllimport)
#endif
#else
#define RCL_EXECUTOR_PUBLIC __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rcl_executor_s rcl_executor_t;
typedef struct rcl_executor_handle_s rcl_executor_handle_t;
typedef uint64_t rcl_executor_id_t;

typedef enum rcl_executor_group_type_e
{
  RCL_EXECUTOR_MUTUALLY_EXCLUSIVE,
  RCL_EXECUTOR_REENTRANT
} rcl_executor_group_type_t;

typedef enum rcl_executor_entity_type_e
{
  RCL_EXECUTOR_SUBSCRIPTION,
  RCL_EXECUTOR_TIMER,
  RCL_EXECUTOR_SERVICE,
  RCL_EXECUTOR_CLIENT,
  RCL_EXECUTOR_EVENT,
  RCL_EXECUTOR_GUARD_CONDITION,
  RCL_EXECUTOR_WORK,
  // Waitables that advance their own timers in take_data() register observers.
  RCL_EXECUTOR_TIMER_OBSERVER
} rcl_executor_entity_type_t;

typedef enum rcl_executor_result_e
{
  RCL_EXECUTOR_READY,
  RCL_EXECUTOR_TIMEOUT,
  RCL_EXECUTOR_INTERRUPTED,
  RCL_EXECUTOR_STOPPED,
  RCL_EXECUTOR_ERROR
} rcl_executor_result_t;

typedef void (*rcl_executor_work_fn)(void * context);

typedef struct rcl_executor_item_s
{
  rcl_executor_id_t registration;
  uint64_t token;
  rcl_executor_entity_type_t type;
  const void * entity;
  void * context;
  size_t count;
  rcl_timer_call_info_t timer_info;
  rcl_executor_work_fn function;
} rcl_executor_item_t;

typedef void (*rcl_executor_dispatch_fn)(const rcl_executor_item_t * item, void * context);

RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_create(rcl_executor_t ** executor);
// Stop and join drivers and finish outstanding claims before destroying the executor.
RCL_EXECUTOR_PUBLIC
void rcl_executor_destroy(rcl_executor_t * executor);

// Handles outlive the executor. Acquire and release outside signal handlers.
RCL_EXECUTOR_PUBLIC
rcl_executor_handle_t * rcl_executor_get_handle(rcl_executor_t * executor);
RCL_EXECUTOR_PUBLIC
void rcl_executor_handle_destroy(rcl_executor_handle_t * handle);
// These three operations preserve errno and are async-signal-safe on Linux.
RCL_EXECUTOR_PUBLIC
void rcl_executor_wake(rcl_executor_handle_t * handle);
RCL_EXECUTOR_PUBLIC
void rcl_executor_interrupt(rcl_executor_handle_t * handle);
RCL_EXECUTOR_PUBLIC
void rcl_executor_stop(rcl_executor_handle_t * handle);
RCL_EXECUTOR_PUBLIC
void rcl_executor_reset(rcl_executor_t * executor);

RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_add_group(
  rcl_executor_t * executor, uint64_t group, rcl_executor_group_type_t type);

// Entities and their clocks must remain valid until removal finishes; contexts until completion.
// Registration owns the entity's readiness/reset callback slot. Group zero is reentrant.
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_add(
  rcl_executor_t * executor, rcl_executor_entity_type_t type, const void * entity,
  void * context, uint64_t group, rcl_executor_id_t * registration);
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_remove(rcl_executor_t * executor, rcl_executor_id_t registration);
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_notify(
  rcl_executor_t * executor, rcl_executor_id_t registration, size_t count);
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_post(
  rcl_executor_t * executor, rcl_executor_work_fn function, void * context, uint64_t group);

// Timeout is in nanoseconds, negative for unlimited. A wake returns INTERRUPTED for recollection.
RCL_EXECUTOR_PUBLIC
rcl_executor_result_t rcl_executor_wait(rcl_executor_t * executor, int64_t timeout_ns);
// Claims one readiness credit, rotating entries with remaining credits to the back of the queue.
// Guard triggers merge into one credit. Timers advance here, before language dispatch.
RCL_EXECUTOR_PUBLIC
rcl_executor_result_t rcl_executor_next(rcl_executor_t * executor, rcl_executor_item_t * item);
// A spin_some pass excludes registrations it has already dispatched before claiming a timer.
RCL_EXECUTOR_PUBLIC
rcl_executor_result_t rcl_executor_next_excluding(
  rcl_executor_t * executor, rcl_executor_item_t * item,
  const rcl_executor_id_t * excluded, size_t excluded_count);
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_complete(rcl_executor_t * executor, uint64_t token);

// Dispatch must not throw through the C boundary. Each item is completed after dispatch returns.
RCL_EXECUTOR_PUBLIC
rcl_ret_t rcl_executor_spin(
  rcl_executor_t * executor, size_t threads, rcl_executor_dispatch_fn dispatch, void * context);

#ifdef __cplusplus
}
#endif
#endif  // RCL_EXECUTOR__EXECUTOR_H_
