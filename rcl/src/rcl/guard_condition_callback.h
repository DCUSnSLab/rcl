// Copyright 2015 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RCL__GUARD_CONDITION_CALLBACK_H_
#define RCL__GUARD_CONDITION_CALLBACK_H_

#include "rcl/allocator.h"
#include "rcl/event_callback.h"
#include "rcl/types.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct rcl_guard_condition_callback_s rcl_guard_condition_callback_t;

rcl_ret_t
rcl_guard_condition_callback_init(
  const rcl_allocator_t * allocator, rcl_guard_condition_callback_t ** state);

void
rcl_guard_condition_callback_fini(
  rcl_guard_condition_callback_t * state, const rcl_allocator_t * allocator);

void
rcl_guard_condition_callback_trigger(rcl_guard_condition_callback_t * state);

void
rcl_guard_condition_callback_set(
  rcl_guard_condition_callback_t * state, rcl_event_callback_t callback, const void * user_data);

#ifdef __cplusplus
}
#endif

#endif  // RCL__GUARD_CONDITION_CALLBACK_H_
