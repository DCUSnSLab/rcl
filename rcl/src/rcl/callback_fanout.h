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

#ifndef RCL__CALLBACK_FANOUT_H_
#define RCL__CALLBACK_FANOUT_H_

#include "rcl/allocator.h"
#include "rcl/event_callback.h"
#include "rcl/types.h"
#include "rmw/types.h"

#ifdef __cplusplus
extern "C"
{
#endif

/// One rmw readiness callback shared by a user callback and an executor callback.
typedef struct rcl_callback_fanout_s rcl_callback_fanout_t;

typedef enum rcl_callback_fanout_slot_e
{
  RCL_CALLBACK_FANOUT_USER = 0,
  RCL_CALLBACK_FANOUT_EXECUTOR = 1
} rcl_callback_fanout_slot_t;

typedef rmw_ret_t (* rcl_callback_fanout_install_t)(
  void * rmw_entity, rmw_event_callback_t callback, const void * user_data);

/// Set one slot; `*fanout` is created on first use and owned by the entity.
rcl_ret_t
rcl_callback_fanout_set(
  rcl_callback_fanout_t ** fanout, const rcl_allocator_t * allocator,
  rcl_callback_fanout_slot_t slot, rcl_event_callback_t callback, const void * user_data,
  rcl_callback_fanout_install_t install, void * rmw_entity);

/// Call after the rmw entity is destroyed.
void
rcl_callback_fanout_fini(rcl_callback_fanout_t * fanout, const rcl_allocator_t * allocator);

#ifdef __cplusplus
}
#endif

#endif  // RCL__CALLBACK_FANOUT_H_
