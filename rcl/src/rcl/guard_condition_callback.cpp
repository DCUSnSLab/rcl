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

#include "rcl/guard_condition_callback.h"

#include <mutex>
#include <new>
#include <system_error>

#include "rcl/error_handling.h"

struct rcl_guard_condition_callback_s
{
  std::recursive_mutex mutex;
  rcl_event_callback_t callback = nullptr;
  const void * user_data = nullptr;
  size_t unread_count = 0;
};

rcl_ret_t
rcl_guard_condition_callback_init(
  const rcl_allocator_t * allocator, rcl_guard_condition_callback_t ** state)
{
  void * storage = allocator->allocate(sizeof(rcl_guard_condition_callback_t), allocator->state);
  if (!storage) {
    RCL_SET_ERROR_MSG("allocating guard condition callback state failed");
    return RCL_RET_BAD_ALLOC;
  }
  try {
    *state = new (storage) rcl_guard_condition_callback_t;
  } catch (const std::system_error & error) {
    allocator->deallocate(storage, allocator->state);
    RCL_SET_ERROR_MSG(error.what());
    return RCL_RET_ERROR;
  }
  return RCL_RET_OK;
}

void
rcl_guard_condition_callback_fini(
  rcl_guard_condition_callback_t * state, const rcl_allocator_t * allocator)
{
  state->~rcl_guard_condition_callback_t();
  allocator->deallocate(state, allocator->state);
}

void
rcl_guard_condition_callback_trigger(rcl_guard_condition_callback_t * state)
{
  std::lock_guard<std::recursive_mutex> lock(state->mutex);
  if (state->callback) {
    state->callback(state->user_data, 1);
  } else {
    ++state->unread_count;
  }
}

void
rcl_guard_condition_callback_set(
  rcl_guard_condition_callback_t * state, rcl_event_callback_t callback, const void * user_data)
{
  std::lock_guard<std::recursive_mutex> lock(state->mutex);
  state->callback = callback;
  state->user_data = user_data;
  if (callback && state->unread_count) {
    size_t count = state->unread_count;
    state->unread_count = 0;
    callback(user_data, count);
  }
}
