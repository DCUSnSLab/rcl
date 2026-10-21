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

#include "rcl/callback_fanout.h"

#include <mutex>
#include <new>
#include <system_error>

#include "rcl/error_handling.h"

namespace
{
std::mutex creation_mutex;
// The rmw setter reports readiness that predates it on the calling thread; only the slot being
// set receives it, because a slot that was already set has seen those events.
thread_local const rcl_callback_fanout_t * replaying = nullptr;
thread_local int replay_slot = -1;
}  // namespace

struct rcl_callback_fanout_s
{
  // Serializes setters across the rmw call, so installing and removing cannot interleave.
  std::recursive_mutex setter;
  // Held while calling a target, so clearing a slot waits for a call in flight.
  std::recursive_mutex mutex;
  rcl_event_callback_t callbacks[2] = {nullptr, nullptr};
  const void * user_data[2] = {nullptr, nullptr};
  // Events a slot missed while the other one kept the rmw callback installed.
  size_t unread[2] = {0, 0};
  bool installed = false;
};

namespace
{
void trampoline(const void * data, size_t count)
{
  auto fanout = static_cast<rcl_callback_fanout_t *>(const_cast<void *>(data));
  std::lock_guard<std::recursive_mutex> lock(fanout->mutex);
  const bool replay = replaying == fanout;
  for (int slot = 0; slot < 2; ++slot) {
    if (replay && slot != replay_slot) {
      if (!fanout->callbacks[slot]) {fanout->unread[slot] += count;}
      continue;
    }
    if (fanout->callbacks[slot]) {
      fanout->callbacks[slot](fanout->user_data[slot], count);
    } else {
      fanout->unread[slot] += count;
    }
  }
}
}  // namespace

rcl_ret_t
rcl_callback_fanout_set(
  rcl_callback_fanout_t ** fanout, const rcl_allocator_t * allocator,
  rcl_callback_fanout_slot_t slot, rcl_event_callback_t callback, const void * user_data,
  rcl_callback_fanout_install_t install, void * rmw_entity)
{
  rcl_callback_fanout_t * state;
  {
    std::lock_guard<std::mutex> lock(creation_mutex);
    if (!*fanout) {
      if (!callback) {
        return RCL_RET_OK;
      }
      void * storage = allocator->allocate(sizeof(rcl_callback_fanout_t), allocator->state);
      if (!storage) {
        RCL_SET_ERROR_MSG("allocating the callback fanout failed");
        return RCL_RET_BAD_ALLOC;
      }
      try {
        *fanout = new (storage) rcl_callback_fanout_t;
      } catch (const std::system_error & error) {
        allocator->deallocate(storage, allocator->state);
        RCL_SET_ERROR_MSG(error.what());
        return RCL_RET_ERROR;
      }
    }
    state = *fanout;
  }
  std::lock_guard<std::recursive_mutex> setter(state->setter);
  std::unique_lock<std::recursive_mutex> lock(state->mutex);
  const rcl_event_callback_t previous_callback = state->callbacks[slot];
  const void * previous_user_data = state->user_data[slot];
  const size_t unread = state->unread[slot];
  state->callbacks[slot] = callback;
  state->user_data[slot] = callback ? user_data : nullptr;
  state->unread[slot] = 0;
  const bool needed = state->callbacks[0] || state->callbacks[1];
  if (needed != state->installed) {
    lock.unlock();
    replaying = state;
    replay_slot = slot;
    rmw_ret_t ret = needed ?
      install(rmw_entity, trampoline, state) : install(rmw_entity, NULL, NULL);
    replaying = nullptr;
    replay_slot = -1;
    lock.lock();
    if (RMW_RET_OK != ret) {
      state->callbacks[slot] = previous_callback;
      state->user_data[slot] = previous_user_data;
      state->unread[slot] += unread;
      return ret;
    }
    state->installed = needed;
  }
  if (callback && unread) {
    callback(user_data, unread);
  }
  return RCL_RET_OK;
}

void
rcl_callback_fanout_fini(rcl_callback_fanout_t * fanout, const rcl_allocator_t * allocator)
{
  if (!fanout) {
    return;
  }
  fanout->~rcl_callback_fanout_t();
  allocator->deallocate(fanout, allocator->state);
}
