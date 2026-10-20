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

#include <cstring>
#include <mutex>
#include <new>
#include <utility>

#include "rcl/error_handling.h"
#include "rcl/ipc_prototype.h"
#include "./publisher_impl.h"
#include "./subscription_impl.h"

struct rcl_ipc_prototype_s
{
  std::mutex mutex;
  rcl_context_t * context;
  void * pending = nullptr;
  rcl_ipc_prototype_release_t release;
  rcl_ipc_prototype_ready_t ready;
  void * user_data;
};

namespace
{
bool supported(const rmw_qos_profile_t & qos)
{
  return qos.history == RMW_QOS_POLICY_HISTORY_KEEP_LAST && qos.depth == 1 &&
         qos.durability == RMW_QOS_POLICY_DURABILITY_VOLATILE &&
         qos.reliability == RMW_QOS_POLICY_RELIABILITY_RELIABLE;
}
}  // namespace

rcl_ret_t rcl_ipc_prototype_create(
  const rcl_publisher_t * publisher, const rcl_subscription_t * subscription,
  rcl_ipc_prototype_release_t release, rcl_ipc_prototype_ready_t ready,
  void * user_data, rcl_ipc_prototype_t ** queue)
{
  if (!rcl_publisher_is_valid(publisher) || !rcl_subscription_is_valid(subscription)) {
    return RCL_RET_INVALID_ARGUMENT;
  }
  if (!queue || *queue || !release || !ready) {
    RCL_SET_ERROR_MSG("queue output, release and ready callbacks are required");
    return RCL_RET_INVALID_ARGUMENT;
  }
  const auto & pub_hash = publisher->impl->type_hash;
  const auto & sub_hash = subscription->impl->type_hash;
  if (publisher->impl->context != subscription->impl->context ||
    std::strcmp(rcl_publisher_get_topic_name(publisher),
    rcl_subscription_get_topic_name(subscription)) != 0 ||
    pub_hash.version == 0 || pub_hash.version != sub_hash.version ||
    std::memcmp(pub_hash.value, sub_hash.value, sizeof(pub_hash.value)) != 0)
  {
    RCL_SET_ERROR_MSG("IPC pair must have the same context, topic and type hash");
    return RCL_RET_INVALID_ARGUMENT;
  }
  if (!supported(publisher->impl->actual_qos) || !supported(subscription->impl->actual_qos)) {
    RCL_SET_ERROR_MSG("prototype requires KEEP_LAST(1), volatile, reliable QoS");
    return RCL_RET_UNSUPPORTED;
  }
  auto value = new (std::nothrow) rcl_ipc_prototype_t;
  if (!value) {
    RCL_SET_ERROR_MSG("allocating IPC queue failed");
    return RCL_RET_BAD_ALLOC;
  }
  value->context = publisher->impl->context;
  value->release = release;
  value->ready = ready;
  value->user_data = user_data;
  *queue = value;
  return RCL_RET_OK;
}

rcl_ret_t rcl_ipc_prototype_publish(rcl_ipc_prototype_t * queue, void * message)
{
  if (!queue || !message || !rcl_context_is_valid(queue->context)) {
    RCL_SET_ERROR_MSG("valid queue, message and context are required");
    return RCL_RET_INVALID_ARGUMENT;
  }
  void * previous;
  {
    std::lock_guard<std::mutex> lock(queue->mutex);
    previous = std::exchange(queue->pending, message);
  }
  if (previous) {
    queue->release(previous);
  } else {
    queue->ready(queue->user_data);
  }
  return RCL_RET_OK;
}

void * rcl_ipc_prototype_take(rcl_ipc_prototype_t * queue)
{
  std::lock_guard<std::mutex> lock(queue->mutex);
  return std::exchange(queue->pending, nullptr);
}

void rcl_ipc_prototype_destroy(rcl_ipc_prototype_t * queue)
{
  if (queue->pending) {queue->release(queue->pending);}
  delete queue;
}
