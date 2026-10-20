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

#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <thread>

#ifdef WP4_IPC
#include "rcl/ipc_prototype.h"
#endif
#include "rcl/rcl.h"
#include "rcl/error_handling.h"
#include "rosidl_runtime_c/primitives_sequence_functions.h"
#include "rcl_executor/executor.h"
#include "sensor_msgs/msg/image.h"

namespace
{
rcl_context_t context = rcl_get_zero_initialized_context();
rcl_node_t node = rcl_get_zero_initialized_node();
rcl_publisher_t publisher = rcl_get_zero_initialized_publisher();
rcl_subscription_t subscription = rcl_get_zero_initialized_subscription();
rcl_executor_t * executor;
rcl_executor_id_t registration;
sensor_msgs__msg__Image * received;
uint64_t token;
#ifdef WP4_IPC
rcl_ipc_prototype_t * queue;
void release_image(void * message)
{
  sensor_msgs__msg__Image__destroy(static_cast<sensor_msgs__msg__Image *>(message));
}
#endif

void check(rcl_ret_t result)
{
  if (result != RCL_RET_OK) {
    throw std::runtime_error(rcl_get_error_string().str);
  }
}
#ifdef WP4_IPC
void notify([[maybe_unused]] void * user_data)
{
  check(rcl_executor_notify(executor, registration, 1));
}
void dispatch(void *) {}
#endif
}  // namespace

extern "C"
{
int wp4_init()
{
  try {
    auto allocator = rcl_get_default_allocator();
    auto options = rcl_get_zero_initialized_init_options();
    check(rcl_init_options_init(&options, allocator));
    check(rcl_init(0, nullptr, &options, &context));
    check(rcl_init_options_fini(&options));
    auto node_options = rcl_node_get_default_options();
    node_options.enable_rosout = false;
    check(rcl_node_init(&node, "wp4_probe", "/", &context, &node_options));
    auto pub_options = rcl_publisher_get_default_options();
    pub_options.qos.depth = 1;
    auto sub_options = rcl_subscription_get_default_options();
    sub_options.qos.depth = 1;
    auto support = ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image);
    check(rcl_publisher_init(&publisher, &node, support, "/wp4_image", &pub_options));
    check(rcl_subscription_init(&subscription, &node, support, "/wp4_image", &sub_options));
    check(rcl_executor_create(&executor));
#ifdef WP4_IPC
    check(rcl_executor_add_source(executor, dispatch, nullptr, 0, &registration));
    check(rcl_ipc_prototype_create(
        &publisher, &subscription, release_image, notify, nullptr, &queue));
#else
    check(rcl_executor_add(
        executor, RCL_EXECUTOR_SUBSCRIPTION, &subscription, nullptr, 0, &registration));
#endif
    for (int i = 0; i < 500; ++i) {
      size_t count = 0;
      check(rcl_publisher_get_subscription_count(&publisher, &count));
      if (count == 1) {return 0;}
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return -2;
  } catch (...) {
    return -1;
  }
}

int64_t wp4_publish(size_t bytes, uint32_t sequence)
{
  auto message = sensor_msgs__msg__Image__create();
  rosidl_runtime_c__uint8__Sequence__fini(&message->data);
  if (!rosidl_runtime_c__uint8__Sequence__init(&message->data, bytes)) {return -1;}
  std::memset(message->data.data, 42, bytes);
  message->height = sequence;
  message->width = static_cast<uint32_t>(bytes);
  message->step = message->width;
  auto start = std::chrono::steady_clock::now().time_since_epoch();
  auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(start).count();
#ifdef WP4_IPC
  auto result = rcl_ipc_prototype_publish(queue, message);
#else
  auto result = rcl_publish(&publisher, message, nullptr);
  sensor_msgs__msg__Image__destroy(message);
#endif
  return result == RCL_RET_OK ? nanos : -1;
}

void * wp4_take()
{
  rcl_executor_item_t item{};
  rcl_executor_result_t result;
  do {
    result = rcl_executor_wait(executor, 2000000000);
  } while (result == RCL_EXECUTOR_INTERRUPTED);
  if (result != RCL_EXECUTOR_READY) {return nullptr;}
  if (rcl_executor_next(executor, &item) != RCL_EXECUTOR_READY) {return nullptr;}
  token = item.token;
#ifdef WP4_IPC
  received = static_cast<sensor_msgs__msg__Image *>(rcl_ipc_prototype_take(queue));
#else
  received = sensor_msgs__msg__Image__create();
  auto status = rcl_take(&subscription, received, nullptr, nullptr);
  if (status != RCL_RET_OK) {
    sensor_msgs__msg__Image__destroy(received);
    received = nullptr;
    check(rcl_executor_complete(executor, token));
  }
#endif
  return received;
}

void wp4_release()
{
  sensor_msgs__msg__Image__destroy(received);
  received = nullptr;
  check(rcl_executor_complete(executor, token));
}

void wp4_fini()
{
  check(rcl_executor_remove(executor, registration));
  rcl_executor_destroy(executor);
#ifdef WP4_IPC
  rcl_ipc_prototype_destroy(queue);
#endif
  check(rcl_subscription_fini(&subscription, &node));
  check(rcl_publisher_fini(&publisher, &node));
  check(rcl_node_fini(&node));
  check(rcl_shutdown(&context));
  check(rcl_context_fini(&context));
}
}
