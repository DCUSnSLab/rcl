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

#include <atomic>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "rcl/error_handling.h"
#include "rcl/ipc_prototype.h"
#include "rcl/rcl.h"
#include "rcl_executor/executor.h"
#include "rcl/subscription_impl.h"
#include "sensor_msgs/msg/image.h"

namespace
{
std::atomic<size_t> released;
void release_image(void * message)
{
  ++released;
  sensor_msgs__msg__Image__destroy(static_cast<sensor_msgs__msg__Image *>(message));
}
void ready(void * count) {++*static_cast<size_t *>(count);}
void dispatch(void *) {}
rcl_executor_result_t wait(rcl_executor_t * executor, int64_t timeout)
{
  rcl_executor_result_t result;
  do {
    result = rcl_executor_wait(executor, timeout);
  } while (result == RCL_EXECUTOR_INTERRUPTED);
  return result;
}

class IPC : public ::testing::Test
{
protected:
  rcl_context_t context = rcl_get_zero_initialized_context();
  rcl_node_t node = rcl_get_zero_initialized_node();
  rcl_publisher_t publisher = rcl_get_zero_initialized_publisher();
  rcl_subscription_t subscription = rcl_get_zero_initialized_subscription();
  rcl_ipc_prototype_t * queue = nullptr;
  size_t notifications = 0;

  void SetUp() override
  {
    released = 0;
    auto options = rcl_get_zero_initialized_init_options();
    ASSERT_EQ(RCL_RET_OK, rcl_init_options_init(&options, rcl_get_default_allocator()));
    ASSERT_EQ(RCL_RET_OK, rcl_init(0, nullptr, &options, &context));
    ASSERT_EQ(RCL_RET_OK, rcl_init_options_fini(&options));
    auto node_options = rcl_node_get_default_options();
    node_options.enable_rosout = false;
    ASSERT_EQ(RCL_RET_OK, rcl_node_init(&node, "wp4_test", "/", &context, &node_options));
    auto pub_options = rcl_publisher_get_default_options();
    pub_options.qos.depth = 1;
    auto sub_options = rcl_subscription_get_default_options();
    sub_options.qos.depth = 1;
    auto support = ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image);
    ASSERT_EQ(RCL_RET_OK, rcl_publisher_init(&publisher, &node, support, "/wp4", &pub_options));
    ASSERT_EQ(RCL_RET_OK, rcl_subscription_init(
        &subscription, &node, support, "/wp4", &sub_options));
  }

  void create()
  {
    ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_create(
        &publisher, &subscription, release_image, ready, &notifications, &queue));
  }

  void TearDown() override
  {
    if (queue) {rcl_ipc_prototype_destroy(queue);}
    if (subscription.impl) {EXPECT_EQ(RCL_RET_OK, rcl_subscription_fini(&subscription, &node));}
    if (publisher.impl) {EXPECT_EQ(RCL_RET_OK, rcl_publisher_fini(&publisher, &node));}
    EXPECT_EQ(RCL_RET_OK, rcl_node_fini(&node));
    EXPECT_EQ(RCL_RET_OK, rcl_shutdown(&context));
    EXPECT_EQ(RCL_RET_OK, rcl_context_fini(&context));
    rcl_reset_error();
  }
};

TEST_F(IPC, UniqueHandoffAndEmptyTake)
{
  create();
  EXPECT_EQ(nullptr, rcl_ipc_prototype_take(queue));
  auto message = sensor_msgs__msg__Image__create();
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, message));
  EXPECT_EQ(1u, notifications);
  EXPECT_EQ(message, rcl_ipc_prototype_take(queue));
  EXPECT_EQ(0u, released);
  release_image(message);
  EXPECT_EQ(nullptr, rcl_ipc_prototype_take(queue));
  EXPECT_EQ(1u, released);
}

TEST_F(IPC, OverwriteAndDestroyReleaseExactlyOnce)
{
  create();
  auto first = sensor_msgs__msg__Image__create();
  auto second = sensor_msgs__msg__Image__create();
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, first));
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, second));
  EXPECT_EQ(1u, notifications);
  EXPECT_EQ(1u, released);
  EXPECT_EQ(second, rcl_ipc_prototype_take(queue));
  release_image(second);
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, sensor_msgs__msg__Image__create()));
  EXPECT_EQ(2u, notifications);
  rcl_ipc_prototype_destroy(queue);
  queue = nullptr;
  EXPECT_EQ(3u, released);
}

TEST_F(IPC, PendingOwnerSurvivesEndpointDestruction)
{
  create();
  auto message = sensor_msgs__msg__Image__create();
  message->height = 123;
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, message));
  ASSERT_EQ(RCL_RET_OK, rcl_publisher_fini(&publisher, &node));
  ASSERT_EQ(RCL_RET_OK, rcl_subscription_fini(&subscription, &node));
  auto taken = static_cast<sensor_msgs__msg__Image *>(rcl_ipc_prototype_take(queue));
  EXPECT_EQ(message, taken);
  EXPECT_EQ(123u, taken->height);
  release_image(taken);
}

TEST_F(IPC, RejectDifferentContext)
{
  auto other = rcl_get_zero_initialized_context();
  auto options = rcl_get_zero_initialized_init_options();
  ASSERT_EQ(RCL_RET_OK, rcl_init_options_init(&options, rcl_get_default_allocator()));
  ASSERT_EQ(RCL_RET_OK, rcl_init(0, nullptr, &options, &other));
  ASSERT_EQ(RCL_RET_OK, rcl_init_options_fini(&options));
  auto other_node = rcl_get_zero_initialized_node();
  auto node_options = rcl_node_get_default_options();
  node_options.enable_rosout = false;
  ASSERT_EQ(RCL_RET_OK, rcl_node_init(&other_node, "wp4_other", "/", &other, &node_options));
  auto sub_options = rcl_subscription_get_default_options();
  sub_options.qos.depth = 1;
  auto sub = rcl_get_zero_initialized_subscription();
  ASSERT_EQ(RCL_RET_OK, rcl_subscription_init(
      &sub, &other_node, ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image),
      "/wp4", &sub_options));
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT, rcl_ipc_prototype_create(
      &publisher, &sub, release_image, ready, &notifications, &queue));
  EXPECT_EQ(nullptr, queue);
  rcl_reset_error();
  EXPECT_EQ(RCL_RET_OK, rcl_subscription_fini(&sub, &other_node));
  EXPECT_EQ(RCL_RET_OK, rcl_node_fini(&other_node));
  EXPECT_EQ(RCL_RET_OK, rcl_shutdown(&other));
  EXPECT_EQ(RCL_RET_OK, rcl_context_fini(&other));
}

TEST_F(IPC, RejectDifferentTopic)
{
  ASSERT_EQ(RCL_RET_OK, rcl_subscription_fini(&subscription, &node));
  auto options = rcl_subscription_get_default_options();
  options.qos.depth = 1;
  ASSERT_EQ(RCL_RET_OK, rcl_subscription_init(
      &subscription, &node, ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image),
      "/wp4_other", &options));
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT, rcl_ipc_prototype_create(
      &publisher, &subscription, release_image, ready, &notifications, &queue));
}

TEST_F(IPC, RejectDifferentType)
{
  auto original = subscription.impl->type_hash;
  subscription.impl->type_hash.value[0] ^= 1;
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT, rcl_ipc_prototype_create(
      &publisher, &subscription, release_image, ready, &notifications, &queue));
  subscription.impl->type_hash = original;
}

TEST_F(IPC, RejectUnsupportedHistoryDepth)
{
  ASSERT_EQ(RCL_RET_OK, rcl_subscription_fini(&subscription, &node));
  auto options = rcl_subscription_get_default_options();
  options.qos.depth = 2;
  ASSERT_EQ(RCL_RET_OK, rcl_subscription_init(
      &subscription, &node, ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image),
      "/wp4", &options));
  EXPECT_EQ(RCL_RET_UNSUPPORTED, rcl_ipc_prototype_create(
      &publisher, &subscription, release_image, ready, &notifications, &queue));
}

TEST_F(IPC, NotifySleepingExecutorAndRearm)
{
  struct State
  {
    rcl_executor_t * executor;
    rcl_executor_id_t id;
  } state{};
  ASSERT_EQ(RCL_RET_OK, rcl_executor_create(&state.executor));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_source(
      state.executor, dispatch, nullptr, 0, &state.id));
  auto notify = [](void * data) {
      auto value = static_cast<State *>(data);
      EXPECT_EQ(RCL_RET_OK, rcl_executor_notify(value->executor, value->id, 1));
    };
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_create(
      &publisher, &subscription, release_image, notify, &state, &queue));
  for (int i = 0; i < 20; ++i) {
    std::thread producer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        EXPECT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, sensor_msgs__msg__Image__create()));
      });
    EXPECT_EQ(RCL_EXECUTOR_READY, wait(state.executor, 2000000000));
    producer.join();
    rcl_executor_item_t item{};
    ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(state.executor, &item));
    EXPECT_EQ(state.id, item.registration);
    auto message = rcl_ipc_prototype_take(queue);
    ASSERT_NE(nullptr, message);
    release_image(message);
    EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(state.executor, item.token));
    EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, wait(state.executor, 0));
  }
  EXPECT_EQ(RCL_RET_OK, rcl_executor_remove(state.executor, state.id));
  rcl_executor_destroy(state.executor);
}

TEST_F(IPC, CallbackCanTakeWithoutLockReentrancy)
{
  auto take = [](void * data) {
      auto handle = static_cast<rcl_ipc_prototype_t **>(data);
      release_image(rcl_ipc_prototype_take(*handle));
    };
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_create(
      &publisher, &subscription, release_image, take, &queue, &queue));
  ASSERT_EQ(RCL_RET_OK, rcl_ipc_prototype_publish(queue, sensor_msgs__msg__Image__create()));
  EXPECT_EQ(1u, released);
  EXPECT_EQ(nullptr, rcl_ipc_prototype_take(queue));
}
}  // namespace
