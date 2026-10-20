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
#include <cstdlib>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "rcl/rcl.h"
#include "rcl/intra_process.h"
#include "rcl/error_handling.h"
#include "rcl_executor/executor.h"
#include "sensor_msgs/msg/image.h"
#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/msg/camera_info.h"
#include "test_msgs/message_fixtures.hpp"
#include "test_msgs/msg/arrays.h"
#include "test_msgs/msg/bounded_sequences.h"
#include "test_msgs/msg/multi_nested.h"
#include "test_msgs/msg/strings.h"
#include "test_msgs/msg/unbounded_sequences.h"
#include "test_msgs/msg/unbounded_sequences.hpp"
#include "test_msgs/msg/w_strings.h"
#include "rmw/serialized_message.h"
#include "rmw/rmw.h"
#include "rosidl_runtime_c/string_functions.h"

namespace
{
// Padding bytes are unspecified in CDR, so the C message is compared after a C++ round trip.
template<typename T>
T through_cdr(const void * message, const rosidl_message_type_support_t * support)
{
  auto serialized = rmw_get_zero_initialized_serialized_message();
  auto allocator = rcutils_get_default_allocator();
  EXPECT_EQ(0, rmw_serialized_message_init(&serialized, 0, &allocator));
  EXPECT_EQ(0, rmw_serialize(message, support, &serialized));
  T result;
  EXPECT_EQ(0, rmw_deserialize(&serialized,
    rosidl_typesupport_cpp::get_message_type_support_handle<T>(), &result));
  EXPECT_EQ(0, rmw_serialized_message_fini(&serialized));
  return result;
}
template<typename T>
void convert_both_ways(
  rcl_node_t * node, const rosidl_message_type_support_t * c_support,
  const std::vector<std::shared_ptr<T>> & messages)
{
  auto cpp_support = rosidl_typesupport_cpp::get_message_type_support_handle<T>();
  auto options = rcl_publisher_get_default_options();
  auto sub_options = rcl_subscription_get_default_options();
  rcl_publisher_t cpp_pub = rcl_get_zero_initialized_publisher();
  rcl_publisher_t c_pub = rcl_get_zero_initialized_publisher();
  rcl_subscription_t c_sub = rcl_get_zero_initialized_subscription();
  rcl_subscription_t cpp_sub = rcl_get_zero_initialized_subscription();
  ASSERT_EQ(0, rcl_publisher_init(&cpp_pub, node, cpp_support, "/to_c", &options));
  ASSERT_EQ(0, rcl_subscription_init(&c_sub, node, c_support, "/to_c", &sub_options));
  ASSERT_EQ(0, rcl_publisher_init(&c_pub, node, c_support, "/to_cpp", &options));
  ASSERT_EQ(0, rcl_subscription_init(&cpp_sub, node, cpp_support, "/to_cpp", &sub_options));
  for (const auto & message : messages) {
    SCOPED_TRACE(rosidl_generator_traits::name<T>());
    ASSERT_EQ(0, rcl_publish(&cpp_pub, message.get(), nullptr));
    rcl_ipc_message_t * c_owner;
    const void * c_data;
    ASSERT_EQ(0, rcl_take_ipc_message(&c_sub, &c_owner, &c_data, nullptr));
    EXPECT_TRUE(*message == through_cdr<T>(c_data, c_support));
    ASSERT_EQ(0, rcl_publish(&c_pub, c_data, nullptr));
    rcl_ipc_message_release(c_owner);
    rcl_ipc_message_t * cpp_owner;
    const void * cpp_data;
    ASSERT_EQ(0, rcl_take_ipc_message(&cpp_sub, &cpp_owner, &cpp_data, nullptr));
    EXPECT_TRUE(*message == *static_cast<const T *>(cpp_data));
    rcl_ipc_message_release(cpp_owner);
  }
  EXPECT_EQ(0, rcl_subscription_fini(&cpp_sub, node));
  EXPECT_EQ(0, rcl_publisher_fini(&c_pub, node));
  EXPECT_EQ(0, rcl_subscription_fini(&c_sub, node));
  EXPECT_EQ(0, rcl_publisher_fini(&cpp_pub, node));
}
class BrokerTest : public ::testing::Test
{
protected:
  rcl_context_t context = rcl_get_zero_initialized_context();
  rcl_node_t node = rcl_get_zero_initialized_node();
  rcl_publisher_t publisher = rcl_get_zero_initialized_publisher();
  rcl_subscription_t subscription = rcl_get_zero_initialized_subscription();
  void SetUp() override
  {
    setenv("RCL_INTRA_PROCESS", "1", 1);
    auto options = rcl_get_zero_initialized_init_options();
    ASSERT_EQ(0, rcl_init_options_init(&options, rcl_get_default_allocator()));
    ASSERT_EQ(0, rcl_init(0, nullptr, &options, &context));
    ASSERT_EQ(0, rcl_init_options_fini(&options));
    auto node_options = rcl_node_get_default_options();
    node_options.enable_rosout = false;
    ASSERT_EQ(0, rcl_node_init(&node, "broker_test", "/", &context, &node_options));
    endpoints(3, RMW_QOS_POLICY_RELIABILITY_RELIABLE);
  }
  void endpoints(
    size_t depth, rmw_qos_reliability_policy_t reliability,
    const char * topic = "/broker_test")
  {
    if (subscription.impl) {ASSERT_EQ(0, rcl_subscription_fini(&subscription, &node));}
    if (publisher.impl) {ASSERT_EQ(0, rcl_publisher_fini(&publisher, &node));}
    auto po = rcl_publisher_get_default_options();
    po.qos.depth = depth;
    po.qos.reliability = reliability;
    auto so = rcl_subscription_get_default_options();
    so.qos = po.qos;
    ASSERT_EQ(0, rcl_publisher_init(&publisher, &node,
        ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image), "/broker_test", &po));
    ASSERT_EQ(0, rcl_subscription_init(&subscription, &node,
        ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image), topic, &so));
    ASSERT_TRUE(rcl_publisher_is_ipc_enabled(&publisher));
    ASSERT_TRUE(rcl_subscription_is_ipc_enabled(&subscription));
  }
  void send(uint32_t n)
  {
    auto message = sensor_msgs__msg__Image__create();
    message->height = n;
    EXPECT_TRUE(rosidl_runtime_c__String__assign(&message->header.frame_id, "frame"));
    EXPECT_EQ(0, rcl_publish(&publisher, message, nullptr));
    sensor_msgs__msg__Image__destroy(message);
  }
  void TearDown() override
  {
    if (subscription.impl) {EXPECT_EQ(0, rcl_subscription_fini(&subscription, &node));}
    if (publisher.impl) {EXPECT_EQ(0, rcl_publisher_fini(&publisher, &node));}
    EXPECT_EQ(0, rcl_node_fini(&node));
    EXPECT_EQ(0, rcl_shutdown(&context));
    EXPECT_EQ(0, rcl_context_fini(&context));
  }
};
TEST_F(BrokerTest, DepthAndWriterOrder)
{
  for (uint32_t n = 0; n < 10; ++n) {
    send(n);
  }
  auto result = sensor_msgs__msg__Image__create();
  for (uint32_t n = 7; n < 10; ++n) {
    rmw_message_info_t info{};
    ASSERT_EQ(0, rcl_take(&subscription, result, &info, nullptr));
    EXPECT_TRUE(info.from_intra_process);
    EXPECT_EQ(n, result->height);
    EXPECT_STREQ("frame", result->header.frame_id.data);
  }
  EXPECT_EQ(RCL_RET_SUBSCRIPTION_TAKE_FAILED, rcl_take(&subscription, result, nullptr, nullptr));
  sensor_msgs__msg__Image__destroy(result);
}
TEST_F(BrokerTest, BestEffortDepth)
{
  endpoints(1, RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT);
  send(1); send(2);
  auto result = sensor_msgs__msg__Image__create();
  ASSERT_EQ(0, rcl_take(&subscription, result, nullptr, nullptr));
  EXPECT_EQ(2u, result->height);
  sensor_msgs__msg__Image__destroy(result);
}
TEST_F(BrokerTest, TopicMismatch)
{
  endpoints(3, RMW_QOS_POLICY_RELIABILITY_RELIABLE, "/other");
  send(1);
  rcl_ipc_message_t * owner;
  const void * data;
  EXPECT_EQ(RCL_RET_SUBSCRIPTION_TAKE_FAILED,
    rcl_take_ipc_message(&subscription, &owner, &data, nullptr));
}
TEST_F(BrokerTest, ProviderReliabilityCompatibility)
{
  ASSERT_EQ(0, rcl_publisher_fini(&publisher, &node));
  auto po = rcl_publisher_get_default_options();
  po.qos.reliability = RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT;
  ASSERT_EQ(0, rcl_publisher_init(&publisher, &node,
      ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image), "/broker_test", &po));
  send(1);
  rcl_ipc_message_t * owner;
  const void * data;
  rmw_message_info_t info{};
  ASSERT_EQ(RCL_RET_OK, rcl_take_ipc_message(&subscription, &owner, &data, &info));
  EXPECT_TRUE(info.from_intra_process);
  EXPECT_EQ(1u, static_cast<const sensor_msgs__msg__Image *>(data)->height);
  rcl_ipc_message_release(owner);
}
TEST_F(BrokerTest, CToCppRepresentationAndRetainedLifetime)
{
  ASSERT_EQ(0, rcl_subscription_fini(&subscription, &node));
  auto so = rcl_subscription_get_default_options();
  ASSERT_EQ(0, rcl_subscription_init(&subscription, &node,
      rosidl_typesupport_cpp::get_message_type_support_handle<sensor_msgs::msg::Image>(),
      "/broker_test", &so));
  send(42);
  rcl_ipc_message_t * owner;
  const void * data;
  ASSERT_EQ(0, rcl_take_ipc_message(&subscription, &owner, &data, nullptr));
  auto image = static_cast<const sensor_msgs::msg::Image *>(data);
  ASSERT_EQ(0, rcl_publisher_fini(&publisher, &node));
  ASSERT_EQ(0, rcl_subscription_fini(&subscription, &node));
  EXPECT_EQ(42u, image->height);
  EXPECT_EQ("frame", image->header.frame_id);
  rcl_ipc_message_release(owner);
}
TEST_F(BrokerTest, RemovalWhilePublishing)
{
  std::atomic<bool> running{true};
  std::thread producer([&] {while (running.load()) {send(1);}});
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  EXPECT_EQ(0, rcl_subscription_fini(&subscription, &node));
  running = false;
  producer.join();
}
TEST_F(BrokerTest, ClassicWaitSetWakes)
{
  auto wait = rcl_get_zero_initialized_wait_set();
  ASSERT_EQ(0, rcl_wait_set_init(&wait, 1, 0, 0, 0, 0, 0, &context, rcl_get_default_allocator()));
  ASSERT_EQ(0, rcl_wait_set_add_subscription(&wait, &subscription, nullptr));
  std::thread producer([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(10)); send(7);
    });
  EXPECT_EQ(0, rcl_wait(&wait, 2000000000));
  producer.join();
  EXPECT_NE(nullptr, wait.subscriptions[0]);
  EXPECT_EQ(0, rcl_wait_set_fini(&wait));
}
TEST_F(BrokerTest, DisabledReaderRemainsReachable)
{
  rcl_subscription_disable_ipc(&subscription);
  send(7);
  auto message = sensor_msgs__msg__Image__create();
  rcl_ret_t ret = RCL_RET_SUBSCRIPTION_TAKE_FAILED;
  for (int i = 0; i < 200 && ret != RCL_RET_OK; ++i) {
    ret = rcl_take(&subscription, message, nullptr, nullptr);
    if (ret != RCL_RET_OK) {std::this_thread::sleep_for(std::chrono::milliseconds(1));}
  }
  ASSERT_EQ(0, ret);
  EXPECT_EQ(7u, message->height);
  sensor_msgs__msg__Image__destroy(message);
}

TEST_F(BrokerTest, SemanticTypeMismatch)
{
  ASSERT_EQ(0, rcl_subscription_fini(&subscription, &node));
  auto options = rcl_subscription_get_default_options();
  ASSERT_EQ(0, rcl_subscription_init(&subscription, &node,
      ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, CameraInfo), "/broker_test", &options));
  send(3);
  rcl_ipc_message_t * owner = nullptr;
  const void * data = nullptr;
  EXPECT_EQ(RCL_RET_SUBSCRIPTION_TAKE_FAILED,
    rcl_take_ipc_message(&subscription, &owner, &data, nullptr));
}
TEST_F(BrokerTest, NestedAndBooleanSequences)
{
  ASSERT_EQ(0, rcl_subscription_fini(&subscription, &node));
  ASSERT_EQ(0, rcl_publisher_fini(&publisher, &node));
  auto po = rcl_publisher_get_default_options();
  auto so = rcl_subscription_get_default_options();
  ASSERT_EQ(0, rcl_publisher_init(&publisher, &node,
      rosidl_typesupport_cpp::get_message_type_support_handle<test_msgs::msg::UnboundedSequences>(),
      "/broker_test", &po));
  ASSERT_EQ(0, rcl_subscription_init(&subscription, &node,
      ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, UnboundedSequences), "/broker_test", &so));
  test_msgs::msg::UnboundedSequences input;
  input.bool_values = {true, false, true};
  input.string_values = {"first", "", "last"};
  input.basic_types_values.resize(2);
  input.basic_types_values[1].int64_value = -123456789;
  ASSERT_EQ(0, rcl_publish(&publisher, &input, nullptr));
  auto output = test_msgs__msg__UnboundedSequences__create();
  ASSERT_EQ(0, rcl_take(&subscription, output, nullptr, nullptr));
  EXPECT_EQ(3u, output->bool_values.size);
  EXPECT_TRUE(output->bool_values.data[0]);
  EXPECT_FALSE(output->bool_values.data[1]);
  EXPECT_TRUE(output->bool_values.data[2]);
  EXPECT_STREQ("last", output->string_values.data[2].data);
  EXPECT_EQ(-123456789, output->basic_types_values.data[1].int64_value);
  test_msgs__msg__UnboundedSequences__destroy(output);
}
TEST_F(BrokerTest, EveryFieldKindConvertsBothWays)
{
  convert_both_ways(&node, ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, Arrays),
    get_messages_arrays());
  convert_both_ways(&node, ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, BoundedSequences),
    get_messages_bounded_sequences());
  convert_both_ways(&node, ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, UnboundedSequences),
    get_messages_unbounded_sequences());
  convert_both_ways(&node, ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, MultiNested),
    get_messages_multi_nested());
  convert_both_ways(&node, ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, Strings),
    get_messages_strings());
  convert_both_ways(&node, ROSIDL_GET_MSG_TYPE_SUPPORT(test_msgs, msg, WStrings),
    get_messages_wstrings());
}
TEST_F(BrokerTest, TransientLocalPublisherFallsBack)
{
  ASSERT_EQ(0, rcl_publisher_fini(&publisher, &node));
  auto po = rcl_publisher_get_default_options();
  po.qos.durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  ASSERT_EQ(0, rcl_publisher_init(&publisher, &node,
      ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image), "/broker_test", &po));
  EXPECT_FALSE(rcl_publisher_is_ipc_enabled(&publisher));
  send(8);
  auto output = sensor_msgs__msg__Image__create();
  rmw_message_info_t info{};
  rcl_ret_t result = RCL_RET_SUBSCRIPTION_TAKE_FAILED;
  for (int i = 0; i < 200 && result != RCL_RET_OK; ++i) {
    result = rcl_take(&subscription, output, &info, nullptr);
    if (result != RCL_RET_OK) {std::this_thread::sleep_for(std::chrono::milliseconds(1));}
  }
  EXPECT_EQ(0, result);
  EXPECT_FALSE(info.from_intra_process);
  EXPECT_EQ(8u, output->height);
  sensor_msgs__msg__Image__destroy(output);
}
TEST_F(BrokerTest, SeparateContextUsesRmw)
{
  auto other = rcl_get_zero_initialized_context();
  auto options = rcl_get_zero_initialized_init_options();
  ASSERT_EQ(0, rcl_init_options_init(&options, rcl_get_default_allocator()));
  ASSERT_EQ(0, rcl_init(0, nullptr, &options, &other));
  ASSERT_EQ(0, rcl_init_options_fini(&options));
  auto other_node = rcl_get_zero_initialized_node();
  auto no = rcl_node_get_default_options();
  no.enable_rosout = false;
  ASSERT_EQ(0, rcl_node_init(&other_node, "other_context", "/", &other, &no));
  auto sub = rcl_get_zero_initialized_subscription();
  auto so = rcl_subscription_get_default_options();
  ASSERT_EQ(0, rcl_subscription_init(&sub, &other_node,
      ROSIDL_GET_MSG_TYPE_SUPPORT(sensor_msgs, msg, Image), "/broker_test", &so));
  size_t matches = 0;
  for (int i = 0; i < 200 && matches < 2; ++i) {
    ASSERT_EQ(0, rcl_publisher_get_subscription_count(&publisher, &matches));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_EQ(2u, matches);
  send(19);
  auto message = sensor_msgs__msg__Image__create();
  rmw_message_info_t info{};
  rcl_ret_t result = RCL_RET_SUBSCRIPTION_TAKE_FAILED;
  for (int i = 0; i < 200 && result != RCL_RET_OK; ++i) {
    result = rcl_take(&sub, message, &info, nullptr);
    if (result != RCL_RET_OK) {std::this_thread::sleep_for(std::chrono::milliseconds(1));}
  }
  EXPECT_EQ(0, result);
  EXPECT_FALSE(info.from_intra_process);
  EXPECT_EQ(19u, message->height);
  sensor_msgs__msg__Image__destroy(message);
  EXPECT_EQ(0, rcl_subscription_fini(&sub, &other_node));
  EXPECT_EQ(0, rcl_node_fini(&other_node));
  EXPECT_EQ(0, rcl_shutdown(&other));
  EXPECT_EQ(0, rcl_context_fini(&other));
}

TEST_F(BrokerTest, CommonExecutorBacklogAndDepthCredits)
{
  send(1); send(2); send(3); send(4);
  rcl_executor_t * executor = nullptr;
  rcl_executor_id_t id;
  ASSERT_EQ(0, rcl_executor_create(&executor));
  ASSERT_EQ(0, rcl_executor_add(executor, RCL_EXECUTOR_SUBSCRIPTION, &subscription,
      nullptr, 0, &id));
  auto result = sensor_msgs__msg__Image__create();
  for (uint32_t n = 2; n <= 4; ++n) {
    EXPECT_EQ(RCL_EXECUTOR_READY, rcl_executor_wait(executor, 1000000000));
    rcl_executor_item_t item{};
    ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
    ASSERT_EQ(0, rcl_take(&subscription, result, nullptr, nullptr));
    EXPECT_EQ(n, result->height);
    EXPECT_EQ(0, rcl_executor_complete(executor, item.token));
  }
  rcl_executor_item_t item{};
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
  EXPECT_EQ(0, rcl_executor_remove(executor, id));
  rcl_executor_destroy(executor);
  sensor_msgs__msg__Image__destroy(result);
}
}  // namespace
