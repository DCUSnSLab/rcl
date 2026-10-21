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
#include <cstdlib>
#include <thread>
#include <vector>
#include "gtest/gtest.h"
#include "rcl/rcl.h"
#include "rcl/graph.h"
#include "rcl/error_handling.h"
#include "rcl/intra_process.h"
#include "test_msgs/srv/basic_types.hpp"
#include "test_msgs/srv/basic_types.h"
#include "rosidl_runtime_c/string_functions.h"

namespace
{
using Service = test_msgs::srv::BasicTypes;
class ServiceBroker : public ::testing::Test
{
protected:
  rcl_context_t context = rcl_get_zero_initialized_context();
  rcl_node_t node = rcl_get_zero_initialized_node();
  rcl_service_t service = rcl_get_zero_initialized_service();
  rcl_client_t client = rcl_get_zero_initialized_client();
  rcl_wait_set_t wait = rcl_get_zero_initialized_wait_set();
  const rosidl_service_type_support_t * type =
    rosidl_typesupport_cpp::get_service_type_support_handle<Service>();
  void SetUp() override
  {
    setenv("RCL_INTRA_PROCESS", "1", 1);
    auto options = rcl_get_zero_initialized_init_options();
    ASSERT_EQ(0, rcl_init_options_init(&options, rcl_get_default_allocator()));
    ASSERT_EQ(0, rcl_init(0, nullptr, &options, &context));
    ASSERT_EQ(0, rcl_init_options_fini(&options));
    auto node_options = rcl_node_get_default_options();
    node_options.enable_rosout = false;
    ASSERT_EQ(0, rcl_node_init(&node, "service_broker", "/", &context, &node_options));
    auto so = rcl_service_get_default_options();
    ASSERT_EQ(0, rcl_service_init(&service, &node, type, "/broker_service", &so));
    auto co = rcl_client_get_default_options();
    ASSERT_EQ(0, rcl_client_init(&client, &node, type, "/broker_service", &co));
    ASSERT_EQ(0, rcl_wait_set_init(&wait, 0, 0, 0, 1, 1, 0, &context, rcl_get_default_allocator()));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool available = false;
    do {
      ASSERT_EQ(0, rcl_service_server_is_available(&node, &client, &available));
      if (!available) {std::this_thread::sleep_for(std::chrono::milliseconds(1));}
    } while (!available && std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(available);
  }
  void TearDown() override
  {
    EXPECT_EQ(0, rcl_wait_set_fini(&wait));
    if (client.impl) {EXPECT_EQ(0, rcl_client_fini(&client, &node));}
    if (service.impl) {EXPECT_EQ(0, rcl_service_fini(&service, &node));}
    EXPECT_EQ(0, rcl_node_fini(&node));
    EXPECT_EQ(0, rcl_shutdown(&context));
    EXPECT_EQ(0, rcl_context_fini(&context));
  }
  void send(int value, int64_t * sequence)
  {
    Service::Request request;
    request.int64_value = value;
    request.string_value = std::string(1024, 'x');
    ASSERT_EQ(0, rcl_send_request(&client, &request, sequence));
  }
  void ready_service()
  {
    ASSERT_EQ(0, rcl_wait_set_clear(&wait));
    ASSERT_EQ(0, rcl_wait_set_add_service(&wait, &service, nullptr));
    ASSERT_EQ(0, rcl_wait(&wait, 1000000000));
    ASSERT_EQ(&service, wait.services[0]);
  }
  void ready_client()
  {
    ASSERT_EQ(0, rcl_wait_set_clear(&wait));
    ASSERT_EQ(0, rcl_wait_set_add_client(&wait, &client, nullptr));
    ASSERT_EQ(0, rcl_wait(&wait, 1000000000));
    ASSERT_EQ(&client, wait.clients[0]);
  }
};
TEST_F(ServiceBroker, DeferredOutOfOrderAndDuplicateResponses)
{
  std::vector<rmw_request_id_t> ids;
  for (int value = 0; value < 3; ++value) {
    int64_t sequence;
    send(value, &sequence);
    ready_service();
    Service::Request request;
    rmw_service_info_t info{};
    ASSERT_EQ(0, rcl_take_request_with_info(&service, &info, &request));
    EXPECT_EQ(value, request.int64_value);
    EXPECT_EQ(std::string(1024, 'x'), request.string_value);
    EXPECT_EQ(sequence, info.request_id.sequence_number);
    EXPECT_GT(info.source_timestamp, 0);
    ids.push_back(info.request_id);
  }
  for (int value = 2; value >= 0; --value) {
    Service::Response response;
    response.int64_value = value;
    ASSERT_EQ(0, rcl_send_response(&service, &ids[value], &response));
    ready_client();
    rmw_service_info_t info{};
    Service::Response received;
    ASSERT_EQ(0, rcl_take_response_with_info(&client, &info, &received));
    EXPECT_EQ(value, received.int64_value);
    EXPECT_EQ(ids[value].sequence_number, info.request_id.sequence_number);
    EXPECT_GT(info.received_timestamp, 0);
    EXPECT_NE(0, rcl_send_response(&service, &ids[value], &response));
    rcl_reset_error();
  }
}
TEST_F(ServiceBroker, DestroyedClientAndRecreatedService)
{
  int64_t sequence;
  send(42, &sequence);
  ready_service();
  Service::Request request;
  rmw_request_id_t id{};
  ASSERT_EQ(0, rcl_take_request(&service, &id, &request));
  ASSERT_EQ(0, rcl_client_fini(&client, &node));
  auto options = rcl_client_get_default_options();
  ASSERT_EQ(0, rcl_client_init(&client, &node, type, "/broker_service", &options));
  Service::Response response;
  ASSERT_EQ(0, rcl_send_response(&service, &id, &response));
  EXPECT_EQ(RCL_RET_CLIENT_TAKE_FAILED, rcl_take_response(&client, &id, &response));
  ASSERT_EQ(0, rcl_service_fini(&service, &node));
  auto so = rcl_service_get_default_options();
  ASSERT_EQ(0, rcl_service_init(&service, &node, type, "/broker_service", &so));
  send(43, &sequence);
  ready_service();
  ASSERT_EQ(0, rcl_take_request(&service, &id, &request));
  EXPECT_EQ(43, request.int64_value);
}
TEST_F(ServiceBroker, ResponseRouteSurvivesAnotherServer)
{
  int64_t sequence;
  send(7, &sequence);
  ready_service();
  Service::Request request;
  rmw_request_id_t id{};
  ASSERT_EQ(0, rcl_take_request(&service, &id, &request));
  auto another = rcl_get_zero_initialized_service();
  auto options = rcl_service_get_default_options();
  ASSERT_EQ(0, rcl_service_init(&another, &node, type, "/broker_service", &options));
  Service::Response response;
  response.int64_value = 8;
  ASSERT_EQ(0, rcl_send_response(&service, &id, &response));
  ready_client();
  ASSERT_EQ(0, rcl_take_response(&client, &id, &response));
  EXPECT_EQ(8, response.int64_value);
  EXPECT_EQ(0, rcl_service_fini(&another, &node));
}
TEST_F(ServiceBroker, CServerAndCppClient)
{
  ASSERT_EQ(0, rcl_service_fini(&service, &node));
  auto options = rcl_service_get_default_options();
  ASSERT_EQ(0, rcl_service_init(&service, &node,
      ROSIDL_GET_SRV_TYPE_SUPPORT(test_msgs, srv, BasicTypes), "/broker_service", &options));
  int64_t sequence;
  send(19, &sequence);
  ready_service();
  auto request = test_msgs__srv__BasicTypes_Request__create();
  auto response = test_msgs__srv__BasicTypes_Response__create();
  rmw_request_id_t id{};
  ASSERT_EQ(0, rcl_take_request(&service, &id, request));
  EXPECT_EQ(19, request->int64_value);
  EXPECT_STREQ(std::string(1024, 'x').c_str(), request->string_value.data);
  ASSERT_TRUE(rosidl_runtime_c__String__assign(&response->string_value, "reply"));
  ASSERT_EQ(0, rcl_send_response(&service, &id, response));
  ready_client();
  Service::Response result;
  ASSERT_EQ(0, rcl_take_response(&client, &id, &result));
  EXPECT_EQ("reply", result.string_value);
  test_msgs__srv__BasicTypes_Request__destroy(request);
  test_msgs__srv__BasicTypes_Response__destroy(response);
}
TEST_F(ServiceBroker, OwnedRequestAndBorrowedResponse)
{
  const void * previous = nullptr;
  for (int value = 0; value < 3; ++value) {
    int64_t sequence;
    send(value, &sequence);
    ready_service();
    rmw_service_info_t info{};
    rcl_ipc_message_t * request_message = nullptr;
    void * request_data = nullptr;
    ASSERT_EQ(0, rcl_take_ipc_request(&service, &info, &request_message, &request_data));
    auto request = static_cast<Service::Request *>(request_data);
    EXPECT_EQ(value, request->int64_value);
    EXPECT_EQ(std::string(1024, 'x'), request->string_value);
    rcl_ipc_message_t * response_message = nullptr;
    void * response_data = nullptr;
    ASSERT_EQ(
      0, rcl_borrow_ipc_response(&service, &info.request_id, &response_message, &response_data));
    auto response = static_cast<Service::Response *>(response_data);
    EXPECT_EQ(0, response->int64_value);
    EXPECT_EQ("", response->string_value);
    if (previous) {
      EXPECT_EQ(previous, response_data);
      EXPECT_GE(response->string_value.capacity(), 4096u);
    }
    response->int64_value = value + 10;
    if (value == 0) {response->string_value = std::string(4096, 'y');}
    ASSERT_EQ(0, rcl_send_ipc_response(&service, &info.request_id, response_message));
    rcl_ipc_message_release(response_message);
    rcl_ipc_message_release(request_message);
    EXPECT_NE(0, rcl_send_response(&service, &info.request_id, response_data));
    rcl_reset_error();
    ready_client();
    rcl_ipc_message_t * taken = nullptr;
    void * taken_data = nullptr;
    ASSERT_EQ(0, rcl_take_ipc_response(&client, &info, &taken, &taken_data));
    EXPECT_EQ(response_data, taken_data);
    EXPECT_EQ(sequence, info.request_id.sequence_number);
    auto result = static_cast<Service::Response *>(taken_data);
    EXPECT_EQ(value + 10, result->int64_value);
    EXPECT_EQ(value == 0 ? std::string(4096, 'y') : std::string(), result->string_value);
    rcl_ipc_message_release(taken);
    previous = response_data;
  }
  rcl_ipc_message_t * message = nullptr;
  void * data = nullptr;
  rmw_service_info_t info{};
  EXPECT_EQ(RCL_RET_SERVICE_TAKE_FAILED, rcl_take_ipc_request(&service, &info, &message, &data));
  EXPECT_EQ(RCL_RET_CLIENT_TAKE_FAILED, rcl_take_ipc_response(&client, &info, &message, &data));
}
TEST_F(ServiceBroker, RetainedRequestIsNotReused)
{
  int64_t sequence;
  send(1, &sequence);
  ready_service();
  rmw_service_info_t info{};
  rcl_ipc_message_t * first = nullptr;
  void * first_data = nullptr;
  ASSERT_EQ(0, rcl_take_ipc_request(&service, &info, &first, &first_data));
  Service::Response response;
  ASSERT_EQ(0, rcl_send_response(&service, &info.request_id, &response));
  send(2, &sequence);
  ready_service();
  Service::Request second;
  ASSERT_EQ(0, rcl_take_request_with_info(&service, &info, &second));
  EXPECT_EQ(2, second.int64_value);
  EXPECT_EQ(1, static_cast<Service::Request *>(first_data)->int64_value);
  rcl_ipc_message_release(first);
}
TEST_F(ServiceBroker, BorrowNeedsSameLayoutClient)
{
  ASSERT_EQ(0, rcl_client_fini(&client, &node));
  auto options = rcl_client_get_default_options();
  ASSERT_EQ(0, rcl_client_init(&client, &node,
      ROSIDL_GET_SRV_TYPE_SUPPORT(test_msgs, srv, BasicTypes), "/broker_service", &options));
  auto request = test_msgs__srv__BasicTypes_Request__create();
  request->int64_value = 3;
  int64_t sequence;
  ASSERT_EQ(0, rcl_send_request(&client, request, &sequence));
  test_msgs__srv__BasicTypes_Request__destroy(request);
  ready_service();
  rmw_service_info_t info{};
  rcl_ipc_message_t * message = nullptr;
  void * data = nullptr;
  ASSERT_EQ(0, rcl_take_ipc_request(&service, &info, &message, &data));
  EXPECT_EQ(3, static_cast<Service::Request *>(data)->int64_value);
  rcl_ipc_message_release(message);
  rcl_ipc_message_t * borrowed = nullptr;
  EXPECT_EQ(RCL_RET_UNSUPPORTED,
      rcl_borrow_ipc_response(&service, &info.request_id, &borrowed, &data));
  Service::Response response;
  response.int64_value = 4;
  ASSERT_EQ(0, rcl_send_response(&service, &info.request_id, &response));
  EXPECT_EQ(RCL_RET_UNSUPPORTED,
      rcl_borrow_ipc_response(&service, &info.request_id, &borrowed, &data));
  ready_client();
  auto result = test_msgs__srv__BasicTypes_Response__create();
  rmw_request_id_t id{};
  ASSERT_EQ(0, rcl_take_response(&client, &id, result));
  EXPECT_EQ(4, result->int64_value);
  test_msgs__srv__BasicTypes_Response__destroy(result);
}
}  // namespace
