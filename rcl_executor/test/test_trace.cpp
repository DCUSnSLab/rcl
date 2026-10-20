// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <rcl_executor/trace.h>
#include <vector>

namespace
{
enum Phase {ENTER, EXIT, CLAIM, COMPLETE};
struct Event
{
  Phase phase;
  const rcl_executor_t * executor;
  int64_t value;
  rcl_executor_entity_type_t type = RCL_EXECUTOR_WORK;
  uint64_t group = 0;
};
std::vector<Event> events;
rcl_executor_item_t claimed;
rcl_executor_trace_client_t claimed_client;
uint64_t claimed_group;

}  // namespace

extern "C"
{
void rcl_executor_trace_claim(
  const rcl_executor_t *, const rcl_executor_item_t * item,
  uint64_t group, rcl_executor_trace_client_t client)
{
  claimed = *item;
  claimed_group = group;
  claimed_client = client;
}
void rcl_executor_trace_wait_enter(const rcl_executor_t * executor, int64_t timeout_ns)
{
  events.push_back({ENTER, executor, timeout_ns});
}
void rcl_executor_trace_wait_exit(const rcl_executor_t * executor, rcl_executor_result_t result)
{
  events.push_back({EXIT, executor, result});
}
void rcl_executor_trace_item_claimed(
  const rcl_executor_t * executor, rcl_executor_id_t entity,
  rcl_executor_entity_type_t type, uint64_t group)
{
  events.push_back({CLAIM, executor, static_cast<int64_t>(entity), type, group});
}
void rcl_executor_trace_item_completed(const rcl_executor_t * executor, rcl_executor_id_t entity)
{
  events.push_back({COMPLETE, executor, static_cast<int64_t>(entity)});
}
}  // extern "C"

TEST(Trace, arguments_order_and_wait_results)
{
  events.clear();
  rcl_executor_t * executor;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_create(&executor));
  EXPECT_EQ(1, rcl_executor_trace_contract_1());
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_trace_client(executor, RCL_EXECUTOR_TRACE_RCLCPP));
  auto handle = rcl_executor_get_handle(executor);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 42, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  uint64_t id;
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, &id, 42, &id));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, id, 1));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_wait(executor, 123));
  rcl_executor_item_t item;
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(item.registration, claimed.registration);
  EXPECT_EQ(item.token, claimed.token);
  EXPECT_EQ(item.entity, claimed.entity);
  EXPECT_EQ(&id, claimed.context);
  EXPECT_EQ(42u, claimed_group);
  EXPECT_EQ(RCL_EXECUTOR_TRACE_RCLCPP, claimed_client);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  ASSERT_EQ(4u, events.size());
  EXPECT_EQ(ENTER, events[0].phase);
  EXPECT_EQ(123, events[0].value);
  EXPECT_EQ(EXIT, events[1].phase);
  EXPECT_EQ(RCL_EXECUTOR_READY, events[1].value);
  EXPECT_EQ(CLAIM, events[2].phase);
  EXPECT_EQ(static_cast<int64_t>(id), events[2].value);
  EXPECT_EQ(RCL_EXECUTOR_WORK, events[2].type);
  EXPECT_EQ(42u, events[2].group);
  EXPECT_EQ(COMPLETE, events[3].phase);
  EXPECT_EQ(static_cast<int64_t>(id), events[3].value);
  EXPECT_EQ(RCL_EXECUTOR_INTERRUPTED, rcl_executor_wait(executor, 0));
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_wait(executor, 0));
  rcl_executor_stop(handle);
  EXPECT_EQ(RCL_EXECUTOR_STOPPED, rcl_executor_wait(executor, -1));
  ASSERT_EQ(10u, events.size());
  EXPECT_EQ(RCL_EXECUTOR_INTERRUPTED, events[5].value);
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, events[7].value);
  EXPECT_EQ(-1, events[8].value);
  EXPECT_EQ(RCL_EXECUTOR_STOPPED, events[9].value);
  for (const auto & event : events) {
    EXPECT_EQ(executor, event.executor);
                                                                         }
  rcl_executor_destroy(executor);
  rcl_executor_handle_destroy(handle);
}
