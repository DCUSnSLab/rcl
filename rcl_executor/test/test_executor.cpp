// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <rcl_executor/executor.h>
#include <rcl/rcl.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include <vector>

class Core : public ::testing::Test
{
protected:
  rcl_executor_t * executor = nullptr;
  rcl_executor_handle_t * handle = nullptr;
  void SetUp() override
  {
    ASSERT_EQ(RCL_RET_OK, rcl_executor_create(&executor));
    handle = rcl_executor_get_handle(executor);
  }
  void TearDown() override
  {
    rcl_executor_destroy(executor);
    rcl_executor_handle_destroy(handle);
  }
  uint64_t add(uint64_t group = 0)
  {
    uint64_t id = 0;
    EXPECT_EQ(RCL_RET_OK,
      rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, group, &id));
    return id;
  }
};

TEST_F(Core, group_claim_is_deferred_and_counts_rotate)
{
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  auto a = add(1), b = add(1), c = add();
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, a, 2));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, b, 1));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, c, 1));
  rcl_executor_item_t first{}, second{}, third{};
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &first));
  EXPECT_EQ(a, first.registration);
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &second));
  EXPECT_EQ(c, second.registration);
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &third));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, first.token));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &third));
  EXPECT_EQ(b, third.registration);
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, second.token));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, third.token));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &first));
  EXPECT_EQ(a, first.registration);
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, first.token));
}

TEST_F(Core, no_spin_when_only_busy_groups_have_work)
{
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  auto id = add(1);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, id, 2));
  rcl_executor_item_t item{};
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  while (rcl_executor_wait(executor, 0) == RCL_EXECUTOR_INTERRUPTED) {}
  auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_wait(executor, 20000000));
  EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(19));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
}

TEST_F(Core, retained_handle_and_signal_stop)
{
  static rcl_executor_handle_t * signal_handle;
  signal_handle = handle;
  auto previous = std::signal(SIGUSR1, [](int) {rcl_executor_stop(signal_handle);});
  std::thread sender([] {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      std::raise(SIGUSR1);
    });
  while (rcl_executor_wait(executor, -1) != RCL_EXECUTOR_STOPPED) {}
  sender.join();
  std::signal(SIGUSR1, previous);
  rcl_executor_destroy(executor);
  executor = nullptr;
  errno = EDOM;
  rcl_executor_wake(handle);
  rcl_executor_interrupt(handle);
  rcl_executor_stop(handle);
  EXPECT_EQ(EDOM, errno);
}

TEST_F(Core, guard_coalesces_and_timer_advances_at_claim)
{
  auto allocator = rcl_get_default_allocator();
  auto options = rcl_get_zero_initialized_init_options();
  ASSERT_EQ(RCL_RET_OK, rcl_init_options_init(&options, allocator));
  auto context = rcl_get_zero_initialized_context();
  ASSERT_EQ(RCL_RET_OK, rcl_init(0, nullptr, &options, &context));
  auto gc = rcl_get_zero_initialized_guard_condition();
  ASSERT_EQ(RCL_RET_OK,
    rcl_guard_condition_init(&gc, &context, rcl_guard_condition_get_default_options()));
  for (int i = 0; i < 5; ++i) {
    ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&gc));
                                                                                       }
  uint64_t guard_id;
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_GUARD_CONDITION, &gc, nullptr, 0, &guard_id));
  rcl_executor_item_t item{};
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(1u, item.count);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_remove(executor, guard_id));
  rcl_clock_t clock;
  ASSERT_EQ(RCL_RET_OK, rcl_clock_init(RCL_ROS_TIME, &clock, &allocator));
  ASSERT_EQ(RCL_RET_OK, rcl_enable_ros_time_override(&clock));
  ASSERT_EQ(RCL_RET_OK, rcl_set_ros_time_override(&clock, 1000000000));
  auto timer = rcl_get_zero_initialized_timer();
  ASSERT_EQ(RCL_RET_OK,
    rcl_timer_init2(&timer, &clock, &context, 1000000000, nullptr, allocator, true));
  uint64_t timer_id;
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_TIMER, &timer, nullptr, 0, &timer_id));
  ASSERT_EQ(RCL_RET_OK, rcl_set_ros_time_override(&clock, 2000000000));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(2000000000, item.timer_info.expected_call_time);
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  ASSERT_EQ(RCL_RET_OK, rcl_set_ros_time_override(&clock, 500000000));
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_remove(executor, timer_id));
  EXPECT_EQ(RCL_RET_OK, rcl_timer_fini(&timer));
  EXPECT_EQ(RCL_RET_OK, rcl_clock_fini(&clock));
  EXPECT_EQ(RCL_RET_OK, rcl_guard_condition_fini(&gc));
  EXPECT_EQ(RCL_RET_OK, rcl_shutdown(&context));
  EXPECT_EQ(RCL_RET_OK, rcl_context_fini(&context));
  EXPECT_EQ(RCL_RET_OK, rcl_init_options_fini(&options));
}

TEST_F(Core, push_runs_posts_and_exclusive_groups_do_not_overlap)
{
  struct State
  {
    std::atomic<int> active{0}, maximum{0}, count{0};
    rcl_executor_handle_t * handle;
  } state;
  state.handle = handle;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(RCL_RET_OK, rcl_executor_post(executor, [](void * data) {
        auto & s = *static_cast<State *>(data);
        int n = ++s.active;
        int old = s.maximum.load();
        while (old < n && !s.maximum.compare_exchange_weak(old, n)) {}
        std::this_thread::yield();
        --s.active;
        if (++s.count == 100) {rcl_executor_stop(s.handle);}
      }, &state, 1));
  }
  EXPECT_EQ(RCL_RET_OK, rcl_executor_spin(executor, 4, nullptr, nullptr));
  EXPECT_EQ(100, state.count);
  EXPECT_EQ(1, state.maximum);
}

TEST_F(Core, equivalent_clock_handles_do_not_multiply_deadline_queries)
{
  struct Source
  {
    int reads = 0;
    static rcl_ret_t now(void * data, rcl_time_point_value_t * value)
    {
      ++static_cast<Source *>(data)->reads;
      *value = 0;
      return RCL_RET_OK;
    }
  } source;
  auto allocator = rcl_get_default_allocator();
  auto options = rcl_get_zero_initialized_init_options();
  ASSERT_EQ(RCL_RET_OK, rcl_init_options_init(&options, allocator));
  auto context = rcl_get_zero_initialized_context();
  ASSERT_EQ(RCL_RET_OK, rcl_init(0, nullptr, &options, &context));
  std::vector<rcl_clock_t> clocks(50);
  std::vector<rcl_timer_t> timers(50);
  std::vector<uint64_t> ids(50);
  for (size_t i = 0; i < timers.size(); ++i) {
    ASSERT_EQ(RCL_RET_OK, rcl_clock_init(RCL_STEADY_TIME, &clocks[i], &allocator));
    clocks[i].data = &source;
    clocks[i].get_now = Source::now;
    ASSERT_EQ(RCL_RET_OK, rcl_timer_init2(
        &timers[i], &clocks[i], &context, 1000000000, nullptr, allocator, true));
    ASSERT_EQ(RCL_RET_OK, rcl_executor_add(
        executor, RCL_EXECUTOR_TIMER, &timers[i], nullptr, 0, &ids[i]));
  }
  source.reads = 0;
  rcl_executor_wait(executor, 0);
  EXPECT_LE(source.reads, 2);
  for (size_t i = 0; i < timers.size(); ++i) {
    EXPECT_EQ(RCL_RET_OK, rcl_executor_remove(executor, ids[i]));
    EXPECT_EQ(RCL_RET_OK, rcl_timer_fini(&timers[i]));
    EXPECT_EQ(RCL_RET_OK, rcl_clock_fini(&clocks[i]));
  }
  EXPECT_EQ(RCL_RET_OK, rcl_shutdown(&context));
  EXPECT_EQ(RCL_RET_OK, rcl_context_fini(&context));
  EXPECT_EQ(RCL_RET_OK, rcl_init_options_fini(&options));
}

TEST(CorePolicy, priority_drains_highest_and_rotates_equal_priorities)
{
  rcl_executor_t * executor;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_create_with_policy(&executor, RCL_EXECUTOR_PRIORITY));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_REENTRANT));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 2, RCL_EXECUTOR_REENTRANT));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_group_priority(executor, 1, -5));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_group_priority(executor, 2, 10));
  uint64_t low, high, equal;
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 1, &low));
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 2, &high));
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 2, &equal));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, low, 3));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, high, 3));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, equal, 2));
  for (auto expected : {high, equal, high, equal, high, low, low, low}) {
    rcl_executor_item_t item;
    ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
    EXPECT_EQ(expected, item.registration);
    ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  }
  rcl_executor_destroy(executor);
}

TEST(CorePolicy, wait_set_round_defers_arrivals_and_takes_one_per_entity)
{
  rcl_executor_t * executor;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_create_with_policy(&executor, RCL_EXECUTOR_WAIT_SET));
  uint64_t first, second, late;
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 0, &first));
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 0, &second));
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 0, &late));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_order(executor, late, 0));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, second, 2));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, first, 2));
  rcl_executor_item_t item;
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(first, item.registration);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, late, 1));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(second, item.registration);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
  for (auto expected : {late, first, second}) {
    ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
    EXPECT_EQ(expected, item.registration);
    ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  }
  rcl_executor_destroy(executor);
}

TEST(CorePolicy, wait_set_skips_busy_groups_without_losing_their_backlog)
{
  rcl_executor_t * executor;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_create_with_policy(&executor, RCL_EXECUTOR_WAIT_SET));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  uint64_t first, skipped, other;
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 1, &first));
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 1, &skipped));
  ASSERT_EQ(RCL_RET_OK,
    rcl_executor_add(executor, RCL_EXECUTOR_WORK, nullptr, nullptr, 0, &other));
  for (auto id : {first, skipped, other}) {
    ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, id, 1));
  }
  rcl_executor_item_t held, item;
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &held));
  EXPECT_EQ(first, held.registration);
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(other, item.registration);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  ASSERT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, held.token));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_wait(executor, 0));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  EXPECT_EQ(skipped, item.registration);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  rcl_executor_destroy(executor);
}
