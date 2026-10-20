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
#include <rcl/error_handling.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <future>
#include <memory>
#include <thread>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

// Clients key groups by address, so a removed group's key returns as a new group of another type.
TEST_F(Core, removed_group_key_is_reused_with_another_type)
{
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  auto old = add(1);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_remove(executor, old));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_remove_group(executor, 1));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_REENTRANT));
  EXPECT_NE(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  rcl_reset_error();
  auto a = add(1);
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, a, 2));
  rcl_executor_item_t first{}, second{};
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &first));
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &second));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, first.token));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, second.token));
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

TEST_F(Core, thread_start_runs_once_on_each_dispatch_thread)
{
  struct State
  {
    std::mutex mutex;
    std::unordered_map<std::thread::id, size_t> starts;
    std::unordered_set<size_t> indices;
    std::atomic<size_t> calls{0};
    rcl_executor_handle_t * handle;
  } state;
  state.handle = handle;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_thread_start(executor,
    [](size_t index, uint64_t group, void * data) {
      auto & s = *static_cast<State *>(data);
      std::lock_guard<std::mutex> lock(s.mutex);
      EXPECT_EQ(0u, group);
      ++s.starts[std::this_thread::get_id()];
      EXPECT_TRUE(s.indices.insert(index).second);
    }, &state));
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(RCL_RET_OK, rcl_executor_post(executor, [](void * data) {
        auto & s = *static_cast<State *>(data);
        {
          std::lock_guard<std::mutex> lock(s.mutex);
          EXPECT_EQ(1u, s.starts.at(std::this_thread::get_id()));
        }
        if (++s.calls == 100) {rcl_executor_stop(s.handle);}
      }, &state, 0));
  }
  ASSERT_EQ(RCL_RET_OK, rcl_executor_spin(executor, 3, nullptr, nullptr));
  EXPECT_EQ(3u, state.starts.size());
  EXPECT_EQ(3u, state.indices.size());
}

TEST_F(Core, external_pending_removal_and_late_notification_are_safe)
{
  int calls = 0;
  uint64_t source;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_source(executor,
    [](void * data) {++*static_cast<int *>(data);}, &calls, 0, &source));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, source, 2));
  rcl_executor_item_t item;
  ASSERT_EQ(RCL_EXECUTOR_READY, rcl_executor_next(executor, &item));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_remove(executor, source));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, source, 1));
  item.function(item.context);
  EXPECT_EQ(1, calls);
  EXPECT_EQ(RCL_RET_OK, rcl_executor_complete(executor, item.token));
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
}

TEST_F(Core, partition_refuses_reentrant_groups)
{
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_partitioned(executor, true));
  auto source = add();
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, source, 1));
  rcl_executor_item_t item;
  EXPECT_EQ(RCL_EXECUTOR_TIMEOUT, rcl_executor_next(executor, &item));
  EXPECT_NE(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_REENTRANT));
  EXPECT_NE(std::string::npos, std::string(rcl_get_error_string().str).find("Reentrant"));
  rcl_reset_error();
}

TEST_F(Core, removed_partition_exits_without_delaying_new_groups)
{
  using namespace std::chrono_literals;
  struct Exit
  {
    std::promise<void> * ended;
    ~Exit() {if (ended) {ended->set_value();}}
  };
  struct State
  {
    std::promise<void> entered, release, ended, added;
    std::shared_future<void> released = release.get_future().share();
  } state;
  auto entered = state.entered.get_future();
  auto ended = state.ended.get_future();
  auto added = state.added.get_future();
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_partitioned(executor, true));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_thread_start(executor,
    [](size_t, uint64_t group, void * data) {
      thread_local std::unique_ptr<Exit> exit;
      if (group == 1) {
        exit = std::make_unique<Exit>(Exit{nullptr});
        exit->ended = &static_cast<State *>(data)->ended;
      }
    }, &state));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  uint64_t first;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_source(executor, [](void * data) {
      auto & s = *static_cast<State *>(data);
      s.entered.set_value();
      s.released.wait();
    }, &state, 1, &first));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, first, 1));
  std::thread spin([&] {EXPECT_EQ(RCL_RET_OK, rcl_executor_spin(executor, 1, nullptr, nullptr));});
  EXPECT_EQ(std::future_status::ready, entered.wait_for(1s));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_remove(executor, first));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_remove_group(executor, 1));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 2, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  uint64_t second;
  EXPECT_EQ(RCL_RET_OK, rcl_executor_add_source(executor,
    [](void * data) {static_cast<State *>(data)->added.set_value();}, &state, 2, &second));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_notify(executor, second, 1));
  EXPECT_EQ(std::future_status::ready, added.wait_for(100ms));
  state.release.set_value();
  EXPECT_EQ(std::future_status::ready, ended.wait_for(1s));
  rcl_executor_stop(handle);
  spin.join();
}

TEST_F(Core, partition_defaults_to_wait_set_order)
{
  struct State
  {
    rcl_executor_handle_t * handle;
    std::vector<int> sequence;
  } state{handle, {}};
  struct Source
  {
    State * state;
    int value;
  } first{&state, 0}, second{&state, 1};
  auto dispatch = [](void * data) {
      auto & source = *static_cast<Source *>(data);
      source.state->sequence.push_back(source.value);
      if (source.state->sequence.size() == 2) {rcl_executor_stop(source.state->handle);}
    };
  ASSERT_EQ(RCL_RET_OK, rcl_executor_set_partitioned(executor, true));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE));
  uint64_t a, b;
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_source(executor, dispatch, &first, 1, &a));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_add_source(executor, dispatch, &second, 1, &b));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, b, 1));
  ASSERT_EQ(RCL_RET_OK, rcl_executor_notify(executor, a, 1));
  EXPECT_EQ(RCL_RET_OK, rcl_executor_spin(executor, 1, nullptr, nullptr));
  EXPECT_EQ((std::vector<int>{0, 1}), state.sequence);
}
