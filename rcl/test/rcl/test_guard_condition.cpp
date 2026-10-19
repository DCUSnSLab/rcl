// Copyright 2017 Open Source Robotics Foundation, Inc.
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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <string>
#include <thread>
#include <vector>

#include "rcl/rcl.h"
#include "rcl/guard_condition.h"

#include "./failing_allocator_functions.hpp"
#include "osrf_testing_tools_cpp/memory_tools/memory_tools.hpp"
#include "osrf_testing_tools_cpp/scope_exit.hpp"
#include "rcl/error_handling.h"

#include "rmw/rmw.h"

#include "../mocking_utils/patch.hpp"

using osrf_testing_tools_cpp::memory_tools::on_unexpected_malloc;
using osrf_testing_tools_cpp::memory_tools::on_unexpected_realloc;
using osrf_testing_tools_cpp::memory_tools::on_unexpected_calloc;
using osrf_testing_tools_cpp::memory_tools::on_unexpected_free;

class TestGuardConditionFixture : public ::testing::Test
{
public:
  void SetUp()
  {
    osrf_testing_tools_cpp::memory_tools::initialize();
    on_unexpected_malloc([]() {ASSERT_FALSE(true) << "UNEXPECTED MALLOC";});
    on_unexpected_realloc([]() {ASSERT_FALSE(true) << "UNEXPECTED REALLOC";});
    on_unexpected_calloc([]() {ASSERT_FALSE(true) << "UNEXPECTED CALLOC";});
    on_unexpected_free([]() {ASSERT_FALSE(true) << "UNEXPECTED FREE";});
  }

  void TearDown()
  {
    osrf_testing_tools_cpp::memory_tools::uninitialize();
  }
};

/* Tests the guard condition accessors, i.e. rcl_guard_condition_get_* functions.
 */
TEST_F(TestGuardConditionFixture, test_rcl_guard_condition_accessors) {
  osrf_testing_tools_cpp::memory_tools::enable_monitoring_in_all_threads();

  rcl_ret_t ret;

  // Initialize rcl with rcl_init().
  rcl_init_options_t init_options = rcl_get_zero_initialized_init_options();
  ret = rcl_init_options_init(&init_options, rcl_get_default_allocator());
  ASSERT_EQ(RCL_RET_OK, ret) << rcl_get_error_string().str;
  OSRF_TESTING_TOOLS_CPP_SCOPE_EXIT(
  {
    osrf_testing_tools_cpp::memory_tools::disable_monitoring_in_all_threads();
    ASSERT_EQ(RCL_RET_OK, rcl_init_options_fini(&init_options));
  });
  rcl_context_t context = rcl_get_zero_initialized_context();
  ret = rcl_init(0, nullptr, &init_options, &context);
  ASSERT_EQ(RCL_RET_OK, ret) << rcl_get_error_string().str;
  // Setup automatic rcl_shutdown()
  OSRF_TESTING_TOOLS_CPP_SCOPE_EXIT(
  {
    osrf_testing_tools_cpp::memory_tools::disable_monitoring_in_all_threads();
    ASSERT_EQ(RCL_RET_OK, rcl_shutdown(&context));
    ASSERT_EQ(RCL_RET_OK, rcl_context_fini(&context));
  });

  // Create a zero initialized guard_condition (but not initialized).
  rcl_guard_condition_t zero_guard_condition = rcl_get_zero_initialized_guard_condition();

  // Create a normal guard_condition.
  rcl_guard_condition_options_t default_options = rcl_guard_condition_get_default_options();
  rcl_guard_condition_t guard_condition = rcl_get_zero_initialized_guard_condition();
  ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
  ASSERT_EQ(RCL_RET_OK, ret);
  // Setup automatic finalization of guard condition.
  OSRF_TESTING_TOOLS_CPP_SCOPE_EXIT(
  {
    osrf_testing_tools_cpp::memory_tools::disable_monitoring_in_all_threads();
    rcl_ret_t ret = rcl_guard_condition_fini(&guard_condition);
    EXPECT_EQ(RCL_RET_OK, ret);
  });

  // Test rcl_guard_condition_get_options().
  const rcl_guard_condition_options_t * actual_options;
  actual_options = rcl_guard_condition_get_options(nullptr);
  EXPECT_EQ(nullptr, actual_options);
  rcl_reset_error();
  actual_options = rcl_guard_condition_get_options(&zero_guard_condition);
  EXPECT_EQ(nullptr, actual_options);
  rcl_reset_error();
  EXPECT_NO_MEMORY_OPERATIONS(
  {
    actual_options = rcl_guard_condition_get_options(&guard_condition);
  });
  EXPECT_NE(nullptr, actual_options);
  if (actual_options) {
    EXPECT_EQ(default_options.allocator.allocate, actual_options->allocator.allocate);
  }
  // Test rcl_guard_condition_get_rmw_handle().
  rmw_guard_condition_t * gc_handle;
  gc_handle = rcl_guard_condition_get_rmw_handle(nullptr);
  EXPECT_EQ(nullptr, gc_handle);
  rcl_reset_error();
  gc_handle = rcl_guard_condition_get_rmw_handle(&zero_guard_condition);
  EXPECT_EQ(nullptr, gc_handle);
  rcl_reset_error();
  EXPECT_NO_MEMORY_OPERATIONS(
  {
    gc_handle = rcl_guard_condition_get_rmw_handle(&guard_condition);
  });
  EXPECT_NE(nullptr, gc_handle);
}

/* Tests the guard condition life cycle, including rcl_guard_condition_init/fini().
 */
TEST_F(TestGuardConditionFixture, test_rcl_guard_condition_life_cycle) {
  rcl_ret_t ret;
  rcl_context_t context = rcl_get_zero_initialized_context();
  rcl_guard_condition_t guard_condition = rcl_get_zero_initialized_guard_condition();
  rcl_guard_condition_options_t default_options = rcl_guard_condition_get_default_options();
  // Trying to init before rcl_init() should fail.
  ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
  ASSERT_EQ(RCL_RET_NOT_INIT, ret) << "Expected RCL_RET_NOT_INIT";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  // Initialize rcl with rcl_init().
  rcl_init_options_t init_options = rcl_get_zero_initialized_init_options();
  ret = rcl_init_options_init(&init_options, rcl_get_default_allocator());
  ASSERT_EQ(RCL_RET_OK, ret) << rcl_get_error_string().str;
  OSRF_TESTING_TOOLS_CPP_SCOPE_EXIT(
  {
    osrf_testing_tools_cpp::memory_tools::disable_monitoring_in_all_threads();
    ASSERT_EQ(RCL_RET_OK, rcl_init_options_fini(&init_options));
  });
  ret = rcl_init(0, nullptr, &init_options, &context);
  ASSERT_EQ(RCL_RET_OK, ret) << rcl_get_error_string().str;
  OSRF_TESTING_TOOLS_CPP_SCOPE_EXIT(
  {
    ASSERT_EQ(RCL_RET_OK, rcl_shutdown(&context));
    ASSERT_EQ(RCL_RET_OK, rcl_context_fini(&context));
  });
  // Try invalid arguments.
  ret = rcl_guard_condition_init(nullptr, &context, default_options);
  ASSERT_EQ(RCL_RET_INVALID_ARGUMENT, ret) << "Expected RCL_RET_INVALID_ARGUMENT";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  // Now with nullptr for context.
  ret = rcl_guard_condition_init(&guard_condition, nullptr, default_options);
  ASSERT_EQ(RCL_RET_INVALID_ARGUMENT, ret) << "Expected RCL_RET_INVALID_ARGUMENT";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  // Try with invalid allocator.
  rcl_guard_condition_options_t options_with_invalid_allocator =
    rcl_guard_condition_get_default_options();
  options_with_invalid_allocator.allocator.allocate = nullptr;
  options_with_invalid_allocator.allocator.deallocate = nullptr;
  options_with_invalid_allocator.allocator.reallocate = nullptr;
  ret = rcl_guard_condition_init(&guard_condition, &context, options_with_invalid_allocator);
  ASSERT_EQ(RCL_RET_INVALID_ARGUMENT, ret) << "Expected RCL_RET_INVALID_ARGUMENT";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  // Try with failing allocator.
  rcl_guard_condition_options_t options_with_failing_allocator =
    rcl_guard_condition_get_default_options();
  options_with_failing_allocator.allocator.allocate = failing_malloc;
  options_with_failing_allocator.allocator.reallocate = failing_realloc;
  options_with_failing_allocator.allocator.zero_allocate = failing_calloc;
  ret = rcl_guard_condition_init(&guard_condition, &context, options_with_failing_allocator);
  ASSERT_EQ(RCL_RET_BAD_ALLOC, ret) << "Expected RCL_RET_BAD_ALLOC";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  // Try init but force an internal error.
  {
    auto mock = mocking_utils::patch_to_fail(
      "lib:rcl", rmw_create_guard_condition, "internal error", nullptr);
    ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
    EXPECT_EQ(RCL_RET_ERROR, ret);
    rcl_reset_error();
  }

  // Try fini with invalid arguments.
  ret = rcl_guard_condition_fini(nullptr);
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT, ret) << "Expected RCL_RET_INVALID_ARGUMENT";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  // Try fini with an uninitialized guard_condition.
  ret = rcl_guard_condition_fini(&guard_condition);
  EXPECT_EQ(RCL_RET_OK, ret);
  // Try a normal init and fini.
  ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
  EXPECT_EQ(RCL_RET_OK, ret);
  ret = rcl_guard_condition_fini(&guard_condition);
  EXPECT_EQ(RCL_RET_OK, ret);
  // Try normal init and fini, but force an internal error on first try.
  {
    auto mock = mocking_utils::inject_on_return(
      "lib:rcl", rmw_destroy_guard_condition, RMW_RET_ERROR);
    ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
    EXPECT_EQ(RCL_RET_OK, ret);
    ret = rcl_guard_condition_fini(&guard_condition);
    EXPECT_EQ(RCL_RET_ERROR, ret);
    rcl_reset_error();
  }
  ret = rcl_guard_condition_fini(&guard_condition);
  EXPECT_EQ(RCL_RET_OK, ret);
  // Try repeated init and fini calls.
  ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
  EXPECT_EQ(RCL_RET_OK, ret);
  ret = rcl_guard_condition_init(&guard_condition, &context, default_options);
  EXPECT_EQ(RCL_RET_ALREADY_INIT, ret) << "Expected RCL_RET_ALREADY_INIT";
  ASSERT_TRUE(rcl_error_is_set());
  rcl_reset_error();
  ret = rcl_guard_condition_fini(&guard_condition);
  EXPECT_EQ(RCL_RET_OK, ret);
  rcl_reset_error();
  ret = rcl_guard_condition_fini(&guard_condition);
  EXPECT_EQ(RCL_RET_OK, ret);
  rcl_reset_error();
}

/* Tests trigger_guard_condition with bad arguments
 */
TEST_F(TestGuardConditionFixture, test_rcl_guard_condition_bad_arg) {
  rcl_guard_condition_t zero_guard_condition = rcl_get_zero_initialized_guard_condition();
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT, rcl_trigger_guard_condition(nullptr));
  rcl_reset_error();
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT, rcl_trigger_guard_condition(&zero_guard_condition));
  rcl_reset_error();
}

class TestGuardConditionCallback : public TestGuardConditionFixture
{
protected:
  void SetUp() override
  {
    TestGuardConditionFixture::SetUp();
    auto options = rcl_get_zero_initialized_init_options();
    ASSERT_EQ(RCL_RET_OK, rcl_init_options_init(&options, rcl_get_default_allocator()));
    ASSERT_EQ(RCL_RET_OK, rcl_init(0, nullptr, &options, &context));
    ASSERT_EQ(RCL_RET_OK, rcl_init_options_fini(&options));
    ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_init(
        &guard, &context, rcl_guard_condition_get_default_options()));
  }

  void TearDown() override
  {
    EXPECT_EQ(RCL_RET_OK, rcl_guard_condition_fini(&guard));
    EXPECT_EQ(RCL_RET_OK, rcl_shutdown(&context));
    EXPECT_EQ(RCL_RET_OK, rcl_context_fini(&context));
    TestGuardConditionFixture::TearDown();
  }

  rcl_context_t context = rcl_get_zero_initialized_context();
  rcl_guard_condition_t guard = rcl_get_zero_initialized_guard_condition();
};

TEST_F(TestGuardConditionCallback, invalid_arguments)
{
  auto zero = rcl_get_zero_initialized_guard_condition();
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT,
    rcl_guard_condition_set_on_trigger_callback(nullptr, nullptr, nullptr));
  rcl_reset_error();
  EXPECT_EQ(RCL_RET_INVALID_ARGUMENT,
    rcl_guard_condition_set_on_trigger_callback(&zero, nullptr, nullptr));
  rcl_reset_error();
}

TEST_F(TestGuardConditionCallback, callback_state_allocation_failure)
{
  struct State
  {
    size_t allocations = 0;
    size_t deallocations = 0;
  } state;
  auto options = rcl_guard_condition_get_default_options();
  options.allocator.state = &state;
  options.allocator.allocate = [](size_t size, void * data) -> void * {
      auto & state = *static_cast<State *>(data);
      if (++state.allocations == 2) {
        return nullptr;
      }
      return std::malloc(size);
    };
  options.allocator.deallocate = [](void * ptr, void * data) {
      ++static_cast<State *>(data)->deallocations;
      std::free(ptr);
    };
  auto other = rcl_get_zero_initialized_guard_condition();
  EXPECT_EQ(RCL_RET_BAD_ALLOC, rcl_guard_condition_init(&other, &context, options));
  EXPECT_EQ(nullptr, other.impl);
  EXPECT_EQ(2u, state.allocations);
  EXPECT_EQ(1u, state.deallocations);
  rcl_reset_error();
}

TEST_F(TestGuardConditionCallback, counts_replacement_and_clear)
{
  std::vector<size_t> counts;
  auto callback = [](const void * data, size_t count) {
      auto & counts = *static_cast<std::vector<size_t> *>(const_cast<void *>(data));
      counts.push_back(count);
    };
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &counts));
  EXPECT_EQ((std::vector<size_t>{2}), counts);
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  EXPECT_EQ((std::vector<size_t>{2, 1}), counts);

  std::vector<size_t> replacement;
  ASSERT_EQ(RCL_RET_OK,
    rcl_guard_condition_set_on_trigger_callback(&guard, callback, &replacement));
  EXPECT_TRUE(replacement.empty());
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  EXPECT_EQ((std::vector<size_t>{1}), replacement);
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, nullptr, nullptr));
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  EXPECT_EQ((std::vector<size_t>{1}), replacement);
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &counts));
  EXPECT_EQ((std::vector<size_t>{2, 1, 2}), counts);

  auto null_data_callback = [](const void * data, size_t count) {
      EXPECT_EQ(nullptr, data);
      EXPECT_EQ(1u, count);
    };
  ASSERT_EQ(RCL_RET_OK,
    rcl_guard_condition_set_on_trigger_callback(&guard, null_data_callback, nullptr));
  EXPECT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
}

TEST_F(TestGuardConditionCallback, rmw_triggers_are_not_reported_and_wait_set_still_wakes)
{
  size_t count = 0;
  auto callback = [](const void * data, size_t count) {
      *static_cast<size_t *>(const_cast<void *>(data)) += count;
    };
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &count));
  auto wait_set = rcl_get_zero_initialized_wait_set();
  ASSERT_EQ(RCL_RET_OK, rcl_wait_set_init(
      &wait_set, 0, 1, 0, 0, 0, 0, &context, rcl_get_default_allocator()));
  OSRF_TESTING_TOOLS_CPP_SCOPE_EXIT({EXPECT_EQ(RCL_RET_OK, rcl_wait_set_fini(&wait_set));});
  ASSERT_EQ(RCL_RET_OK, rcl_wait_set_add_guard_condition(&wait_set, &guard, nullptr));
  ASSERT_EQ(RMW_RET_OK, rmw_trigger_guard_condition(rcl_guard_condition_get_rmw_handle(&guard)));
  EXPECT_EQ(0u, count);
  ASSERT_EQ(RCL_RET_OK, rcl_wait(&wait_set, 1000000000));
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  EXPECT_EQ(1u, count);
  ASSERT_EQ(RCL_RET_OK, rcl_wait(&wait_set, 1000000000));
  EXPECT_EQ(&guard, wait_set.guard_conditions[0]);
}

TEST_F(TestGuardConditionCallback, failed_trigger_is_not_reported)
{
  auto mock = mocking_utils::patch_and_return(
    "lib:rcl", rmw_trigger_guard_condition, RMW_RET_ERROR);
  EXPECT_EQ(RCL_RET_ERROR, rcl_trigger_guard_condition(&guard));
  rcl_reset_error();
  size_t count = 0;
  auto callback = [](const void * data, size_t events) {
      *static_cast<size_t *>(const_cast<void *>(data)) += events;
    };
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &count));
  EXPECT_EQ(0u, count);
}

TEST_F(TestGuardConditionCallback, concurrent_trigger_registration_replacement_and_clear)
{
  std::atomic<size_t> first{0};
  std::atomic<size_t> second{0};
  std::atomic<size_t> errors{0};
  std::atomic<bool> start{false};
  auto callback = [](const void * data, size_t count) {
      *static_cast<std::atomic<size_t> *>(const_cast<void *>(data)) += count;
    };
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([&]() {
        while (!start.load()) {std::this_thread::yield();}
        for (size_t j = 0; j < 1000; ++j) {
          if (RCL_RET_OK != rcl_trigger_guard_condition(&guard)) {++errors;}
        }
      });
  }
  threads.emplace_back([&]() {
      start.store(true);
      for (size_t i = 0; i < 1000; ++i) {
        if (RCL_RET_OK != rcl_guard_condition_set_on_trigger_callback(&guard, callback, &first)) {
          ++errors;
        }
        if (RCL_RET_OK != rcl_guard_condition_set_on_trigger_callback(&guard, callback, &second)) {
          ++errors;
        }
        if (RCL_RET_OK != rcl_guard_condition_set_on_trigger_callback(&guard, nullptr, nullptr)) {
          ++errors;
        }
      }
    });
  for (auto & thread : threads) {
    thread.join();
  }
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &first));
  EXPECT_EQ(0u, errors.load());
  EXPECT_EQ(4000u, first.load() + second.load());
}

TEST_F(TestGuardConditionCallback, clear_waits_for_in_flight_callback)
{
  struct State
  {
    std::promise<void> entered;
    std::shared_future<void> release;
  };
  std::promise<void> release;
  State state{{}, release.get_future().share()};
  auto entered = state.entered.get_future();
  auto callback = [](const void * data, size_t) {
      auto & state = *static_cast<State *>(const_cast<void *>(data));
      state.entered.set_value();
      state.release.wait();
    };
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &state));
  auto trigger = std::async(
    std::launch::async, [&]() {
      return rcl_trigger_guard_condition(&guard);
    });
  entered.wait();
  std::promise<void> clearing;
  auto clearing_started = clearing.get_future();
  auto clear = std::async(
    std::launch::async, [&]() {
      clearing.set_value();
      return rcl_guard_condition_set_on_trigger_callback(&guard, nullptr, nullptr);
    });
  clearing_started.wait();
  EXPECT_EQ(std::future_status::timeout, clear.wait_for(std::chrono::milliseconds(20)));
  release.set_value();
  EXPECT_EQ(RCL_RET_OK, trigger.get());
  EXPECT_EQ(RCL_RET_OK, clear.get());
}

TEST_F(TestGuardConditionCallback, callback_can_clear_itself)
{
  ASSERT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
  auto callback = [](const void * data, size_t count) {
      auto guard = static_cast<rcl_guard_condition_t *>(const_cast<void *>(data));
      EXPECT_EQ(1u, count);
      EXPECT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(guard, nullptr, nullptr));
    };
  ASSERT_EQ(RCL_RET_OK, rcl_guard_condition_set_on_trigger_callback(&guard, callback, &guard));
  EXPECT_EQ(RCL_RET_OK, rcl_trigger_guard_condition(&guard));
}
