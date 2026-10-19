// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include "rcl_executor/executor.h"

#include <rcl/client.h>
#include <rcl/error_handling.h>
#include <rcl/event.h>
#include <rcl/guard_condition.h>
#include <rcl/service.h>
#include <rcl/subscription.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#ifdef __linux__
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace
{
using Steady = std::chrono::steady_clock;
using Ns = std::chrono::nanoseconds;

struct Control
{
  alignas(4) std::atomic<uint32_t> bell{0};
  std::atomic<uint32_t> heard{0};
  // Pairs with bell: a ringer that sees no sleeper is seen by the next sleeper's futex compare.
  std::atomic<uint32_t> sleepers{0};
#ifndef __linux__
  std::mutex mutex;
  std::condition_variable condition;
#endif

  void ring(uint32_t flag = 0, int threads = INT_MAX) noexcept
  {
    int saved = errno;
    if (flag) {bell.fetch_or(flag, std::memory_order_seq_cst);}
    bell.fetch_add(4, std::memory_order_seq_cst);
#ifdef __linux__
    if (sleepers.load(std::memory_order_seq_cst)) {
      syscall(SYS_futex, &bell, FUTEX_WAKE_PRIVATE, threads, nullptr, nullptr, 0);
    }
#else
    condition.notify_all();
#endif
    errno = saved;
  }

  void sleep(uint32_t expected, int64_t ns)
  {
#ifdef __linux__
    timespec timeout{};
    if (ns >= 0) {
      timeout.tv_sec = ns / 1000000000;
      timeout.tv_nsec = ns % 1000000000;
    }
    sleepers.fetch_add(1, std::memory_order_seq_cst);
    syscall(SYS_futex, &bell, FUTEX_WAIT_PRIVATE, expected,
      ns < 0 ? nullptr : &timeout, nullptr, 0);
    sleepers.fetch_sub(1, std::memory_order_relaxed);
#else
    std::unique_lock<std::mutex> lock(mutex);
    auto changed = [&] {return bell.load(std::memory_order_acquire) != expected;};
    if (ns < 0) {condition.wait(lock, changed);} else {condition.wait_for(lock, Ns(ns), changed);}
#endif
  }
};
static_assert(std::atomic<uint32_t>::is_always_lock_free,
    "signal handles require lock-free atomics");

struct Group
{
  rcl_executor_group_type_t type;
  size_t active = 0;
};
struct Entry;
struct Clock
{
  uint64_t id;
  rcl_clock_t * clock;
  Control * control;
  std::atomic<bool> dirty{true};
  std::vector<Entry *> heap;
  void exchange(size_t a, size_t b);
  void up(size_t index);
  void down(size_t index);
  void insert(Entry & entry);
  void erase(Entry & entry);

  static void jump(const rcl_time_jump_t *, bool before, void * data)
  {
    if (!before) {
      auto self = static_cast<Clock *>(data);
      self->dirty.store(true, std::memory_order_release);
      self->control->ring();
    }
  }
};
struct Entry
{
  rcl_executor_t * owner;
  rcl_executor_id_t id;
  rcl_executor_entity_type_t type;
  const void * entity;
  void * context;
  uint64_t group;
  size_t pending = 0;
  size_t active = 0;
  bool registered = true;
  bool one_shot = false;
  bool in_heap = false;
  size_t position = 0;
  int64_t deadline = 0;
  Clock * clock = nullptr;
  rcl_clock_t * timer_clock = nullptr;
  rcl_executor_work_fn function = nullptr;
};

void Clock::exchange(size_t a, size_t b)
{
  std::swap(heap[a], heap[b]);
  heap[a]->position = a;
  heap[b]->position = b;
}

bool earlier(const Entry * a, const Entry * b)
{
  return a->deadline < b->deadline || (a->deadline == b->deadline && a->id < b->id);
}

void Clock::up(size_t index)
{
  while (index > 0) {
    size_t parent = (index - 1) / 2;
    if (!earlier(heap[index], heap[parent])) {break;}
    exchange(index, parent);
    index = parent;
  }
}

void Clock::down(size_t index)
{
  for (;; ) {
    size_t child = index * 2 + 1;
    if (child >= heap.size()) {break;}
    if (child + 1 < heap.size() && earlier(heap[child + 1], heap[child])) {++child;}
    if (!earlier(heap[child], heap[index])) {break;}
    exchange(index, child);
    index = child;
  }
}

void Clock::insert(Entry & entry)
{
  entry.position = heap.size();
  entry.in_heap = true;
  heap.push_back(&entry);
  up(entry.position);
}

void Clock::erase(Entry & entry)
{
  size_t index = entry.position;
  exchange(index, heap.size() - 1);
  heap.pop_back();
  entry.in_heap = false;
  if (index < heap.size()) {
    if (index > 0 && earlier(heap[index], heap[(index - 1) / 2])) {up(index);} else {down(index);}
  }
}

template<typename F>
rcl_ret_t boundary(F && function)
{
  try {
    function();
    return RCL_RET_OK;
  } catch (const std::bad_alloc &) {
    RCL_SET_ERROR_MSG("executor allocation failed");
    return RCL_RET_BAD_ALLOC;
  } catch (const std::exception & error) {
    RCL_SET_ERROR_MSG(error.what());
    return RCL_RET_ERROR;
  }
}
void check(rcl_ret_t result)
{
  if (result != RCL_RET_OK) {
    throw std::runtime_error(rcl_get_error_string().str);
  }
}
}  // namespace

struct rcl_executor_handle_s
{
  std::shared_ptr<Control> control;
};

struct rcl_executor_s
{
  std::shared_ptr<Control> control = std::make_shared<Control>();
  std::mutex mutex;
  std::unordered_map<uint64_t, Group> groups{{0, {RCL_EXECUTOR_REENTRANT, 0}}};
  std::unordered_map<rcl_executor_id_t, std::shared_ptr<Entry>> entries;
  std::unordered_map<uint64_t, std::shared_ptr<Entry>> claims;
  std::unordered_map<uint64_t, std::unique_ptr<Clock>> clocks;
  std::deque<std::shared_ptr<Entry>> ready;
  uint64_t sequence = 0;
  uint64_t token_sequence = 0;
  uint64_t clock_sequence = 0;
  std::atomic<bool> spinning{false};

  static void notification(const void * data, size_t count)
  {
    auto entry = const_cast<Entry *>(static_cast<const Entry *>(data));
    auto self = entry->owner;
    {
      std::lock_guard<std::mutex> lock(self->mutex);
      self->enqueue(self->entries.at(entry->id), count);
    }
    self->control->ring(0, 1);
  }

  static void reset_timer(const void * data, size_t)
  {
    auto entry = static_cast<const Entry *>(data);
    entry->clock->dirty.store(true, std::memory_order_release);
    entry->owner->control->ring();
  }

  static rcl_ret_t callback(Entry & e, bool install)
  {
    auto cb = install ? notification : nullptr;
    const void * data = install ? &e : nullptr;
    switch (e.type) {
      case RCL_EXECUTOR_SUBSCRIPTION:
        return rcl_subscription_set_on_new_message_callback(
          static_cast<const rcl_subscription_t *>(e.entity), cb, data);
      case RCL_EXECUTOR_SERVICE:
        return rcl_service_set_on_new_request_callback(
          static_cast<const rcl_service_t *>(e.entity), cb, data);
      case RCL_EXECUTOR_CLIENT:
        return rcl_client_set_on_new_response_callback(
          static_cast<const rcl_client_t *>(e.entity), cb, data);
      case RCL_EXECUTOR_EVENT:
        return rcl_event_set_callback(static_cast<const rcl_event_t *>(e.entity), cb, data);
      case RCL_EXECUTOR_GUARD_CONDITION:
        return rcl_guard_condition_set_on_trigger_callback(
          const_cast<rcl_guard_condition_t *>(static_cast<const rcl_guard_condition_t *>(e.entity)),
          cb, data);
      case RCL_EXECUTOR_TIMER:
      case RCL_EXECUTOR_TIMER_OBSERVER:
        return rcl_timer_set_on_reset_callback(
          const_cast<rcl_timer_t *>(static_cast<const rcl_timer_t *>(e.entity)),
          install ? reset_timer : nullptr, data);
      case RCL_EXECUTOR_WORK:
        return RCL_RET_OK;
    }
    return RCL_RET_INVALID_ARGUMENT;
  }

  void enqueue(const std::shared_ptr<Entry> & entry, size_t count)
  {
    if (!entry->registered || count == 0) {return;}
    if (entry->pending == 0) {ready.push_back(entry);}
    if (entry->type == RCL_EXECUTOR_GUARD_CONDITION || entry->type == RCL_EXECUTOR_TIMER ||
      entry->type == RCL_EXECUTOR_TIMER_OBSERVER)
    {
      entry->pending = 1;
    } else {
      entry->pending += count;
    }
  }

  void schedule(Entry & entry)
  {
    auto & clock = *entry.clock;
    if (entry.in_heap) {
      clock.erase(entry);
    }
    bool canceled = false;
    auto timer = static_cast<const rcl_timer_t *>(entry.entity);
    check(rcl_timer_is_canceled(timer, &canceled));
    if (!canceled && entry.pending == 0 && entry.registered &&
      (entry.type != RCL_EXECUTOR_TIMER_OBSERVER || entry.active == 0))
    {
      int64_t deadline;
      check(rcl_timer_get_next_call_time(timer, &deadline));
      entry.deadline = deadline;
      clock.insert(entry);
    }
  }

  int64_t timers()
  {
    int64_t delay = -1;
    for (auto & pair : clocks) {
      auto & clock = *pair.second;
      if (clock.dirty.exchange(false, std::memory_order_acq_rel)) {
        for (auto & entry : entries) {
          if (entry.second->clock == &clock) {schedule(*entry.second);}
        }
      }
      int64_t now;
      check(rcl_clock_get_now(clock.clock, &now));
      while (!clock.heap.empty() && clock.heap.front()->deadline <= now) {
        auto entry = clock.heap.front();
        clock.erase(*entry);
        bool is_ready;
        check(rcl_timer_is_ready(static_cast<const rcl_timer_t *>(entry->entity), &is_ready));
        if (is_ready) {enqueue(entries.at(entry->id), 1);} else {schedule(*entry);}
      }
      bool ros = false;
      if (clock.clock->type == RCL_ROS_TIME) {
        check(rcl_is_enabled_ros_time_override(clock.clock, &ros));
      }
      if (!ros && !clock.heap.empty()) {
        int64_t remaining = std::max<int64_t>(0, clock.heap.front()->deadline - now);
        if (delay < 0 || remaining < delay) {delay = remaining;}
      }
    }
    return delay;
  }

  bool eligible(const Entry & entry)
  {
    const auto & group = groups.at(entry.group);
    return entry.registered && entry.pending &&
           (group.type == RCL_EXECUTOR_REENTRANT || group.active == 0);
  }

  // A spin thread completing its own claim loops back to claim again, so it needs no wake.
  rcl_ret_t complete(uint64_t token, bool wake)
  {
    auto executor = this;
    return boundary([&] {
               std::lock_guard<std::mutex> lock(executor->mutex);
               auto found = executor->claims.find(token);
               if (found == executor->claims.end()) {
                 throw std::invalid_argument("unknown completion token");
               }
               auto entry = found->second;
               auto & group = executor->groups.at(entry->group);
               --group.active;
               --entry->active;
               bool observer = entry->registered && entry->type == RCL_EXECUTOR_TIMER_OBSERVER;
               if (observer) {
                 executor->schedule(*entry);
               }
               executor->claims.erase(found);
               if (entry->one_shot) {executor->entries.erase(entry->id);}
             // Only a released group or a rescheduled observer can change what waiters see.
               if (observer || (wake &&
               group.type == RCL_EXECUTOR_MUTUALLY_EXCLUSIVE && executor->any_eligible()))
               {
                 executor->control->ring(0, 1);
               }
    });
  }

  bool any_eligible()
  {
    return std::any_of(ready.begin(), ready.end(),
             [this](const auto & entry) {return eligible(*entry);});
  }
};

extern "C"
{
rcl_ret_t rcl_executor_create(rcl_executor_t ** executor)
{
  return boundary([&] {*executor = new rcl_executor_t;});
}

void rcl_executor_destroy(rcl_executor_t * executor)
{
  if (!executor) {return;}
  executor->control->ring(1);
  while (!executor->entries.empty()) {
    auto result = rcl_executor_remove(executor, executor->entries.begin()->first);
    if (result != RCL_RET_OK) {rcl_reset_error();}
  }
  for (auto & pair : executor->clocks) {
    auto result = rcl_clock_remove_jump_callback(pair.second->clock, Clock::jump,
        pair.second.get());
    if (result != RCL_RET_OK) {rcl_reset_error();}
  }
  delete executor;
}

rcl_executor_handle_t * rcl_executor_get_handle(rcl_executor_t * executor)
{
  try {
    return new rcl_executor_handle_t{executor->control};
  } catch (...) {
    return nullptr;
  }
}
void rcl_executor_handle_destroy(rcl_executor_handle_t * handle) {delete handle;}
void rcl_executor_wake(rcl_executor_handle_t * handle) {handle->control->ring();}
void rcl_executor_interrupt(rcl_executor_handle_t * handle) {handle->control->ring(2);}
void rcl_executor_stop(rcl_executor_handle_t * handle) {handle->control->ring(1);}
void rcl_executor_reset(rcl_executor_t * executor)
{
  executor->control->bell.fetch_and(~uint32_t(3), std::memory_order_acq_rel);
}

rcl_ret_t rcl_executor_add_group(
  rcl_executor_t * executor, uint64_t group, rcl_executor_group_type_t type)
{
  return boundary([&] {
             std::lock_guard<std::mutex> lock(executor->mutex);
             auto inserted = executor->groups.emplace(group, Group{type, 0});
             if (!inserted.second && inserted.first->second.type != type) {
               throw std::invalid_argument("group type cannot change");
             }
    });
}

rcl_ret_t rcl_executor_add(
  rcl_executor_t * executor, rcl_executor_entity_type_t type, const void * entity,
  void * context, uint64_t group, rcl_executor_id_t * registration)
{
  return boundary([&] {
             auto entry = std::make_shared<Entry>();
             entry->owner = executor;
             entry->type = type;
             entry->entity = entity;
             entry->context = context;
             entry->group = group;
             {
               std::lock_guard<std::mutex> lock(executor->mutex);
               if (executor->groups.count(group) == 0) {
                 throw std::invalid_argument("unknown group");
               }
               entry->id = ++executor->sequence;
               if (type == RCL_EXECUTOR_TIMER || type == RCL_EXECUTOR_TIMER_OBSERVER) {
                 rcl_clock_t * clock;
                 check(rcl_timer_clock(static_cast<const rcl_timer_t *>(entity), &clock));
                 entry->timer_clock = clock;
                 auto found = std::find_if(executor->clocks.begin(), executor->clocks.end(),
                 [clock](const auto & pair) {
                   auto existing = pair.second->clock;
                   if (clock->type == RCL_ROS_TIME) {return existing == clock;}
                   return existing->type == clock->type && existing->get_now == clock->get_now &&
                          existing->data == clock->data;
                   });
                 if (found == executor->clocks.end()) {
                   auto manager = std::make_unique<Clock>();
                   manager->id = ++executor->clock_sequence;
                   manager->clock = clock;
                   manager->control = executor->control.get();
                   if (clock->type == RCL_ROS_TIME) {
                     rcl_jump_threshold_t threshold{true, {1}, {-1}};
                     check(rcl_clock_add_jump_callback(clock, threshold, Clock::jump,
                manager.get()));
                   }
                   auto id = manager->id;
                   found = executor->clocks.emplace(id, std::move(manager)).first;
                 }
                 entry->clock = found->second.get();
               }
               executor->entries.emplace(entry->id, entry);
               if (entry->clock) {executor->schedule(*entry);}
             }
      // Middleware may synchronously replay existing readiness while installing the callback.
             auto result = rcl_executor_t::callback(*entry, true);
             if (result != RCL_RET_OK) {
               auto ignored = rcl_executor_remove(executor, entry->id);
               (void)ignored;
               check(result);
             }
             *registration = entry->id;
             executor->control->ring();
    });
}

rcl_ret_t rcl_executor_remove(rcl_executor_t * executor, rcl_executor_id_t registration)
{
  return boundary([&] {
             std::shared_ptr<Entry> entry;
             {
               std::lock_guard<std::mutex> lock(executor->mutex);
               auto found = executor->entries.find(registration);
               if (found == executor->entries.end()) {return;}
               entry = found->second;
               entry->registered = false;
               if (entry->in_heap) {
                 entry->clock->erase(*entry);
               }
             }
      // Clearing a readiness callback joins in-flight middleware callbacks.
             check(rcl_executor_t::callback(*entry, false));
             std::lock_guard<std::mutex> lock(executor->mutex);
             executor->entries.erase(registration);
             executor->ready.erase(std::remove(executor->ready.begin(), executor->ready.end(),
        entry),
        executor->ready.end());
             auto clock = entry->clock;
             if (clock) {
               auto remaining = std::find_if(executor->entries.begin(), executor->entries.end(),
               [clock](const auto & pair) {return pair.second->clock == clock;});
               if (remaining == executor->entries.end()) {
                 if (clock->clock->type == RCL_ROS_TIME) {
                   check(rcl_clock_remove_jump_callback(clock->clock, Clock::jump, clock));
                 }
                 executor->clocks.erase(clock->id);
               } else {
                 clock->clock = remaining->second->timer_clock;
               }
             }
             executor->control->ring();
    });
}

rcl_ret_t rcl_executor_notify(
  rcl_executor_t * executor, rcl_executor_id_t registration, size_t count)
{
  return boundary([&] {
             {
               std::lock_guard<std::mutex> lock(executor->mutex);
               auto found = executor->entries.find(registration);
               if (found == executor->entries.end()) {return;}
               executor->enqueue(found->second, count);
             }
             executor->control->ring();
    });
}

rcl_ret_t rcl_executor_post(
  rcl_executor_t * executor, rcl_executor_work_fn function, void * context, uint64_t group)
{
  return boundary([&] {
             std::lock_guard<std::mutex> lock(executor->mutex);
             if (executor->groups.count(group) == 0) {throw std::invalid_argument("unknown group");}
             auto entry = std::make_shared<Entry>();
             entry->owner = executor;
             entry->id = ++executor->sequence;
             entry->type = RCL_EXECUTOR_WORK;
             entry->entity = nullptr;
             entry->context = context;
             entry->group = group;
             entry->function = function;
             entry->one_shot = true;
             executor->entries.emplace(entry->id, entry);
             executor->enqueue(entry, 1);
             executor->control->ring();
    });
}

rcl_executor_result_t rcl_executor_wait(rcl_executor_t * executor, int64_t timeout_ns)
{
  auto start = Steady::now();
  try {
    for (;; ) {
      auto bell = executor->control->bell.load(std::memory_order_acquire);
      if (bell & 1) {return RCL_EXECUTOR_STOPPED;}
      int64_t delay;
      {
        std::lock_guard<std::mutex> lock(executor->mutex);
        delay = executor->timers();
        if (executor->any_eligible()) {
          return RCL_EXECUTOR_READY;
        }
      }
      if (bell != executor->control->heard.exchange(bell, std::memory_order_acq_rel)) {
        return RCL_EXECUTOR_INTERRUPTED;
      }
      if (timeout_ns >= 0) {
        auto remaining = timeout_ns - std::chrono::duration_cast<Ns>(Steady::now() - start).count();
        if (remaining <= 0) {return RCL_EXECUTOR_TIMEOUT;}
        if (delay < 0 || remaining < delay) {delay = remaining;}
      }
      executor->control->sleep(bell, delay);
    }
  } catch (const std::exception & error) {
    RCL_SET_ERROR_MSG(error.what());
    return RCL_EXECUTOR_ERROR;
  }
}

rcl_executor_result_t rcl_executor_next(rcl_executor_t * executor, rcl_executor_item_t * item)
{
  return rcl_executor_next_excluding(executor, item, nullptr, 0);
}

rcl_executor_result_t rcl_executor_next_excluding(
  rcl_executor_t * executor, rcl_executor_item_t * item,
  const rcl_executor_id_t * excluded, size_t excluded_count)
{
  try {
    std::lock_guard<std::mutex> lock(executor->mutex);
    if (executor->control->bell.load(std::memory_order_acquire) & 1) {return RCL_EXECUTOR_STOPPED;}
    executor->timers();
    std::shared_ptr<Entry> entry;
    for (;; ) {
      auto found = std::find_if(executor->ready.begin(), executor->ready.end(),
          [&](const auto & candidate) {
            return executor->eligible(*candidate) && (excluded_count == 0 ||
                   std::find(excluded, excluded + excluded_count, candidate->id) ==
                   excluded + excluded_count);
        });
      if (found == executor->ready.end()) {return RCL_EXECUTOR_TIMEOUT;}
      entry = *found;
      executor->ready.erase(found);
      --entry->pending;
      if (entry->pending) {executor->ready.push_back(entry);}
      if (entry->clock) {
        bool ready;
        check(rcl_timer_is_ready(static_cast<const rcl_timer_t *>(entry->entity), &ready));
        if (!ready) {
          executor->schedule(*entry);
          continue;
        }
      }
      break;
    }
    *item = {};
    item->registration = entry->id;
    item->token = ++executor->token_sequence;
    item->type = entry->type;
    item->entity = entry->entity;
    item->context = entry->context;
    item->count = 1;
    item->function = entry->function;
    if (entry->type == RCL_EXECUTOR_TIMER) {
      auto result = rcl_timer_call_with_info(
        const_cast<rcl_timer_t *>(static_cast<const rcl_timer_t *>(entry->entity)),
          &item->timer_info);
      if (result == RCL_RET_TIMER_CANCELED) {return RCL_EXECUTOR_TIMEOUT;}
      check(result);
      executor->schedule(*entry);
    }
    ++executor->groups.at(entry->group).active;
    ++entry->active;
    executor->claims.emplace(item->token, entry);
    // Readiness wakes one thread; it passes the wake on while claimable work remains.
    // Readiness wakes one thread; it passes the wake on while claimable work remains.
    if (executor->any_eligible()) {executor->control->ring(0, 1);}
    return RCL_EXECUTOR_READY;
  } catch (const std::exception & error) {
    RCL_SET_ERROR_MSG(error.what());
    return RCL_EXECUTOR_ERROR;
  }
}

rcl_ret_t rcl_executor_complete(rcl_executor_t * executor, uint64_t token)
{
  return executor->complete(token, true);
}

rcl_ret_t rcl_executor_spin(
  rcl_executor_t * executor, size_t threads, rcl_executor_dispatch_fn dispatch, void * context)
{
  if (executor->spinning.exchange(true)) {
    RCL_SET_ERROR_MSG("executor is already spinning");
    return RCL_RET_ERROR;
  }
  auto result = boundary([&] {
        std::atomic<bool> failed{false};
        auto run = [&] {
          for (;; ) {
            auto status = rcl_executor_wait(executor, -1);
            if (status == RCL_EXECUTOR_STOPPED) {return;}
            rcl_executor_item_t item{};
            if (status != RCL_EXECUTOR_ERROR) {status = rcl_executor_next(executor, &item);}
            if (status == RCL_EXECUTOR_ERROR) {
              failed = true;
              executor->control->ring(1);
              return;
            }
            if (status != RCL_EXECUTOR_READY) {continue;}
            if (item.function) {item.function(item.context);} else {dispatch(&item, context);}
            if (executor->complete(item.token, false) != RCL_RET_OK) {
              failed = true;
              executor->control->ring(1);
              return;
            }
          }
        };
        std::vector<std::thread> workers;
        try {
          for (size_t i = 1; i < std::max<size_t>(threads, 1); ++i) {
            workers.emplace_back(run);
          }
        } catch (...) {
          executor->control->ring(1);
          for (auto & worker : workers) {
            worker.join();
          }
          throw;
        }
        run();
        for (auto & worker : workers) {
          worker.join();
        }
        if (failed) {throw std::runtime_error("executor worker failed");}
    });
  executor->spinning = false;
  return result;
}
}  // extern "C"
