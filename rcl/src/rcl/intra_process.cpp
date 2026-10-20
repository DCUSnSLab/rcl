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

#include <dlfcn.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <utility>
#include "rcl/error_handling.h"
#include "rmw/intra_process.h"
#include "rosidl_runtime_c/string_functions.h"
#include "rosidl_runtime_c/u16string_functions.h"
#include "rosidl_typesupport_introspection_c/message_introspection.h"
#include "rosidl_typesupport_introspection_cpp/message_introspection.hpp"
#include "./intra_process_internal.h"
#include "./context_impl.h"
#include "./publisher_impl.h"
#include "./subscription_impl.h"

namespace
{
using CM = rosidl_typesupport_introspection_c__MessageMembers;
using XM = rosidl_typesupport_introspection_cpp::MessageMembers;
using CF = rosidl_typesupport_introspection_c__MessageMember;
using XF = rosidl_typesupport_introspection_cpp::MessageMember;
struct Schema
{
  bool cpp;
  const void * members;
  std::shared_ptr<void> library;
  size_t size() const
  {
    return cpp ? static_cast<const XM *>(members)->size_of_ :
           static_cast<const CM *>(members)->size_of_;
  }
  size_t count() const
  {
    return cpp ? static_cast<const XM *>(members)->member_count_ :
           static_cast<const CM *>(members)->member_count_;
  }
  void init(void * data) const
  {
    if (cpp) {
      static_cast<const XM *>(members)->init_function(data,
          rosidl_runtime_cpp::MessageInitialization::ALL);
    } else {static_cast<const CM *>(members)->init_function(data, ROSIDL_RUNTIME_C_MSG_INIT_ALL);}
  }
  void fini(void * data) const
  {
    if (cpp) {static_cast<const XM *>(members)->fini_function(data);} else {
      static_cast<const CM *>(members)->fini_function(data);
    }
  }
};
struct Field
{
  const CF * c = nullptr;
  const XF * x = nullptr;
  uint8_t type() const {return x ? x->type_id_ : c->type_id_;}
  bool array() const {return x ? x->is_array_ : c->is_array_;}
  size_t offset() const {return x ? x->offset_ : c->offset_;}
  size_t fixed() const {return x ? x->array_size_ : c->array_size_;}
  bool bounded() const {return x ? x->is_upper_bound_ : c->is_upper_bound_;}
  size_t size(const void * data) const
  {
    if (fixed() && !bounded()) {return fixed();}
    return x ? x->size_function(data) : c->size_function(data);
  }
  void resize(void * data, size_t n) const
  {
    if (fixed() && !bounded()) {return;}
    if (x) {x->resize_function(data, n);} else if (!c->resize_function(data, n)) {
      throw std::bad_alloc();
    }
  }
  auto get_const() const {return x ? x->get_const_function : c->get_const_function;}
  auto get() const {return x ? x->get_function : c->get_function;}
  auto fetch() const {return x ? x->fetch_function : c->fetch_function;}
  auto assign() const {return x ? x->assign_function : c->assign_function;}
  Schema nested(const Schema & parent) const
  {
    return Schema{parent.cpp, x ? x->members_->data : c->members_->data, parent.library};
  }
};
Field field(const Schema & s, size_t i)
{
  if (s.cpp) {
    auto members = static_cast<const XM *>(s.members);
    return Field{nullptr, &members->members_[i]};
  }
  auto members = static_cast<const CM *>(s.members);
  return Field{&members->members_[i], nullptr};
}
size_t width(uint8_t id)
{
  static const size_t sizes[] = {0, 4, 8, sizeof(long double), 1, 2, 1, 1, 1, 1, 2, 2, 4, 4, 8, 8};
  return sizes[id];
}
void copy(const Schema &, const void *, const Schema &, void *);
void scalar(
  const Schema & a, const Field & af, const void * src,
  const Schema & b, const Field & bf, void * dst)
{
  if (af.type() < 16) {std::memcpy(dst, src, width(af.type()));} else if (af.type() == 16) {
    const char * data = a.cpp ? static_cast<const std::string *>(src)->data() :
      static_cast<const rosidl_runtime_c__String *>(src)->data;
    size_t n = a.cpp ? static_cast<const std::string *>(src)->size() :
      static_cast<const rosidl_runtime_c__String *>(src)->size;
    if (b.cpp) {
      static_cast<std::string *>(dst)->assign(data, n);
    } else if (!rosidl_runtime_c__String__assignn(static_cast<rosidl_runtime_c__String *>(dst),
        data, n)) {throw std::bad_alloc();}
  } else if (af.type() == 17) {
    auto c_string = static_cast<const rosidl_runtime_c__U16String *>(src);
    const char16_t * data = a.cpp ? static_cast<const std::u16string *>(src)->data() :
      reinterpret_cast<const char16_t *>(c_string->data);
    size_t n = a.cpp ? static_cast<const std::u16string *>(src)->size() :
      static_cast<const rosidl_runtime_c__U16String *>(src)->size;
    if (b.cpp) {
      static_cast<std::u16string *>(dst)->assign(data, n);
    } else if (!rosidl_runtime_c__U16String__assignn(
        static_cast<rosidl_runtime_c__U16String *>(dst),
      reinterpret_cast<const uint16_t *>(data), n)) {throw std::bad_alloc();}
  } else {copy(af.nested(a), src, bf.nested(b), dst);}
}
void copy(const Schema & a, const void * src, const Schema & b, void * dst)
{
  for (size_t i = 0; i < a.count(); ++i) {
    auto af = field(a, i), bf = field(b, i);
    auto sp = static_cast<const char *>(src) + af.offset();
    auto dp = static_cast<char *>(dst) + bf.offset();
    if (!af.array()) {scalar(a, af, sp, b, bf, dp); continue;}
    size_t n = af.size(sp);
    bf.resize(dp, n);
    if (!n) {continue;}
    if (af.type() < 16 && af.type() != 6) {
      const void * from = af.get_const() ? af.get_const()(sp, 0) : sp;
      void * to = bf.get() ? bf.get()(dp, 0) : dp;
      std::memcpy(to, from, n * width(af.type()));
    } else {
      for (size_t j = 0; j < n; ++j) {
        if (af.type() == 6) {
          bool value;
          af.fetch()(sp, j, &value);
          bf.assign()(dp, j, &value);
        } else {scalar(a, af, af.get_const()(sp, j), b, bf, bf.get()(dp, j));}
      }
    }
  }
}
std::shared_ptr<Schema> schema(const rosidl_message_type_support_t * support)
{
  bool cpp = std::strstr(support->typesupport_identifier, "cpp") != nullptr;
  auto id = cpp ? "rosidl_typesupport_introspection_cpp" : "rosidl_typesupport_introspection_c";
  auto introspection = support->func(support, id);
  if (!introspection) {rcl_reset_error(); return {};}
  Dl_info info{};
  if (!dladdr(introspection->data, &info)) {return {};}
  auto library = std::shared_ptr<void>(dlopen(info.dli_fname, RTLD_NOW), [](void * p) {
        dlclose(p);
      });
  return std::make_shared<Schema>(Schema{cpp, introspection->data, library});
}
std::shared_ptr<void> clone(const Schema & source, const void * data, const Schema & target)
{
  auto p = std::malloc(target.size());
  if (!p) {throw std::bad_alloc();}
  target.init(p);
  auto destroy = [target](void * value) {
      target.fini(value);
      std::free(value);
    };
  auto result = std::shared_ptr<void>(p, destroy);
  copy(source, data, target, p);
  return result;
}
using Gid = std::array<uint8_t, RMW_GID_STORAGE_SIZE>;
struct Sample
{
  std::shared_ptr<void> owner;
  rmw_message_info_t info;
};
struct Subscription
{
  std::mutex notify_mutex;
  std::mutex mutex;
  std::atomic<size_t> ready{0};
  bool closed = false;
  uint64_t reception = 0;
  std::string topic;
  rosidl_type_hash_t hash;
  rmw_qos_profile_t qos;
  std::shared_ptr<Schema> type;
  const rmw_subscription_t * rmw;
  const rmw_ipc_api_t * api;
  Gid gid;
  std::vector<Sample> queue;
  std::map<Gid, int64_t> admitted;
};
struct Broker
{
  std::mutex mutex;
  const rmw_ipc_api_t * api;
  std::shared_ptr<void> library;
  std::vector<std::weak_ptr<Subscription>> subscriptions;
};
struct Publisher
{
  Broker * broker;
  std::string topic;
  rosidl_type_hash_t hash;
  rmw_qos_profile_t qos;
  std::shared_ptr<Schema> type;
};
bool unlimited(const rmw_time_t & value)
{
  const rmw_time_t infinite = RMW_DURATION_INFINITE;
  return (value.sec == 0 && value.nsec == 0) ||
         (value.sec == infinite.sec && value.nsec == infinite.nsec);
}
bool supported(const rmw_qos_profile_t & qos)
{
  return qos.history == RMW_QOS_POLICY_HISTORY_KEEP_LAST && qos.depth > 0 &&
         qos.durability == RMW_QOS_POLICY_DURABILITY_VOLATILE &&
         (qos.reliability == RMW_QOS_POLICY_RELIABILITY_RELIABLE ||
         qos.reliability == RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT) &&
         unlimited(qos.deadline) && unlimited(qos.lifespan) &&
         unlimited(qos.liveliness_lease_duration) &&
         qos.liveliness != RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC;
}
struct Prepared
{
  Publisher * pub;
  std::vector<Gid> excluded;
  std::vector<std::shared_ptr<Subscription>> notify;
  std::vector<Sample> retired;
};
thread_local std::shared_ptr<void> publishing_owner;
rmw_ret_t prepare(
  void * user, const void * message, const rmw_message_info_t * info,
  const uint8_t * matched, size_t count, rmw_ipc_route_t * route)
{
  auto & state = *static_cast<Prepared *>(user);
  auto pub = state.pub;
  std::lock_guard<std::mutex> lock(pub->broker->mutex);
  try {
    std::map<bool, std::shared_ptr<void>> converted;
    std::vector<std::pair<std::shared_ptr<Subscription>, Sample>> pending;
    for (auto & weak : pub->broker->subscriptions) {
      auto sub = weak.lock();
      if (!sub || sub->topic != pub->topic || sub->hash.version != pub->hash.version ||
        std::memcmp(sub->hash.value, pub->hash.value, sizeof(sub->hash.value)) != 0) {continue;}
      bool known = false;
      for (size_t i = 0; i < count; ++i) {
        known = known || std::memcmp(sub->gid.data(), matched + i * sub->gid.size(),
            sub->gid.size()) == 0;
      }
      Gid writer;
      std::copy_n(info->publisher_gid.data, writer.size(), writer.begin());
      {
        std::lock_guard<std::mutex> sub_lock(sub->mutex);
        auto last = sub->admitted.try_emplace(writer, 0).first;
        int64_t sequence = last->second;
        if (static_cast<int64_t>(info->publication_sequence_number) > sequence + 1) {continue;}
      }
      if (!known) {continue;}
      auto & owner = converted[sub->type->cpp];
      if (!owner) {
        owner = publishing_owner.get() == message &&
          sub->type->cpp == pub->type->cpp ? publishing_owner :
          clone(*pub->type, message, *sub->type);
      }
      auto metadata = *info;
      metadata.from_intra_process = true;
      pending.emplace_back(sub, Sample{owner, metadata});
    }
    state.excluded.reserve(pending.size());
    state.notify.reserve(pending.size());
    state.retired.reserve(pending.size());
    for (auto & entry : pending) {
      auto sub = entry.first;
      std::lock_guard<std::mutex> sub_lock(sub->mutex);
      if (sub->closed) {continue;}
      bool grew = sub->queue.size() < sub->qos.depth;
      if (!grew) {
        state.retired.push_back(std::move(sub->queue.front()));
        sub->queue.erase(sub->queue.begin());
      }
      Gid writer;
      std::copy_n(info->publisher_gid.data, writer.size(), writer.begin());
      sub->admitted[writer] = info->publication_sequence_number;
      entry.second.info.received_timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
      entry.second.info.reception_sequence_number = sub->reception++;
      sub->queue.push_back(std::move(entry.second));
      sub->ready.store(sub->queue.size(), std::memory_order_release);
      state.excluded.push_back(sub->gid);
      if (grew) {state.notify.push_back(sub);}
    }
    route->excluded_gids = state.excluded.empty() ? nullptr : state.excluded.front().data();
    route->excluded_count = state.excluded.size();
    return RMW_RET_OK;
  } catch (const std::bad_alloc &) {
    RCL_SET_ERROR_MSG("allocating intra-process delivery failed");
    return RMW_RET_BAD_ALLOC;
  }
}
}  // namespace
struct rcl_ipc_message_s {Sample sample;};

void rcl_ipc_context_init(rcl_context_t * context)
{
  try {
    const char * enabled = std::getenv("RCL_INTRA_PROCESS");
    if (!enabled || std::strcmp(enabled, "1") != 0) {return;}
    auto name = std::string("lib") + rmw_get_implementation_identifier() + ".so";
    void * library = dlopen(name.c_str(), RTLD_NOW | RTLD_NOLOAD);
    if (!library) {return;}
    auto handle = std::shared_ptr<void>(library, [](void * value) {dlclose(value);});
    auto getter = reinterpret_cast<rmw_get_ipc_api_fn>(dlsym(library,
      "rmw_get_intra_process_api_v1"));
    if (!getter) {return;}
    auto api = getter();
    if (!api || api->version != 1 || api->size < sizeof(rmw_ipc_api_t)) {return;}
    auto broker = new Broker;
    broker->api = api;
    broker->library = handle;
    context->impl->ipc = broker;
  } catch (const std::bad_alloc &) {
    return;
  }
}
void rcl_ipc_context_fini(rcl_context_t * context)
{
  delete static_cast<Broker *>(context->impl->ipc);
  context->impl->ipc = nullptr;
}
void rcl_ipc_publisher_init(
  rcl_publisher_t * publisher,
  const rosidl_message_type_support_t * support)
{
  try {
    auto broker = static_cast<Broker *>(publisher->impl->context->impl->ipc);
    if (!broker || publisher->impl->type_hash.version == 0 ||
      !supported(publisher->impl->actual_qos)) {return;}
    auto type = schema(support);
    if (!type) {return;}
    publisher->impl->ipc = new Publisher{broker, rcl_publisher_get_topic_name(publisher),
      publisher->impl->type_hash, publisher->impl->actual_qos, type};
  } catch (const std::bad_alloc &) {
    return;
  }
}
void rcl_ipc_publisher_fini(rcl_publisher_t * publisher)
{
  delete static_cast<Publisher *>(publisher->impl->ipc);
  publisher->impl->ipc = nullptr;
}
void rcl_ipc_subscription_init(
  rcl_subscription_t * subscription,
  const rosidl_message_type_support_t * support)
{
  try {
    auto broker = static_cast<Broker *>(subscription->impl->context->impl->ipc);
    if (!broker || subscription->impl->type_hash.version == 0 ||
      subscription->impl->options.rmw_subscription_options.ignore_local_publications ||
      !supported(subscription->impl->actual_qos) ||
      subscription->impl->options.rmw_subscription_options.content_filter_options) {return;}
    auto type = schema(support);
    if (!type) {return;}
    auto sub = std::make_shared<Subscription>();
    sub->topic = rcl_subscription_get_topic_name(subscription);
    sub->hash = subscription->impl->type_hash;
    sub->qos = subscription->impl->actual_qos;
    sub->queue.reserve(sub->qos.depth);
    sub->type = type;
    sub->rmw = subscription->impl->rmw_handle;
    sub->api = broker->api;
    std::lock_guard<std::mutex> lock(broker->mutex);
    broker->subscriptions.erase(std::remove_if(broker->subscriptions.begin(),
    broker->subscriptions.end(), [](const auto & entry) {
        return entry.expired();
      }), broker->subscriptions.end());
    broker->subscriptions.reserve(broker->subscriptions.size() + 1);
    auto holder = std::make_unique<std::shared_ptr<Subscription>>(sub);
    broker->api->configure_subscription(sub->rmw, [](void * value) {
        return static_cast<Subscription *>(value)->ready.load(std::memory_order_acquire) > 0;
    }, [](void * value, void * input, rmw_ipc_decode_fn decode,
      const rmw_message_info_t * info) -> rmw_ipc_ingress_result_t {
        auto sub = static_cast<Subscription *>(value);
        try {
          auto data = std::malloc(sub->type->size());
          if (!data) {return {0, nullptr, nullptr};}
          sub->type->init(data);
          auto owner = std::shared_ptr<void>(data, [type = sub->type](void * p) {
            type->fini(p); std::free(p);
          });
          if (!decode(input, data)) {return {0, nullptr, nullptr};}
          auto retired = std::make_unique<Sample>();
          std::lock_guard<std::mutex> lock(sub->mutex);
          if (sub->closed) {return {0, nullptr, nullptr};}
          Gid writer;
          std::copy_n(info->publisher_gid.data, writer.size(), writer.begin());
          sub->admitted[writer] = info->publication_sequence_number;
          bool grew = sub->queue.size() < sub->qos.depth;
          if (!grew) {
            *retired = std::move(sub->queue.front());
            sub->queue.erase(sub->queue.begin());
          }
          auto metadata = *info;
          metadata.reception_sequence_number = sub->reception++;
          sub->queue.push_back(Sample{owner, metadata});
          sub->ready.store(sub->queue.size(), std::memory_order_release);
          return {grew ? 2 : 1, retired.release(), [](void * p) {delete static_cast<Sample *>(p);}};
        } catch (const std::bad_alloc &) {
          return {0, nullptr, nullptr};
        }
    }, sub.get(), sub->gid.data());
    broker->subscriptions.push_back(sub);
    subscription->impl->ipc = holder.release();
  } catch (const std::bad_alloc &) {
    return;
  }
}
void rcl_subscription_disable_ipc(rcl_subscription_t * subscription)
{
  if (!subscription->impl->ipc) {return;}
  auto holder = static_cast<std::shared_ptr<Subscription> *>(subscription->impl->ipc);
  auto sub = *holder;
  std::vector<Sample> retired;
  {
    std::lock_guard<std::mutex> notify_lock(sub->notify_mutex);
    sub->api->configure_subscription(sub->rmw, nullptr, nullptr, nullptr, nullptr);
    std::lock_guard<std::mutex> lock(sub->mutex);
    sub->closed = true;
    retired.swap(sub->queue);
    sub->ready.store(0, std::memory_order_release);
  }
  delete holder;
  subscription->impl->ipc = nullptr;
}
bool rcl_publisher_is_ipc_enabled(const rcl_publisher_t * publisher)
{return publisher && publisher->impl && publisher->impl->ipc;}
bool rcl_subscription_is_ipc_enabled(const rcl_subscription_t * subscription)
{return subscription && subscription->impl && subscription->impl->ipc;}
rcl_ret_t rcl_ipc_publish(const rcl_publisher_t * publisher, const void * message)
{
  Prepared state{static_cast<Publisher *>(publisher->impl->ipc), {}, {}, {}};
  rmw_ret_t ret;
  try {
    ret = state.pub->broker->api->publish(publisher->impl->rmw_handle, message, prepare, &state);
  } catch (const std::bad_alloc &) {
    RCL_SET_ERROR_MSG("allocating transport publication failed");
    ret = RMW_RET_BAD_ALLOC;
  }
  for (auto & sub : state.notify) {
    std::lock_guard<std::mutex> lock(sub->notify_mutex);
    if (!sub->closed && sub->ready.load()) {sub->api->notify(sub->rmw);}
  }
  return ret;
}
rcl_ret_t rcl_take_ipc_message(
  const rcl_subscription_t * subscription,
  rcl_ipc_message_t ** message, const void ** data, rmw_message_info_t * info)
{
  if (!message || !data) {
    RCL_SET_ERROR_MSG("message and data outputs are required");
    return RCL_RET_INVALID_ARGUMENT;
  }
  try {
    if (!rcl_subscription_is_ipc_enabled(subscription)) {return RCL_RET_SUBSCRIPTION_TAKE_FAILED;}
    auto sub = *static_cast<std::shared_ptr<Subscription> *>(subscription->impl->ipc);
    std::lock_guard<std::mutex> lock(sub->mutex);
    if (sub->queue.empty()) {return RCL_RET_SUBSCRIPTION_TAKE_FAILED;}
    auto result = std::make_unique<rcl_ipc_message_t>();
    result->sample = std::move(sub->queue.front());
    sub->queue.erase(sub->queue.begin());
    sub->ready.store(sub->queue.size(), std::memory_order_release);
    *data = result->sample.owner.get();
    if (info) {*info = result->sample.info;}
    *message = result.release();
    return RCL_RET_OK;
  } catch (const std::bad_alloc &) {
    RCL_SET_ERROR_MSG("allocating retained message failed");
    return RCL_RET_BAD_ALLOC;
  }
}
void rcl_ipc_message_release(rcl_ipc_message_t * message) {delete message;}
rcl_ret_t rcl_ipc_take_copy(
  const rcl_subscription_t * subscription, void * data,
  rmw_message_info_t * info)
{
  rcl_ipc_message_t * message;
  const void * source;
  auto ret = rcl_take_ipc_message(subscription, &message, &source, info);
  if (ret != RCL_RET_OK) {return ret;}
  std::unique_ptr<rcl_ipc_message_t> owner(message);
  auto sub = *static_cast<std::shared_ptr<Subscription> *>(subscription->impl->ipc);
  try {
    copy(*sub->type, source, *sub->type, data); return RCL_RET_OK;
  } catch (const std::bad_alloc &) {
    return RCL_RET_BAD_ALLOC;
  }
}

void rcl_publisher_disable_ipc(rcl_publisher_t * publisher) {rcl_ipc_publisher_fini(publisher);}
rcl_ret_t rcl_publish_ipc_owned(
  const rcl_publisher_t * publisher,
  const void * data, void (*release)(void *), void * value)
{
  std::unique_ptr<void, decltype(release)> consumed(value, release);
  try {
    Dl_info info{};
    std::shared_ptr<void> module;
    if (dladdr(reinterpret_cast<void *>(release), &info)) {
      if (auto handle = dlopen(info.dli_fname, RTLD_NOW | RTLD_NOLOAD)) {
        module = std::shared_ptr<void>(handle, [](void * p) {dlclose(p);});
      }
    }
    consumed.release();
    auto owner = std::shared_ptr<void>(const_cast<void *>(data),
        [release, value, module](void *) {release(value);});
    struct Scope
    {
      std::shared_ptr<void> previous;
      ~Scope() {publishing_owner = std::move(previous);}
    } scope{std::exchange(publishing_owner, std::move(owner))};
    return rcl_publish(publisher, data, nullptr);
  } catch (const std::bad_alloc &) {
    RCL_SET_ERROR_MSG("allocating publication owner failed");
    return RCL_RET_BAD_ALLOC;
  }
}
