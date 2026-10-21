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
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "rmw/intra_process_service.h"
#include "rmw/error_handling.h"
#include "rcl/error_handling.h"
#include "./context_impl.h"
#include "rcl/intra_process_service.h"
#include "./intra_process_message.hpp"

namespace
{
using Gid = std::array<uint8_t, RMW_GID_STORAGE_SIZE>;
using Key = std::pair<Gid, int64_t>;
Key key(const rmw_request_id_t & id)
{
  Gid gid;
  std::copy_n(id.writer_guid, gid.size(), gid.begin());
  return {gid, id.sequence_number};
}
int64_t now()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}
struct Client;
struct Sample
{
  std::shared_ptr<void> data;
  rmw_service_info_t info;
  std::weak_ptr<Client> client;
};
struct Endpoint
{
  std::mutex mutex;
  std::mutex notify_mutex;
  std::atomic<size_t> ready{0};
  bool closed = false;
  size_t depth;
  rosidl_type_hash_t hash;
  std::shared_ptr<rcl_ipc::Type> request;
  std::shared_ptr<rcl_ipc::Type> response;
  std::deque<Sample> queue;
  const rmw_ipc_service_api_t * api;
};
struct Route
{
  std::weak_ptr<Client> client;
  bool rmw;
};
struct Service : Endpoint
{
  const rmw_service_t * rmw;
  Gid gid;
  std::map<Key, Route> pending;
};
struct Broker
{
  std::mutex mutex;
  std::vector<std::weak_ptr<Service>> services;
  const rmw_ipc_service_api_t * api;
  std::shared_ptr<void> library;
};
struct Client : Endpoint
{
  const rmw_client_t * rmw;
  Broker * broker;
};
bool ready(void * value)
{
  return static_cast<Endpoint *>(value)->ready.load(std::memory_order_acquire) != 0;
}
bool unlimited(const rmw_time_t & value)
{
  const rmw_time_t infinite = RMW_DURATION_INFINITE;
  return (value.sec == 0 && value.nsec == 0) ||
         (value.sec == infinite.sec && value.nsec == infinite.nsec);
}
bool supported(const rmw_qos_profile_t * qos)
{
  return qos->history == RMW_QOS_POLICY_HISTORY_KEEP_LAST && qos->depth > 0 &&
         qos->durability == RMW_QOS_POLICY_DURABILITY_VOLATILE &&
         (qos->reliability == RMW_QOS_POLICY_RELIABILITY_RELIABLE ||
         qos->reliability == RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT) &&
         unlimited(qos->deadline) && unlimited(qos->lifespan) &&
         unlimited(qos->liveliness_lease_duration) &&
         qos->liveliness != RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC;
}
template<typename T>
std::shared_ptr<T> endpoint(
  const rcl_node_t * node, const rosidl_service_type_support_t * support,
  const rmw_qos_profile_t * qos)
{
  auto broker = static_cast<Broker *>(node->context->impl->ipc_services);
  if (!broker || !supported(qos)) {return {};}
  auto request = rcl_ipc::service_type(support, false);
  auto response = rcl_ipc::service_type(support, true);
  if (!request || !response) {return {};}
  auto value = std::make_shared<T>();
  auto hash = support->get_type_hash_func(support);
  if (!hash || !hash->version) {return {};}
  value->hash = *hash;
  value->request = request;
  value->response = response;
  value->depth = qos->depth;
  value->api = broker->api;
  return value;
}
struct Prepared
{
  std::shared_ptr<Client> client;
  std::shared_ptr<Service> notify;
};
rmw_ret_t prepare(
  void * user, const void * data, const rmw_service_info_t * info,
  const uint8_t * matched, size_t count, bool * delivered)
{
  if (count != 1) {return RMW_RET_OK;}
  auto & state = *static_cast<Prepared *>(user);
  auto client = state.client;
  try {
    std::lock_guard<std::mutex> lock(client->broker->mutex);
    for (const auto & weak : client->broker->services) {
      auto service = weak.lock();
      if (!service || std::memcmp(service->gid.data(), matched, service->gid.size())) {continue;}
      if (service->hash.version != client->hash.version ||
        std::memcmp(service->hash.value, client->hash.value, sizeof(client->hash.value)))
      {return RMW_RET_OK;}
      auto data_copy = rcl_ipc::clone(*client->request, data, *service->request);
      std::lock_guard<std::mutex> service_lock(service->mutex);
      if (service->closed) {return RMW_RET_OK;}
      service->queue.push_back(Sample{std::move(data_copy), *info, client});
      if (service->queue.size() > service->depth) {service->queue.pop_front();}
      service->ready.store(service->queue.size(), std::memory_order_release);
      state.notify = service;
      *delivered = true;
      return RMW_RET_OK;
    }
    return RMW_RET_OK;
  } catch (const std::bad_alloc &) {
    RMW_SET_ERROR_MSG("allocating intra-process request failed");
    return RMW_RET_BAD_ALLOC;
  }
}
}  // namespace

void rcl_ipc_services_init(rcl_context_t * context)
{
  if (!context->impl->ipc) {return;}
  try {
    auto name = std::string("lib") + rmw_get_implementation_identifier() + ".so";
    auto library = std::shared_ptr<void>(dlopen(name.c_str(), RTLD_NOW | RTLD_NOLOAD),
        [](void * p) {if (p) {dlclose(p);}});
    auto getter = reinterpret_cast<rmw_get_ipc_service_api_fn>(
      dlsym(library.get(), "rmw_get_intra_process_service_api_v1"));
    if (!getter) {return;}
    auto api = getter();
    if (api->version != 1 || api->size < sizeof(rmw_ipc_service_api_t)) {return;}
    auto broker = std::make_unique<Broker>();
    broker->api = api;
    broker->library = library;
    context->impl->ipc_services = broker.release();
  } catch (const std::bad_alloc &) {
    return;
  }
}
void rcl_ipc_services_fini(rcl_context_t * context)
{
  delete static_cast<Broker *>(context->impl->ipc_services);
  context->impl->ipc_services = nullptr;
}
void * rcl_ipc_client_init(
  rcl_client_t * client, const rcl_node_t * node, const rosidl_service_type_support_t * support)
{
  try {
    auto value = endpoint<Client>(node, support,
      rcl_client_response_subscription_get_actual_qos(client));
    if (!value || !supported(rcl_client_request_publisher_get_actual_qos(client))) {return nullptr;}
    value->rmw = rcl_client_get_rmw_handle(client);
    value->broker = static_cast<Broker *>(node->context->impl->ipc_services);
    auto holder = std::make_unique<std::shared_ptr<Client>>(value);
    value->api->configure_client(value->rmw, ready, value.get());
    return holder.release();
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}
void * rcl_ipc_service_init(
  rcl_service_t * service, const rcl_node_t * node, const rosidl_service_type_support_t * support)
{
  try {
    auto value = endpoint<Service>(node, support,
      rcl_service_request_subscription_get_actual_qos(service));
    if (!value || !supported(rcl_service_response_publisher_get_actual_qos(service))) {
      return nullptr;
    }
    value->rmw = rcl_service_get_rmw_handle(service);
    auto broker = static_cast<Broker *>(node->context->impl->ipc_services);
    auto holder = std::make_unique<std::shared_ptr<Service>>(value);
    std::lock_guard<std::mutex> lock(broker->mutex);
    broker->services.erase(std::remove_if(broker->services.begin(), broker->services.end(),
      [](const auto & weak) {return weak.expired();}), broker->services.end());
    broker->services.reserve(broker->services.size() + 1);
    value->api->configure_service(value->rmw, ready, value.get(), value->gid.data());
    broker->services.push_back(value);
    return holder.release();
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}
void rcl_ipc_client_fini(void * input)
{
  if (!input) {return;}
  auto holder = static_cast<std::shared_ptr<Client> *>(input);
  auto value = *holder;
  {
    std::lock_guard<std::mutex> notify_lock(value->notify_mutex);
    value->api->configure_client(value->rmw, nullptr, nullptr);
    std::lock_guard<std::mutex> lock(value->mutex);
    value->closed = true;
    value->queue.clear();
    value->ready.store(0, std::memory_order_release);
  }
  delete holder;
}
void rcl_ipc_service_fini(void * input)
{
  if (!input) {return;}
  auto holder = static_cast<std::shared_ptr<Service> *>(input);
  auto value = *holder;
  {
    std::lock_guard<std::mutex> notify_lock(value->notify_mutex);
    value->api->configure_service(value->rmw, nullptr, nullptr, nullptr);
    std::lock_guard<std::mutex> lock(value->mutex);
    value->closed = true;
    value->queue.clear();
    value->pending.clear();
    value->ready.store(0, std::memory_order_release);
  }
  delete holder;
}
rcl_ret_t rcl_ipc_send_request(void * input, const void * data, int64_t * sequence)
{
  Prepared state{*static_cast<std::shared_ptr<Client> *>(input), {}};
  auto ret = state.client->api->send_request(state.client->rmw, data, sequence, prepare, &state);
  if (state.notify) {
    std::lock_guard<std::mutex> lock(state.notify->notify_mutex);
    if (!state.notify->closed) {state.notify->api->notify_service(state.notify->rmw);}
  }
  return ret;
}
rcl_ret_t rcl_ipc_take_request(void * input, rmw_service_info_t * info, void * data, bool * taken)
{
  auto service = *static_cast<std::shared_ptr<Service> *>(input);
  try {
    std::lock_guard<std::mutex> lock(service->mutex);
    if (!service->queue.empty()) {
      auto & sample = service->queue.front();
      rcl_ipc::copy(*service->request, sample.data.get(), *service->request, data);
      service->pending.emplace(key(sample.info.request_id), Route{sample.client, false});
      *info = sample.info;
      service->queue.pop_front();
      service->ready.store(service->queue.size(), std::memory_order_release);
      *taken = true;
      return RCL_RET_OK;
    }
  } catch (const std::bad_alloc &) {
    RMW_SET_ERROR_MSG("allocating intra-process service message failed");
    return RCL_RET_BAD_ALLOC;
  }
  auto ret = rmw_take_request(service->rmw, info, data, taken);
  if (ret == RMW_RET_OK && *taken) {
    try {
      std::lock_guard<std::mutex> lock(service->mutex);
      service->pending.emplace(key(info->request_id), Route{{}, true});
    } catch (const std::bad_alloc &) {
      RMW_SET_ERROR_MSG("allocating response route failed");
      return RMW_RET_BAD_ALLOC;
    }
  }
  return ret;
}
rcl_ret_t rcl_ipc_take_response(void * input, rmw_service_info_t * info, void * data, bool * taken)
{
  auto client = *static_cast<std::shared_ptr<Client> *>(input);
  try {
    std::lock_guard<std::mutex> lock(client->mutex);
    if (!client->queue.empty()) {
      auto & sample = client->queue.front();
      rcl_ipc::copy(*client->response, sample.data.get(), *client->response, data);
      *info = sample.info;
      client->queue.pop_front();
      client->ready.store(client->queue.size(), std::memory_order_release);
      *taken = true;
      return RCL_RET_OK;
    }
  } catch (const std::bad_alloc &) {
    RMW_SET_ERROR_MSG("allocating intra-process service message failed");
    return RCL_RET_BAD_ALLOC;
  }
  return rmw_take_response(client->rmw, info, data, taken);
}
rcl_ret_t rcl_ipc_send_response(void * input, rmw_request_id_t * id, void * data)
{
  auto service = *static_cast<std::shared_ptr<Service> *>(input);
  std::shared_ptr<Client> client;
  try {
    std::lock_guard<std::mutex> lock(service->mutex);
    auto route = service->pending.find(key(*id));
    if (route == service->pending.end()) {
      RMW_SET_ERROR_MSG("request has no pending response route");
      return RMW_RET_ERROR;
    }
    if (route->second.rmw) {
      auto ret = rmw_send_response(service->rmw, id, data);
      if (ret == RMW_RET_OK) {service->pending.erase(route);}
      return ret;
    }
    client = route->second.client.lock();
    if (!client) {service->pending.erase(route); return RCL_RET_OK;}
    auto response = rcl_ipc::clone(*service->response, data, *client->response);
    {
      std::lock_guard<std::mutex> client_lock(client->mutex);
      if (!client->closed) {
        rmw_service_info_t info{};
        info.request_id = *id;
        info.source_timestamp = now();
        info.received_timestamp = info.source_timestamp;
        client->queue.push_back(Sample{std::move(response), info, {}});
        if (client->queue.size() > client->depth) {client->queue.pop_front();}
        client->ready.store(client->queue.size(), std::memory_order_release);
      }
    }
    service->pending.erase(route);
  } catch (const std::bad_alloc &) {
    RMW_SET_ERROR_MSG("allocating intra-process service message failed");
    return RCL_RET_BAD_ALLOC;
  }
  std::lock_guard<std::mutex> notify_lock(client->notify_mutex);
  if (!client->closed) {client->api->notify_client(client->rmw);}
  return RCL_RET_OK;
}
