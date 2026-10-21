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

#ifndef RCL__INTRA_PROCESS_SERVICE_H_
#define RCL__INTRA_PROCESS_SERVICE_H_
#include "rcl/client.h"
#include "rcl/intra_process.h"
#include "rcl/service.h"
#ifdef __cplusplus
extern "C" {
#endif
void rcl_ipc_services_init(rcl_context_t *);
void rcl_ipc_services_fini(rcl_context_t *);
void * rcl_ipc_client_init(
  rcl_client_t *, const rcl_node_t *,
  const rosidl_service_type_support_t *);
void * rcl_ipc_service_init(
  rcl_service_t *, const rcl_node_t *,
  const rosidl_service_type_support_t *);
void rcl_ipc_client_fini(void *);
void rcl_ipc_service_fini(void *);
rcl_ret_t rcl_ipc_send_request(void *, const void *, int64_t *);
rcl_ret_t rcl_ipc_take_request(void *, rmw_service_info_t *, void *, bool *);
rcl_ret_t rcl_ipc_take_response(void *, rmw_service_info_t *, void *, bool *);
rcl_ret_t rcl_ipc_send_response(void *, rmw_request_id_t *, void *);
rcl_ret_t rcl_ipc_take_owned_request(void *, rmw_service_info_t *, rcl_ipc_message_t **, void **);
rcl_ret_t rcl_ipc_take_owned_response(void *, rmw_service_info_t *, rcl_ipc_message_t **, void **);
rcl_ret_t rcl_ipc_borrow_response(void *, const rmw_request_id_t *, rcl_ipc_message_t **, void **);
rcl_ret_t rcl_ipc_send_owned_response(void *, rmw_request_id_t *, rcl_ipc_message_t *);
void * rcl_ipc_message_data(rcl_ipc_message_t *);
#ifdef __cplusplus
}
#endif
#endif  // RCL__INTRA_PROCESS_SERVICE_H_
