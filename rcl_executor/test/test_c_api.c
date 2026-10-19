// Copyright 2026 nev
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#include "rcl_executor/executor.h"

struct state
{
  rcl_executor_handle_t * handle;
  int count;
};

static void work(void * context)
{
  struct state * state = context;
  ++state->count;
  rcl_executor_stop(state->handle);
}

int main(void)
{
  rcl_executor_t * executor = NULL;
  if (rcl_executor_create(&executor) != RCL_RET_OK) {return 1;}
  struct state state = {rcl_executor_get_handle(executor), 0};
  rcl_executor_id_t id;
  if (rcl_executor_add_group(executor, 1, RCL_EXECUTOR_MUTUALLY_EXCLUSIVE) != RCL_RET_OK) {
    return 5;
  }
  if (rcl_executor_add(executor, RCL_EXECUTOR_WORK, NULL, &state, 1, &id) != RCL_RET_OK) {
    return 6;
  }
  if (rcl_executor_notify(executor, id, 1) != RCL_RET_OK) {return 7;}
  rcl_executor_item_t item;
  if (rcl_executor_next(executor, &item) != RCL_EXECUTOR_READY) {return 8;}
  if (item.context != &state || item.count != 1) {return 9;}
  if (rcl_executor_complete(executor, item.token) != RCL_RET_OK) {return 10;}
  if (rcl_executor_remove(executor, id) != RCL_RET_OK) {return 11;}
  if (rcl_executor_post(executor, work, &state, 0) != RCL_RET_OK) {return 2;}
  if (rcl_executor_spin(executor, 1, NULL, NULL) != RCL_RET_OK) {return 3;}
  rcl_executor_destroy(executor);
  rcl_executor_handle_destroy(state.handle);
  return state.count == 1 ? 0 : 4;
}
