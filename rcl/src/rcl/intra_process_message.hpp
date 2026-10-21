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

#ifndef RCL__INTRA_PROCESS_MESSAGE_HPP_
#define RCL__INTRA_PROCESS_MESSAGE_HPP_

#include <memory>
#include "rosidl_runtime_c/service_type_support_struct.h"

namespace rcl_ipc
{
struct Type;
std::shared_ptr<Type> service_type(const rosidl_service_type_support_t * support, bool response);
std::shared_ptr<void> clone(const Type & source, const void * data, const Type & target);
// NOLINTNEXTLINE(build/include_what_you_use)
void copy(
  const Type & source, const void * data, const Type & target, void * output);
}  // namespace rcl_ipc

#endif  // RCL__INTRA_PROCESS_MESSAGE_HPP_
