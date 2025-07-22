// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License")
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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;

namespace ArmoniK.Extension.CSharp.Client.Queryable.TaskStateQuery;

internal class TaskStateQueryProvider : ArmoniKQueryProvider<ITasksService, TaskPagination, TaskDetailedPage, TaskState, TaskField, Filters, FiltersAnd, FilterField>
{
  private readonly ITasksService tasksService_;

  public TaskStateQueryProvider(ITasksService          service,
                                ILogger<ITasksService> logger)
    : base(logger)
    => tasksService_ = service;

  protected override QueryExecution<TaskPagination, TaskDetailedPage, TaskState, TaskField, Filters, FiltersAnd, FilterField> CreateQueryExecution()
    => new TaskStateQueryExecution(tasksService_,
                                   logger_);
}
