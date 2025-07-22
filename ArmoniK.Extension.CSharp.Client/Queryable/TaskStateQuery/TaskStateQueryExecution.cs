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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;

namespace ArmoniK.Extension.CSharp.Client.Queryable.TaskStateQuery;

internal class TaskStateQueryExecution : QueryExecution<TaskPagination, TaskDetailedPage, TaskState, TaskField, Filters, FiltersAnd, FilterField>
{
  private readonly ILogger<ITasksService> logger_;
  private readonly ITasksService          tasksService_;

  public TaskStateQueryExecution(ITasksService          service,
                                 ILogger<ITasksService> logger)
  {
    tasksService_ = service;
    logger_       = logger;
  }

  protected override void LogError(Exception ex,
                                   string    message)
    => logger_.LogError(ex,
                        message);

  protected override async Task<TaskDetailedPage> RequestInstances(TaskPagination    pagination,
                                                                   CancellationToken cancellationToken)
  {
    pagination.Page++;
    return await tasksService_.ListTasksDetailedAsync(pagination,
                                                      cancellationToken)
                              .ConfigureAwait(false);
  }

  protected override QueryExpressionTreeVisitor<TaskState, TaskField, Filters, FiltersAnd, FilterField> CreateQueryExpressionTreeVisitor()
    => new TaskStateQueryExpressionTreeVisitor();

  protected override TaskPagination CreatePaginationInstance(Filters   filter,
                                                             TaskField sortCriteria,
                                                             bool      isAscending)
    => new()
       {
         Filter   = filter,
         Page     = -1,
         PageSize = 50,
         SortDirection = isAscending
                           ? SortDirection.Asc
                           : SortDirection.Desc,
         SortField = sortCriteria,
       };

  protected override int GetTotalPageElements(TaskDetailedPage page)
    => page.TotalTasks;

  protected override IEnumerable<TaskState> GetPageElements(TaskDetailedPage page)
    => page.TaskDetails;
}
