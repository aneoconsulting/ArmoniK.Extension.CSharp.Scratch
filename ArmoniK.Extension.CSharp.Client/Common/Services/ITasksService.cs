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

using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Enum;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for managing tasks, including submission, retrieval, and cancellation of tasks.
/// </summary>
public interface ITasksService
{
  /// <summary>
  ///   Asynchronously submits a collection of tasks for a given session.
  /// </summary>
  /// <param name="session">The session information to which the tasks belong.</param>
  /// <param name="taskNodes">The tasks to be submitted.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains an enumerable of task information.</returns>
  Task<IEnumerable<TaskInfos>> SubmitTasksAsync(SessionInfo           session,
                                                IEnumerable<TaskNode> taskNodes,
                                                CancellationToken     cancellationToken = default);

  /// <summary>
  ///   Asynchronously lists tasks based on pagination options.
  /// </summary>
  /// <param name="paginationOptions">The options for pagination, including page number, page size, and sorting.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of task pages.</returns>
  IAsyncEnumerable<TaskPage> ListTasksAsync(TaskPagination    paginationOptions,
                                            CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously retrieves detailed information about a specific task.
  /// </summary>
  /// <param name="taskId">The identifier of the task to retrieve details for.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the detailed task state.</returns>
  Task<TaskState> GetTasksDetailedAsync(string            taskId,
                                        CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously lists detailed task information based on session and pagination options.
  /// </summary>
  /// <param name="session">The session information to which the tasks belong.</param>
  /// <param name="paginationOptions">The options for pagination, including page number, page size, and sorting.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of detailed task pages.</returns>
  IAsyncEnumerable<TaskDetailedPage> ListTasksDetailedAsync(SessionInfo       session,
                                                            TaskPagination    paginationOptions,
                                                            CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously cancels a collection of tasks based on their identifiers.
  /// </summary>
  /// <param name="taskIds">The identifiers of the tasks to cancel.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task CancelTask(IEnumerable<string> taskIds,
                  CancellationToken   cancellationToken = default);
}

/// <summary>
///   Provides extension methods for the <see cref="ITasksService" /> interface.
/// </summary>
public static class TasksServiceExt
{
  /// <summary>
  ///   Asynchronously retrieves tasks based on their identifiers, with support for pagination.
  /// </summary>
  /// <param name="taskService">The task service instance.</param>
  /// <param name="taskIds">The identifiers of the tasks to retrieve.</param>
  /// <param name="pageSize">The number of tasks to retrieve per page.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of task pages.</returns>
  public static async IAsyncEnumerable<TaskPage> GetTasksAsync(this ITasksService                         taskService,
                                                               IEnumerable<string>                        taskIds,
                                                               int                                        pageSize          = 50,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var taskPagination = new TaskPagination
                         {
                           Filter = new Filters
                                    {
                                      Or =
                                      {
                                        taskIds.Select(id => TaskIdFilter(id)),
                                      },
                                    },
                           Page          = 0,
                           PageSize      = pageSize,
                           SortDirection = SortDirection.Asc,
                         };

    var                        total     = 0;
    var                        firstPage = true;
    IAsyncEnumerable<TaskPage> res;
    while (await (res = taskService.ListTasksAsync(taskPagination,
                                                   cancellationToken)).AnyAsync(cancellationToken)
                                                                      .ConfigureAwait(false))
    {
      await foreach (var taskPage in res.WithCancellation(cancellationToken)
                                        .ConfigureAwait(false))
      {
        if (firstPage)
        {
          total     = taskPage.TotalTasks;
          firstPage = false;
        }

        yield return taskPage;
      }

      taskPagination.Page++;
    }
  }

  /// <summary>
  ///   Creates a filter for a task based on its identifier.
  /// </summary>
  /// <param name="taskId">The identifier of the task to filter.</param>
  /// <returns>A filter that matches the specified task identifier.</returns>
  private static FiltersAnd TaskIdFilter(string taskId)
    => new()
       {
         And =
         {
           new FilterField
           {
             Field = new TaskField
                     {
                       TaskSummaryField = new TaskSummaryField
                                          {
                                            Field = TaskSummaryEnumField.TaskId,
                                          },
                     },
             FilterString = new FilterString
                            {
                              Value    = taskId,
                              Operator = FilterStringOperator.Equal,
                            },
           },
         },
       };
}
