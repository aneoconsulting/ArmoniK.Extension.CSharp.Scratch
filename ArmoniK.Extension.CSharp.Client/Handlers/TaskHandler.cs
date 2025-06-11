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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

namespace ArmoniK.Extension.CSharp.Client.Handlers;

/// <summary>
///   Handles operations related to tasks using the ArmoniK client.
/// </summary>
public class TaskHandler
{
  /// <summary>
  ///   Gets the ArmoniK client used to interact with task services.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   Gets the task information for which this handler will perform operations.
  /// </summary>
  private readonly TaskInfos taskInfos_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskHandler" /> class with a specified ArmoniK client and task
  ///   information.
  /// </summary>
  /// <param name="armoniKClient">The ArmoniK client to be used for task service operations.</param>
  /// <param name="taskInfo">The task information related to the tasks that will be handled.</param>
  public TaskHandler(ArmoniKClient armoniKClient,
                     TaskInfos     taskInfo)
  {
    ArmoniKClient = armoniKClient;
    taskInfos_    = taskInfo;
  }

  /// <summary>
  ///   Asynchronously retrieves detailed state information about the task associated with this handler.
  /// </summary>
  /// <param name="cancellationToken">A token that can be used to request cancellation of the asynchronous operation.</param>
  /// <returns>
  ///   A <see cref="Task{TaskState}" /> representing the asynchronous operation, with the task's detailed state as
  ///   the result.
  /// </returns>
  public async Task<TaskState> GetTaskDetailsAsync(CancellationToken cancellationToken)
  {
    var taskClient = await ArmoniKClient.GetTasksServiceAsync();
    return await taskClient.GetTasksDetailedAsync(taskInfos_.TaskId,
                                                  cancellationToken)
                           .ConfigureAwait(false);
  }
}
