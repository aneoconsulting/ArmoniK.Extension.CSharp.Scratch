// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Common.Common.Domain.Task;

namespace ArmoniK.Extension.CSharp.Client.Handles;

/// <summary>
///   Handles operations related to tasks using the ArmoniK client.
/// </summary>
public class TaskHandle
{
  /// <summary>
  ///   Gets the ArmoniK client used to interact with task services.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   Gets the task information for which this handle will perform operations.
  /// </summary>
  private readonly TaskInfos taskInfos_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskHandle" /> class with a specified ArmoniK client and task
  ///   information.
  /// </summary>
  /// <param name="armoniKClient">The ArmoniK client to be used for task service operations.</param>
  /// <param name="taskInfo">The task information related to the tasks that will be handled.</param>
  /// <exception cref="ArgumentNullException">Thrown when armoniKClient or taskInfo is null.</exception>
  public TaskHandle(ArmoniKClient armoniKClient,
                    TaskInfos     taskInfo)
  {
    ArmoniKClient = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
    taskInfos_    = taskInfo      ?? throw new ArgumentNullException(nameof(taskInfo));
  }

  /// <summary>
  ///   Implicit conversion operator from TaskHandle to TaskInfos.
  ///   Allows TaskHandle to be used wherever TaskInfos is expected.
  /// </summary>
  /// <param name="taskHandle">The TaskHandle to convert.</param>
  /// <returns>The TaskInfos contained within the TaskHandle.</returns>
  /// <exception cref="ArgumentNullException">Thrown when taskHandle is null.</exception>
  public static implicit operator TaskInfos(TaskHandle taskHandle)
    => taskHandle?.taskInfos_ ?? throw new ArgumentNullException(nameof(taskHandle));

  /// <summary>
  ///   Creates a TaskHandle from TaskInfos and ArmoniKClient.
  /// </summary>
  /// <param name="taskInfos">The TaskInfos to wrap.</param>
  /// <param name="armoniKClient">The ArmoniK client for operations.</param>
  /// <returns>A new TaskHandle instance.</returns>
  /// <exception cref="ArgumentNullException">Thrown when taskInfos or armoniKClient is null.</exception>
  public static TaskHandle FromTaskInfos(TaskInfos     taskInfos,
                                         ArmoniKClient armoniKClient)
    => new(armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient)),
           taskInfos     ?? throw new ArgumentNullException(nameof(taskInfos)));

  /// <summary>
  ///   Asynchronously retrieves detailed state information about the task associated with this handle.
  /// </summary>
  /// <param name="cancellationToken">A token that can be used to request cancellation of the asynchronous operation.</param>
  /// <returns>
  ///   A <see cref="Task{TaskState}" /> representing the asynchronous operation, with the task's detailed state as
  ///   the result.
  /// </returns>
  public async Task<TaskState> GetTaskDetailsAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.TasksService.GetTasksDetailedAsync(taskInfos_.TaskId,
                                                              cancellationToken)
                          .ConfigureAwait(false);
}
