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

using ArmoniK.Api.gRPC.V1.Tasks;

using Google.Protobuf.Reflection;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

/// <summary>
///   Represents the state of a task at any given moment, extending the TaskInfos.
/// </summary>
public record TaskState : TaskInfos
{
  /// <summary>
  ///   Initializes a new instance of the TaskState class.
  /// </summary>
  public TaskState()
  {
  }

  /// <summary>
  ///   Initializes a new instance of the TaskState class with details about task timings and status.
  /// </summary>
  /// <param name="createAt">The creation time of the task.</param>
  /// <param name="endedAt">The end time of the task.</param>
  /// <param name="startedAt">The start time of the task.</param>
  /// <param name="status">The status of the task.</param>
  public TaskState(DateTime   createAt,
                   DateTime   endedAt,
                   DateTime   startedAt,
                   TaskStatus status)
  {
    CreateAt  = createAt;
    EndedAt   = endedAt;
    StartedAt = startedAt;
    Status    = status;
  }

  /// <summary>
  ///   Time when the task was created.
  /// </summary>
  public DateTime CreateAt { get; init; }

  /// <summary>
  ///   Time when the task ended.
  /// </summary>
  public DateTime? EndedAt { get; init; }

  /// <summary>
  ///   Time when the task started.
  /// </summary>
  public DateTime? StartedAt { get; init; }

  /// <summary>
  ///   Current status of the task.
  /// </summary>
  public TaskStatus Status { get; init; }
}

/// <summary>
///   Defines the various statuses a task can have through its lifecycle.
/// </summary>
public enum TaskStatus
{
  /// <summary>
  ///   The task is in an unknown state.
  /// </summary>
  [OriginalName("TASK_STATUS_UNSPECIFIED")]
  Unspecified,

  /// <summary>
  ///   The task is being created in the database.
  /// </summary>
  [OriginalName("TASK_STATUS_CREATING")]
  Creating,

  /// <summary>
  ///   The task has been submitted to the queue.
  /// </summary>
  [OriginalName("TASK_STATUS_SUBMITTED")]
  Submitted,

  /// <summary>
  ///   The task is dispatched to a worker.
  /// </summary>
  [OriginalName("TASK_STATUS_DISPATCHED")]
  Dispatched,

  /// <summary>
  ///   The task is completed.
  /// </summary>
  [OriginalName("TASK_STATUS_COMPLETED")]
  Completed,

  /// <summary>
  ///   The task is in an error state.
  /// </summary>
  [OriginalName("TASK_STATUS_ERROR")]
  Error,

  /// <summary>
  ///   The task is in timeout state.
  /// </summary>
  [OriginalName("TASK_STATUS_TIMEOUT")]
  Timeout,

  /// <summary>
  ///   The task is being cancelled.
  /// </summary>
  [OriginalName("TASK_STATUS_CANCELLING")]
  Cancelling,

  /// <summary>
  ///   The task is cancelled.
  /// </summary>
  [OriginalName("TASK_STATUS_CANCELLED")]
  Cancelled,

  /// <summary>
  ///   The task is being processed.
  /// </summary>
  [OriginalName("TASK_STATUS_PROCESSING")]
  Processing,

  /// <summary>
  ///   The task is processed.
  /// </summary>
  [OriginalName("TASK_STATUS_PROCESSED")]
  Processed,

  /// <summary>
  ///   The task is being retried.
  /// </summary>
  [OriginalName("TASK_STATUS_RETRIED")]
  Retried,

  /// <summary>
  ///   Task is waiting for its dependencies before becoming executable.
  /// </summary>
  [OriginalName("TASK_STATUS_PENDING")]
  Pending,

  /// <summary>
  ///   Task is paused and will not be executed until session is resumed.
  /// </summary>
  [OriginalName("TASK_STATUS_PAUSED")]
  Paused,
}

/// <summary>
///   Class of extensions methods to convert instances of Api.gRPC.V1.TaskStatus and TaskStatus
/// </summary>
public static class TaskStatusExt
{
  /// <summary>
  ///   Convert a TaskStatus into an Api.gRPC.V1.TaskStatus
  /// </summary>
  /// <param name="status">The TaskStatus value</param>
  /// <returns>The Api.gRPC.V1.TaskStatus value</returns>
  /// <exception cref="ArgumentOutOfRangeException">For an unknown TaskStatus</exception>
  public static Api.gRPC.V1.TaskStatus ToGrpcStatus(this TaskStatus status)
    => status switch
       {
         TaskStatus.Unspecified => Api.gRPC.V1.TaskStatus.Unspecified,
         TaskStatus.Creating    => Api.gRPC.V1.TaskStatus.Creating,
         TaskStatus.Completed   => Api.gRPC.V1.TaskStatus.Completed,
         TaskStatus.Submitted   => Api.gRPC.V1.TaskStatus.Submitted,
         TaskStatus.Dispatched  => Api.gRPC.V1.TaskStatus.Dispatched,
         TaskStatus.Error       => Api.gRPC.V1.TaskStatus.Error,
         TaskStatus.Timeout     => Api.gRPC.V1.TaskStatus.Timeout,
         TaskStatus.Cancelled   => Api.gRPC.V1.TaskStatus.Cancelled,
         TaskStatus.Cancelling  => Api.gRPC.V1.TaskStatus.Cancelling,
         TaskStatus.Processing  => Api.gRPC.V1.TaskStatus.Processing,
         TaskStatus.Processed   => Api.gRPC.V1.TaskStatus.Processed,
         TaskStatus.Retried     => Api.gRPC.V1.TaskStatus.Retried,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  /// <summary>
  ///   Convert an Api.gRPC.V1.TaskStatus into a TaskStatus
  /// </summary>
  /// <param name="status">The Api.gRPC.V1.TaskStatus value</param>
  /// <returns>The TaskStatus value</returns>
  /// <exception cref="ArgumentOutOfRangeException">For an unknown Api.gRPC.V1.TaskStatus</exception>
  public static TaskStatus ToInternalStatus(this Api.gRPC.V1.TaskStatus status)
    => status switch
       {
         Api.gRPC.V1.TaskStatus.Unspecified => TaskStatus.Unspecified,
         Api.gRPC.V1.TaskStatus.Creating    => TaskStatus.Creating,
         Api.gRPC.V1.TaskStatus.Completed   => TaskStatus.Completed,
         Api.gRPC.V1.TaskStatus.Submitted   => TaskStatus.Submitted,
         Api.gRPC.V1.TaskStatus.Dispatched  => TaskStatus.Dispatched,
         Api.gRPC.V1.TaskStatus.Error       => TaskStatus.Error,
         Api.gRPC.V1.TaskStatus.Timeout     => TaskStatus.Timeout,
         Api.gRPC.V1.TaskStatus.Cancelled   => TaskStatus.Cancelled,
         Api.gRPC.V1.TaskStatus.Cancelling  => TaskStatus.Cancelling,
         Api.gRPC.V1.TaskStatus.Processing  => TaskStatus.Processing,
         Api.gRPC.V1.TaskStatus.Processed   => TaskStatus.Processed,
         Api.gRPC.V1.TaskStatus.Retried     => TaskStatus.Retried,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}

/// <summary>
///   Class of extensions methods to convert Protobuf instances into TaskState instances
/// </summary>
public static class TaskStateExt
{
  /// <summary>
  ///   Convert a TaskDetailed instance into a TaskState
  /// </summary>
  /// <param name="taskDetailed">The TaskDetailed instance</param>
  /// <returns>The TaskState instance</returns>
  public static TaskState ToTaskState(this TaskDetailed taskDetailed)
    => new()
       {
         DataDependencies = taskDetailed.DataDependencies,
         ExpectedOutputs  = taskDetailed.ExpectedOutputIds,
         TaskId           = taskDetailed.Id,
         Status           = taskDetailed.Status.ToInternalStatus(),
         CreateAt         = taskDetailed.CreatedAt.ToDateTime(),
         StartedAt        = taskDetailed.StartedAt.ToDateTime(),
         EndedAt          = taskDetailed.EndedAt.ToDateTime(),
         SessionId        = taskDetailed.SessionId,
         PayloadId        = taskDetailed.PayloadId,
       };

  /// <summary>
  ///   Convert a TaskSummary instance into a TaskState
  /// </summary>
  /// <param name="taskSummary">The TaskSummary instance</param>
  /// <returns>The TaskState instance</returns>
  public static TaskState ToTaskState(this TaskSummary taskSummary)
    => new()
       {
         TaskId    = taskSummary.Id,
         Status    = taskSummary.Status.ToInternalStatus(),
         CreateAt  = taskSummary.CreatedAt.ToDateTime(),
         StartedAt = taskSummary.StartedAt.ToDateTime(),
         EndedAt   = taskSummary.EndedAt.ToDateTime(),
         SessionId = taskSummary.SessionId,
         PayloadId = taskSummary.PayloadId,
       };
}
