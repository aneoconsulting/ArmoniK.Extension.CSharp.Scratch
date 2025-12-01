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

using ArmoniK.Api.gRPC.V1.Tasks;

namespace ArmoniK.Extension.CSharp.Common.Common.Domain.Task;

/// <summary>
///   Represents task characteristics that are typically derived from a task.
/// </summary>
public record TaskInfos
{
  /// <summary>
  ///   Identifier of the task.
  /// </summary>
  public string TaskId { get; init; }

  /// <summary>
  ///   Collection of expected output IDs.
  /// </summary>
  public IEnumerable<string> ExpectedOutputs { get; init; }

  /// <summary>
  ///   Collection of data dependencies IDs.
  /// </summary>
  public IEnumerable<string> DataDependencies { get; init; }

  /// <summary>
  ///   Identifier for the payload associated with the task.
  /// </summary>
  public string PayloadId { get; init; }

  /// <summary>
  ///   Session ID associated with the task.
  /// </summary>
  public string SessionId { get; init; }
}

/// <summary>
///   Class of extensions methods to convert Protobuf instances and TaskInfos instances
/// </summary>
public static class TaskInfosExt
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskInfos" /> class with detailed task information from a task
  ///   submission and session ID.
  /// </summary>
  /// <param name="taskInfo">The task information from the task submission response.</param>
  /// <param name="sessionId">The session ID associated with the task.</param>
  public static TaskInfos ToTaskInfos(this SubmitTasksResponse.Types.TaskInfo taskInfo,
                                      string                                  sessionId)
    => new()
       {
         TaskId           = taskInfo.TaskId,
         ExpectedOutputs  = taskInfo.ExpectedOutputIds,
         DataDependencies = taskInfo.DataDependencies,
         PayloadId        = taskInfo.PayloadId,
         SessionId        = sessionId,
       };
}
