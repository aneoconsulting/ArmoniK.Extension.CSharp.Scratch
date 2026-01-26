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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extensions.CSharp.Worker.Interfaces;

namespace ArmoniK.Extensions.CSharp.Worker;

/// <summary>
///   Represents the context for handling service requests.
/// </summary>
public interface IServiceRequestContext
{
  /// <summary>
  ///   Check the health of the library worker.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the heath status of the worker.</returns>
  Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken);

  /// <summary>
  ///   Executes a task asynchronously.
  /// </summary>
  /// <param name="taskHandler">The task handler containing the task details.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  Task<Output> ExecuteTaskAsync(ITaskHandler      taskHandler,
                                CancellationToken cancellationToken);
}
