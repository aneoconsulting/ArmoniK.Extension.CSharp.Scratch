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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.DllCommon;

/// <summary>
///   Defines the contract for worker implementations that can execute tasks and perform health checks.
/// </summary>
public interface IWorker
{
  /// <summary>
  ///   Executes a task asynchronously.
  /// </summary>
  /// <param name="taskHandler">The task handler containing task details and data.</param>
  /// <param name="logger">The logger instance for recording execution information.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the execution output.</returns>
  Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                            ILogger           logger,
                            CancellationToken cancellationToken);

  /// <summary>
  ///   Checks the health of the component.
  /// </summary>
  /// <returns>True if the component is healthy, false otherwise.</returns>
  Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default);
}
