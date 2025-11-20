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

using ArmoniK.Extension.CSharp.Worker.Interfaces;
using ArmoniK.Extension.CSharp.Worker.Interfaces.Common.Domain.Task;

namespace StaticWorkerExample;

/// <summary>
///   Example implementation of IWorker <see cref="IWorker" /> for demonstration purposes.
/// </summary>
internal class HelloWorker : IWorker
{
  /// <summary>
  ///   Executes a task asynchronously that return "Hello " before the name received as input.
  /// </summary>
  /// <param name="taskHandler">The task handler containing task details and expected results.</param>
  /// <param name="logger">The logger instance for recording execution information.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing a successful output.</returns>
  /// <exception cref="InvalidOperationException">Thrown when no expected results are found (Single() fails).</exception>
  public async Task<TaskResult> ExecuteAsync(ISdkTaskHandler   taskHandler,
                                             ILogger           logger,
                                             CancellationToken cancellationToken)
  {
    var name = taskHandler.Inputs["name"]
                          .GetStringData();

    await taskHandler.Outputs["helloResult"]
                     .SendStringResultAsync($"Hello {name}!",
                                            cancellationToken: cancellationToken)
                     .ConfigureAwait(false);
    return TaskResult.Success;
  }

  /// <summary>
  ///   Checks the health status of this example worker.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token to cancel the health check operation.</param>
  /// <returns>A healthy HealthCheckResult indicating the example worker is always operational.</returns>
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());
}
