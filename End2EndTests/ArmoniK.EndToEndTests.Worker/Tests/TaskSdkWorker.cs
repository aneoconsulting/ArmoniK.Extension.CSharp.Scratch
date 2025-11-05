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

using System.Text;

using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Worker.Tests;

public class TaskSdkWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public async Task<TaskResult> ExecuteAsync(ISdkTaskHandler   taskHandler,
                                             ILogger           logger,
                                             CancellationToken cancellationToken)
  {
    var resultString = taskHandler.GetStringDependency("myString");
    var resultInt    = taskHandler.GetIntDependency("myInt");
    var resultDouble = taskHandler.GetDoubleDependency("myDouble");

    // Send the input as results as is.
    await taskHandler.Outputs["resultString"]
                     .SendResultAsync(Encoding.ASCII.GetBytes(resultString),
                                      CancellationToken.None)
                     .ConfigureAwait(false);
    await taskHandler.Outputs["resultInt"]
                     .SendResultAsync(Encoding.ASCII.GetBytes(resultInt.ToString()),
                                      CancellationToken.None)
                     .ConfigureAwait(false);
    await taskHandler.Outputs["resultDouble"]
                     .SendResultAsync(Encoding.ASCII.GetBytes(resultDouble.ToString("F2")),
                                      CancellationToken.None)
                     .ConfigureAwait(false);

    return TaskResult.Success;
  }
}
