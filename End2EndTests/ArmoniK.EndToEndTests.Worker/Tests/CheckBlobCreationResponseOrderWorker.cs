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

using ArmoniK.Extensions.CSharp.Worker.Interfaces;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Task;

using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Worker.Tests;

internal class CheckBlobCreationResponseOrderWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public async Task<TaskResult> ExecuteAsync(ISdkTaskHandler   taskHandler,
                                             ILogger           logger,
                                             CancellationToken cancellationToken)
  {
    var isSubTask = taskHandler.Inputs.ContainsKey("SubTask");

    foreach (var pair in taskHandler.Inputs)
    {
      var inputName = pair.Key;
      if (inputName == "SubTask")
      {
        continue;
      }

      var inputValue = pair.Value.GetStringData();
      if (inputName != inputValue)
      {
        // Then it is different because while the blobs were created, they were not assigned to the right blob id.
        return TaskResult.Failure($"Unexpected difference between input '{inputName}' and value '{inputValue}'");
      }

      if (isSubTask)
      {
        // The outputs are delegated to the sub task
        await taskHandler.Outputs[inputName]
                         .SendStringResultAsync(inputName,
                                                cancellationToken: cancellationToken)
                         .ConfigureAwait(false);
      }
    }

    if (!isSubTask)
    {
      // Create a sub task to do the same test at sub task level
      string[] cities         = ["Paris", "Lyon", "Marseilles", "Nice", "Bordeaux", "Grenoble", "Brest", "Nancy", "Montpelier"];
      var      currentLibrary = taskHandler.TaskOptions.GetDynamicLibrary();

      var taskDefinition = new TaskDefinition().WithInput("SubTask",
                                                          BlobDefinition.FromString("SubTask",
                                                                                    "1"))
                                               .WithLibrary(currentLibrary)
                                               .WithTaskOptions(taskHandler.TaskOptions);

      foreach (var city in cities)
      {
        taskDefinition.WithInput(city,
                                 BlobDefinition.FromString(city,
                                                           city))
                      .WithOutput(city,
                                  BlobDefinition.FromBlobHandle(taskHandler.Outputs[city]));
      }

      await taskHandler.SubmitTasksAsync([taskDefinition],
                                         taskHandler.TaskOptions,
                                         cancellationToken)
                       .ConfigureAwait(false);
    }

    return TaskResult.Success;
  }
}
