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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Logging;

using TaskDefinition = ArmoniK.Extension.CSharp.DllCommon.TaskDefinition;

namespace ArmoniK.EndToEndTests.Worker.Tests;

public class GaussProblemWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public async Task<TaskResult> ExecuteAsync(ISdkTaskHandler   taskHandler,
                                             ILogger           logger,
                                             CancellationToken cancellationToken)
  {
    if (taskHandler.Inputs.Count == 2)
    {
      var result = taskHandler.Inputs.Values.Select(i => int.Parse(i.GetStringData()))
                              .Sum();

      await taskHandler.Outputs.Values.Single()
                       .SendStringResultAsync(result.ToString(),
                                              cancellationToken: cancellationToken)
                       .ConfigureAwait(false);
    }
    else
    {
      var inputs = taskHandler.Inputs.Values.Select(BlobDefinition.FromBlobHandle)
                              .ToList();
      var             taskDefinitions    = new List<TaskDefinition>();
      var             allTaskDefinitions = new List<TaskDefinition>();
      BlobDefinition? lastBlobDefinition = null;

      do
      {
        var blobCount = inputs.Count;
        if (blobCount == 2)
        {
          // This is the last task
          var task = new TaskDefinition().WithInput("blob1",
                                                    inputs[0])
                                         .WithInput("blob2",
                                                    inputs[1])
                                         .WithOutput("finalOutput",
                                                     taskHandler.Outputs.Values.Single())
                                         .WithTaskOptions(taskHandler.TaskOptions);
          allTaskDefinitions.Add(task);
          break;
        }

        if (blobCount % 2 == 1)
        {
          blobCount--;
          lastBlobDefinition = inputs[blobCount];
        }

        for (var i = 0; i < blobCount; i += 2)
        {
          taskDefinitions.Add(CreateTask(inputs[i],
                                         inputs[i + 1],
                                         taskHandler.TaskOptions));
        }

        // All outputs of current task level become the inputs of the next level
        inputs = taskDefinitions.SelectMany(t => t.OutputDefinitions.Values)
                                .ToList();
        if (lastBlobDefinition != null)
        {
          inputs.Add(lastBlobDefinition);
          lastBlobDefinition = null;
        }

        allTaskDefinitions.AddRange(taskDefinitions);
        taskDefinitions.Clear();
      } while (true);

      await taskHandler.SubmitTasksAsync(allTaskDefinitions,
                                         taskHandler.TaskOptions,
                                         cancellationToken)
                       .ConfigureAwait(false);
    }

    return TaskResult.Success;
  }

  private TaskDefinition CreateTask(BlobDefinition    blob1,
                                    BlobDefinition    blob2,
                                    TaskConfiguration taskOptions)
    => new TaskDefinition().WithInput("blob1",
                                      blob1)
                           .WithInput("blob2",
                                      blob2)
                           .WithOutput("output")
                           .WithTaskOptions(taskOptions);
}
