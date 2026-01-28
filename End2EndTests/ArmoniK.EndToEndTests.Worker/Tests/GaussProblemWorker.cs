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

using TaskDefinition = ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Task.TaskDefinition;

namespace ArmoniK.EndToEndTests.Worker.Tests;

/// <summary>
///   Adds all integers given as inputs, and set the result in the blob output.
///   Inputs and output are expected to be strings containing integers.
/// </summary>
public class GaussProblemWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public async Task<TaskResult> ExecuteAsync(ISdkTaskHandler   taskHandler,
                                             ILogger           logger,
                                             CancellationToken cancellationToken)
  {
    if (taskHandler.Inputs.Count > 2)
    {
      // Create the graph of subtasks, each subtask having 2 inputs only
      await BuildSubTasksGraph(taskHandler,
                               cancellationToken)
        .ConfigureAwait(false);
    }
    else if (taskHandler.Inputs.Count == 2)
    {
      // Execute the subtask that add only 2 operands
      await Addition(taskHandler,
                     cancellationToken)
        .ConfigureAwait(false);
    }
    else
    {
      return TaskResult.Failure("Unexpected input count");
    }

    return TaskResult.Success;
  }

  private async Task BuildSubTasksGraph(ISdkTaskHandler   taskHandler,
                                        CancellationToken cancellationToken)
  {
    var             taskDefinitions    = new List<TaskDefinition>();
    var             allTaskDefinitions = new List<TaskDefinition>();
    BlobDefinition? lastBlobDefinition = null;
    var inputs = taskHandler.Inputs.Values.Select(BlobDefinition.FromBlobHandle)
                            .ToList();
    var currentLibrary = taskHandler.TaskOptions.GetDynamicLibrary();

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
                                                   BlobDefinition.FromBlobHandle(taskHandler.Outputs.Values.Single()))
                                       .WithLibrary(currentLibrary)
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
        taskDefinitions.Add(new TaskDefinition().WithInput("blob1",
                                                           inputs[i])
                                                .WithInput("blob2",
                                                           inputs[i + 1])
                                                .WithOutput("output",
                                                            BlobDefinition.CreateOutput("output"))
                                                .WithLibrary(currentLibrary)
                                                .WithTaskOptions(taskHandler.TaskOptions));
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

  private static async Task Addition(ISdkTaskHandler   taskHandler,
                                     CancellationToken cancellationToken)
  {
    var op1 = int.Parse(taskHandler.Inputs["blob1"]
                                   .GetStringData());
    var op2 = int.Parse(taskHandler.Inputs["blob2"]
                                   .GetStringData());

    var result = (op1 + op2).ToString();
    await taskHandler.Outputs.Values.Single()
                     .SendStringResultAsync(result,
                                            cancellationToken: cancellationToken)
                     .ConfigureAwait(false);
  }
}
