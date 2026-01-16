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

using System.Text;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Handles;

namespace ArmoniK.EndToEndTests.Client.Tests;

public class PriorityClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("PriorityWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task Priority()
  {
    var nTasksPerSessionPerPriority = 5;

    var allTasks = new List<TaskDefinition>();
    foreach (var priority in Enumerable.Range(1,
                                              5))
    {
      var options = TaskConfiguration! with
                    {
                      Priority = priority,
                      PartitionId = Partition,
                    };

      var taskDefinitions = new List<TaskDefinition>();
      for (var i = 0; i < nTasksPerSessionPerPriority; i++)
      {
        var priorityBlobInfo = await Client!.BlobService.CreateBlobAsync(SessionHandle!,
                                                                         "Priority",
                                                                         Encoding.UTF8.GetBytes(priority.ToString()))
                                            .ConfigureAwait(false);
        var priorityBlobHandle = new BlobHandle(priorityBlobInfo,
                                                Client);

        var resultName = "Result" + priority;
        var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary!)
                                                 .WithInput("Priority",
                                                            BlobDefinition.FromBlobHandle(priorityBlobHandle))
                                                 .WithOutput(resultName,
                                                             BlobDefinition.CreateOutput(resultName))
                                                 .WithTaskOptions(options);
        taskDefinitions.Add(taskDefinition);
      }

      allTasks.AddRange(taskDefinitions);
      foreach (var taskDefinition in taskDefinitions)
      {
        await SessionHandle!.SubmitAsync([taskDefinition])
                            .ConfigureAwait(false);
      }

      taskDefinitions.Clear();
    }

    var allResults = allTasks.SelectMany(t => t.Outputs.Values.Select(o => o.BlobHandle!.BlobInfo))
                             .ToList();
    await Client!.EventsService.WaitForBlobsAsync(SessionHandle!,
                                                  allResults,
                                                  CancellationToken.None)
                 .ConfigureAwait(false);

    foreach (var blobInfo in allResults)
    {
      var result = await Client.BlobService.DownloadBlobAsync(blobInfo)
                               .ConfigureAwait(false);
      var strResult = Encoding.ASCII.GetString(result);
      var priority  = blobInfo.BlobName.Substring("Result".Length);

      Assert.That(strResult,
                  Is.EqualTo($"Payload is {priority} and TaskOptions.Priority is {priority}"));
    }
  }
}
