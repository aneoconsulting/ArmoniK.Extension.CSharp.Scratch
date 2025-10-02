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
using System.Text.Json;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.DllHelper;

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

    var allResults = new List<BlobInfo>();
    foreach (var priority in Enumerable.Range(1,
                                              5))
    {
      var options = TaskConfiguration with
                    {
                      Priority = priority,
                      PartitionId = Partition,
                    };

      var taskNodes = new List<TaskNodeExt>();
      for (var i = 0; i < nTasksPerSessionPerPriority; i++)
      {
        var priorityBlobInfo = await Client.BlobService.CreateBlobAsync(Session,
                                                                        "Priority",
                                                                        BitConverter.GetBytes(priority))
                                           .ConfigureAwait(false);

        var resultName = "Result" + priority;
        var results = await Client.BlobService.CreateBlobsMetadataAsync(Session,
                                                                        [resultName])
                                  .ToListAsync()
                                  .ConfigureAwait(false);

        var payload = new Payload(new Dictionary<string, string>
                                  {
                                    {
                                      "Priority", priorityBlobInfo.BlobId
                                    },
                                  },
                                  new Dictionary<string, string>
                                  {
                                    {
                                      resultName, results[0].BlobId
                                    },
                                  });

        var payloadJson = JsonSerializer.Serialize(payload);

        var payloadBlobId = await Client.BlobService.CreateBlobAsync(Session,
                                                                     "Payload",
                                                                     Encoding.ASCII.GetBytes(payloadJson))
                                        .ConfigureAwait(false);

        allResults.Add(results[0]);
        taskNodes.Add(new TaskNodeExt
                      {
                        TaskOptions      = options,
                        Session          = Session,
                        Payload          = payloadBlobId,
                        DataDependencies = [priorityBlobInfo],
                        ExpectedOutputs  = [results[0]],
                        DynamicLibrary   = TaskLibraryDefinition,
                      });
      }

      await Client.TasksService.SubmitTasksWithDllAsync(Session,
                                                        taskNodes,
                                                        DllBlob,
                                                        false,
                                                        CancellationToken.None)
                  .ConfigureAwait(false);
    }

    await Client.EventsService.WaitForBlobsAsync(Session,
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
