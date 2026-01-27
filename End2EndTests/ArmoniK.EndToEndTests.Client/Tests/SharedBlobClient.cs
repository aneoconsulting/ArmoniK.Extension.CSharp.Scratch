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

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Utils;

namespace ArmoniK.EndToEndTests.Client.Tests;

public class SharedBlobClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("SharedBlobWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task SharedBlob()
  {
    var inputBlobDefinition = BlobDefinition.FromString("sharedBlobInput",
                                                        "Hello!");

    var taskDefinitions = new List<TaskDefinition>();
    for (var i = 0; i < 10; i++)
    {
      taskDefinitions.Add(new TaskDefinition().WithLibrary(WorkerLibrary!)
                                              .WithInput("sharedBlobInput",
                                                         inputBlobDefinition)
                                              .WithOutput("outputBlob",
                                                          BlobDefinition.CreateOutput("outputBlob"))
                                              .WithTaskOptions(TaskConfiguration!));
    }

    await SessionHandle!.SubmitAsync(taskDefinitions)
                        .ConfigureAwait(false);
    var allResults = taskDefinitions.SelectMany(task => task.Outputs.Values.Select(blobDef => blobDef.BlobHandle));
    await Client!.EventsService.WaitForBlobsAsync(SessionHandle!,
                                                  allResults.Select(blobHandle => blobHandle!.BlobInfo)
                                                            .AsICollection(),
                                                  CancellationToken.None)
                 .ConfigureAwait(false);

    var referenceInputBlobId = inputBlobDefinition.BlobHandle!.BlobInfo.BlobId;
    foreach (var blobHandle in allResults)
    {
      var inputBlobId = Encoding.UTF8.GetString(await blobHandle!.DownloadBlobDataAsync(CancellationToken.None)
                                                                 .ConfigureAwait(false));

      // As the input blob is the same for all tasks, we check that the blob id is the same
      Assert.That(inputBlobId,
                  Is.EqualTo(referenceInputBlobId));
    }
  }
}
