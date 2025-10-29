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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

namespace ArmoniK.EndToEndTests.Client.Tests;

public class TaskSdkClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("TaskSdkWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task TaskSdk()
  {
    var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary)
                                             .WithInput("inputString",
                                                        BlobDefinition.FromString("blobInputString",
                                                                                  "Hello world!"))
                                             .WithOutput("outputString",
                                                         BlobDefinition.CreateOutputBlobDefinition("blobOutputString"))
                                             .WithTaskOptions(TaskConfiguration);
    var taskHandle = await SessionHandle.SubmitAsync(taskDefinition)
                                        .ConfigureAwait(false);

    await Client.EventsService.WaitForBlobsAsync(SessionHandle,
                                                 taskDefinition.Outputs.Values.Select(b => b.BlobHandle!.BlobInfo)
                                                               .ToList(),
                                                 CancellationToken.None);

    var resultString = "";
    var outputName = taskDefinition.Outputs.Single()
                                   .Key;
    var outputBlobHandle = taskDefinition.Outputs.Single()
                                         .Value.BlobHandle;
    var inputBlobHandle = taskDefinition.InputDefinitions.Single()
                                        .Value.BlobHandle;
    var rawData = await outputBlobHandle!.DownloadBlobDataAsync(CancellationToken.None)
                                         .ConfigureAwait(false);
    if (outputName == "outputString")
    {
      resultString = Encoding.UTF8.GetString(rawData);
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(inputBlobHandle!.BlobInfo.BlobName,
                                  Is.EqualTo("blobInputString"));
                      Assert.That(outputBlobHandle!.BlobInfo.BlobName,
                                  Is.EqualTo("blobOutputString"));
                      Assert.That(resultString,
                                  Is.EqualTo("Hello world!"));
                    });
  }
}
