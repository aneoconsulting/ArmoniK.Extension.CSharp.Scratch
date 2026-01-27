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
using ArmoniK.Extensions.CSharp.Client.Handles;

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
    var callback = new Callback();
    var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary!)
                                             .WithInput("inputString",
                                                        BlobDefinition.FromString("blobInputString",
                                                                                  "Hello world!"))
                                             .WithOutput("outputString",
                                                         BlobDefinition.CreateOutput("blobOutputString")
                                                                       .WithCallback(callback))
                                             .WithTaskOptions(TaskConfiguration!);
    await SessionHandle!.SubmitAsync([taskDefinition])
                        .ConfigureAwait(false);

    await SessionHandle.WaitCallbacksAsync()
                       .ConfigureAwait(false);

    var resultString = "";
    var outputName = taskDefinition.Outputs.Single()
                                   .Key;
    var outputBlobHandle = taskDefinition.Outputs.Single()
                                         .Value.BlobHandle;
    var inputBlobHandle = taskDefinition.InputDefinitions.Single()
                                        .Value.BlobHandle;
    if (outputName == "outputString")
    {
      resultString = Encoding.UTF8.GetString(callback.Result);
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

  [Test]
  public async Task NumerousCallbacks()
  {
    var taskDefinitions = new List<TaskDefinition>();
    for (var i = 0; i < 100; i++)
    {
      taskDefinitions.Add(new TaskDefinition().WithLibrary(WorkerLibrary!)
                                              .WithInput("inputString",
                                                         BlobDefinition.FromString("blobInputString" + i,
                                                                                   "Hello world!"))
                                              .WithOutput("outputString",
                                                          BlobDefinition.CreateOutput("blobOutputString" + i)
                                                                        .WithCallback(new Callback()))
                                              .WithTaskOptions(TaskConfiguration!));
    }

    await SessionHandle!.SubmitAsync(taskDefinitions)
                        .ConfigureAwait(false);
    await SessionHandle.WaitCallbacksAsync()
                       .ConfigureAwait(false);

    foreach (var task in taskDefinitions)
    {
      var resultString = Encoding.UTF8.GetString(((Callback)task.Outputs.Single()
                                                                .Value.CallBack!).Result);

      Assert.That(resultString,
                  Is.EqualTo("Hello world!"));
    }
  }

  private class Callback : ICallback
  {
    public byte[] Result { get; private set; } = [];

    public ValueTask OnSuccessAsync(BlobHandle        blob,
                                    byte[]            rawData,
                                    CancellationToken cancellationToken)
    {
      Result = rawData;
      return ValueTask.CompletedTask;
    }

    public ValueTask OnErrorAsync(BlobHandle        blob,
                                  Exception?        exception,
                                  CancellationToken cancellationToken)
    {
      Assert.Fail(exception?.Message ?? $"blob {blob.BlobInfo.BlobId} aborted");
      return ValueTask.CompletedTask;
    }
  }
}
