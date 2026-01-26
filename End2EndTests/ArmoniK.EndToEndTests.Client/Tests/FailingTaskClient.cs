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

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Handles;

namespace ArmoniK.EndToEndTests.Client.Tests;

internal class FailingTaskClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("FailingTaskWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task AbortedTask()
  {
    var callback = new Callback(SessionHandle!);
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

    Assert.That(callback.Aborted,
                Is.EqualTo(true));
  }

  private class Callback : ICallback
  {
    private readonly SessionHandle sessionHandle_;

    public Callback(SessionHandle sessionHandle)
      => sessionHandle_ = sessionHandle;

    public bool Aborted { get; private set; }

    public ValueTask OnSuccessAsync(BlobHandle        blob,
                                    byte[]            rawData,
                                    CancellationToken cancellationToken)
    {
      Assert.Fail($"blob {blob.BlobInfo.BlobId} expected to be aborted");
      return ValueTask.CompletedTask;
    }

    public async ValueTask OnErrorAsync(BlobHandle        blob,
                                        Exception?        exception,
                                        CancellationToken cancellationToken)
    {
      Aborted = true;
      await sessionHandle_.AbortCallbacksAsync()
                          .ConfigureAwait(false);
    }
  }
}
