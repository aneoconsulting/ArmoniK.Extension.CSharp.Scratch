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

public class GaussProblemClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("GaussProblemWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  /// <summary>
  ///   Adds all integers from 1 to 10 (added two by two on worker side with subtasking)
  ///   and check that result is 55
  /// </summary>
  [Test]
  public async Task GaussProblem()
  {
    var N        = 10;
    var callback = new Callback();
    var task = new TaskDefinition().WithLibrary(WorkerLibrary!)
                                   .WithTaskOptions(TaskConfiguration!)
                                   .WithOutput("result",
                                               BlobDefinition.CreateOutput("resultBlob")
                                                             .WithCallback(callback));
    for (var i = 1; i <= N; i++)
    {
      task.WithInput("blob" + i,
                     BlobDefinition.FromString("input" + 1,
                                               i.ToString()));
    }

    await SessionHandle!.SubmitAsync([task])
                        .ConfigureAwait(false);

    await SessionHandle.WaitCallbacksAsync()
                       .ConfigureAwait(false);

    var resultString = Encoding.UTF8.GetString(callback.Result);

    var totalExpected = N * (N + 1) / 2; // 55 for N=10, 5050 for N=100
    Assert.That(resultString,
                Is.EqualTo(totalExpected.ToString()));
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
