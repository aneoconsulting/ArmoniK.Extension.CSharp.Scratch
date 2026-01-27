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

internal class CheckBlobCreationResponseOrderClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("CheckBlobCreationResponseOrderWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task Cities()
  {
    string[] cities = ["Paris", "Lyon", "Marseilles", "Nice", "Bordeaux", "Grenoble", "Brest", "Nancy", "Montpelier"];

    var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary!)
                                             .WithTaskOptions(TaskConfiguration!);
    foreach (var city in cities)
    {
      taskDefinition.WithInput(city,
                               BlobDefinition.FromString(city,
                                                         city))
                    .WithOutput(city,
                                BlobDefinition.CreateOutput(city)
                                              .WithCallback(new Callback()));
    }

    await SessionHandle!.SubmitAsync([taskDefinition])
                        .ConfigureAwait(false);

    await SessionHandle.WaitCallbacksAsync()
                       .ConfigureAwait(false);

    foreach (var blob in taskDefinition.Outputs.Values)
    {
      var name = blob.BlobHandle!.BlobInfo.BlobName;
      var data = ((Callback)blob.CallBack!).Result;
      Assert.That(data,
                  Is.EqualTo(name));
    }
  }

  private class Callback : ICallback
  {
    public string Result { get; private set; } = string.Empty;

    public ValueTask OnSuccessAsync(BlobHandle        blob,
                                    byte[]            rawData,
                                    CancellationToken cancellationToken)
    {
      Result = Encoding.UTF8.GetString(rawData);
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
