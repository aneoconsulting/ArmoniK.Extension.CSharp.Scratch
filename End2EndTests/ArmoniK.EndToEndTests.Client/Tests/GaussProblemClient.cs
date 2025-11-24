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
    var N = 10;
    var task = new TaskDefinition().WithLibrary(WorkerLibrary)
                                   .WithTaskOptions(TaskConfiguration)
                                   .WithOutput("result",
                                               BlobDefinition.CreateOutput("resultBlob"));
    for (var i = 1; i <= N; i++)
    {
      task.WithInput("blob" + i,
                     BlobDefinition.FromString("input" + 1,
                                               i.ToString()));
    }

    await SessionHandle.SubmitAsync([task])
                       .ConfigureAwait(false);

    await Client.EventsService.WaitForBlobsAsync(SessionHandle,
                                                 task.Outputs.Values.Select(b => b.BlobHandle!.BlobInfo)
                                                     .ToList(),
                                                 CancellationToken.None);

    var blobHandle = task.Outputs.Single()
                         .Value.BlobHandle;
    var rawData = await blobHandle!.DownloadBlobDataAsync(CancellationToken.None)
                                   .ConfigureAwait(false);
    var resultString = Encoding.UTF8.GetString(rawData);

    var totalExpected = N * (N + 1) / 2; // 55 for N=10, 5050 for N=100
    Assert.That(resultString,
                Is.EqualTo(totalExpected.ToString()));
  }
}
