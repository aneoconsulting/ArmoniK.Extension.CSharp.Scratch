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
    => await SetupBaseAsync("TaskSdk")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task TaskSdk()
  {
    var options = TaskConfiguration with
                  {
                    Priority = 1,
                    PartitionId = Partition,
                  };

    var taskDefinition = new TaskDefinition().WithInput("myString",
                                                        BlobDefinition.FromString("Hello world!"))
                                             .WithInput("myInt",
                                                        BlobDefinition.FromInt(404))
                                             .WithInput("myDouble",
                                                        BlobDefinition.FromDouble(3.14))
                                             .WithOutput("resultString")
                                             .WithOutput("resultInt")
                                             .WithOutput("resultDouble")
                                             .WithAdditionalTaskOptions(options);
    var taskHandle = await SessionHandle.SubmitAsync(taskDefinition)
                                        .ConfigureAwait(false);

    await Client.EventsService.WaitForBlobsAsync(Session,
                                                 taskDefinition.Outputs.Values.Select(b => b.BlobHandle!.BlobInfo)
                                                               .ToList(),
                                                 CancellationToken.None);

    var resultString = "";
    var resultInt    = "";
    var resultDouble = "";
    foreach (var pair in taskDefinition.Outputs)
    {
      var name       = pair.Key;
      var blobHandle = pair.Value.BlobHandle;
      var rawData = await blobHandle!.DownloadBlobDataAsync(CancellationToken.None)
                                     .ConfigureAwait(false);
      switch (name)
      {
        case "resultString":
          resultString = Encoding.UTF8.GetString(rawData);
          break;
        case "resultInt":
          resultInt = Encoding.UTF8.GetString(rawData);
          break;
        case "resultDouble":
          resultDouble = Encoding.UTF8.GetString(rawData);
          break;
      }
    }

    Assert.That(resultString,
                Is.EqualTo("Hello world!"));
    Assert.That(resultInt,
                Is.EqualTo("404"));
    Assert.That(resultDouble,
                Is.EqualTo("3.14"));
  }
}
