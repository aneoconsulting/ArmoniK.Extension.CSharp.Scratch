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

internal class FizzBuzzClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("FizzBuzzWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task FizzBuzz()
  {
    var input = new[]
                {
                  1,
                  3,
                  5,
                  10,
                  13,
                  15,
                  -1,
                };

    var taskDefinitions = new List<TaskDefinition>();
    foreach (var item in input)
    {
      var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary)
                                               .WithInput("value",
                                                          BlobDefinition.FromInt(item))
                                               .WithOutput("result")
                                               .WithTaskOptions(TaskConfiguration);
      var taskHandle = await SessionHandle.SubmitAsync(taskDefinition)
                                          .ConfigureAwait(false);
      taskDefinitions.Add(taskDefinition);
    }

    await Client.EventsService.WaitForBlobsAsync(SessionHandle,
                                                 taskDefinitions.Select(t => t.Outputs.Values.First()
                                                                              .BlobHandle!.BlobInfo)
                                                                .ToList(),
                                                 CancellationToken.None);

    foreach (var task in taskDefinitions)
    {
      var value = BitConverter.ToInt32(task.InputDefinitions.First()
                                           .Value.Data.Value.ToArray());
      var name = task.Outputs.First()
                     .Key;
      var blobHandle = task.Outputs.First()
                           .Value.BlobHandle;
      var rawData = await blobHandle!.DownloadBlobDataAsync(CancellationToken.None)
                                     .ConfigureAwait(false);
      switch (name)
      {
        case "result":
          var resultString = Encoding.UTF8.GetString(rawData);
          switch (value)
          {
            case 1:
              Assert.That(resultString,
                          Is.EqualTo("1 -> 1"));
              break;
            case 3:
              Assert.That(resultString,
                          Is.EqualTo("3 -> Fizz"));
              break;
            case 5:
              Assert.That(resultString,
                          Is.EqualTo("5 -> Buzz"));
              break;
            case 10:
              Assert.That(resultString,
                          Is.EqualTo("10 -> Buzz"));
              break;
            case 13:
              Assert.That(resultString,
                          Is.EqualTo("13 -> 13"));
              break;
            case 15:
              Assert.That(resultString,
                          Is.EqualTo("15 -> FizzBuzz"));
              break;
            case -1:
              Assert.That(resultString,
                          Is.EqualTo("-1 -> invalid input"));
              break;
            default:
              Assert.Fail("Invalid input: " + value);
              break;
          }

          break;
        default:
          Assert.Fail("Unknown output name: " + name);
          break;
      }
    }
  }
}
