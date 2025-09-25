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

using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.DllHelper;
using ArmoniK.Extension.CSharp.Client.DllHelper.Common;
using ArmoniK.Extension.CSharp.DllCommon;
using ArmoniK.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Client.Tests;

public class PriorityClient
{
  public const string                Partition = "dllworker";
  private      Properties            Properties            { get; set; }
  private      TaskConfiguration     TaskConfiguration     { get; set; }
  private      SessionInfo           Session               { get; set; }
  private      DllBlob               DllBlob               { get; set; }
  private      TaskLibraryDefinition TaskLibraryDefinition { get; set; }
  private      ArmoniKClient         Client                { get; set; }

  [SetUp]
  public async Task Setup()
  {
    var builder = new ConfigurationBuilder().SetBasePath(TestContext.CurrentContext.TestDirectory)
                                            .AddJsonFile("appsettings.json",
                                                         true,
                                                         false)
                                            .AddEnvironmentVariables();

    var config = builder.Build();
    var loggerFactory = new LoggerFactory([
                                            new SerilogLoggerProvider(new LoggerConfiguration().ReadFrom.Configuration(config)
                                                                                               .CreateLogger()),
                                          ],
                                          new LoggerFilterOptions().AddFilter("Grpc",
                                                                              LogLevel.Error));

    var properties = new Properties(config);
    TaskConfiguration = new TaskConfiguration(5,
                                              1,
                                              Partition,
                                              TimeSpan.FromSeconds(300));
    var dynamicLibrary = new DynamicLibrary
                         {
                           Name        = "ArmoniK.EndToEndTests.Worker",
                           DllFileName = "ArmoniK.EndToEndTests.Worker.dll",
                           Version     = "1.0.0.0",
                           PathToFile  = @"ArmoniK.EndToEndTests.Worker/1.0.0-100",
                         };

    Client = new ArmoniKClient(properties,
                               loggerFactory,
                               TaskConfiguration);

    Session = await Client.SessionService.CreateSessionWithDllAsync(TaskConfiguration,
                                                                    [Partition],
                                                                    [dynamicLibrary])
                          .ConfigureAwait(false);

    var filePath = Path.Join(AppContext.BaseDirectory,
                             @"..\..\..\..\..\packages\ArmoniK.EndToEndTests.Worker-v1.0.0-100.zip");
    DllBlob = await Client.BlobService.SendDllBlobAsync(Session,
                                                        dynamicLibrary,
                                                        filePath,
                                                        false,
                                                        CancellationToken.None)
                          .ConfigureAwait(false);

    TaskLibraryDefinition = new TaskLibraryDefinition(dynamicLibrary,
                                                      "ArmoniK.EndToEndTests.Worker.Tests",
                                                      "PriorityWorker");
  }

  [TearDown]
  public async Task TearDown()
  {
    await Client.SessionService.CloseSessionAsync(Session);
    await Client.SessionService.PurgeSessionAsync(Session);
    await Client.SessionService.DeleteSessionAsync(Session);
    Properties            = null;
    TaskConfiguration     = null;
    Session               = null;
    DllBlob               = null;
    TaskLibraryDefinition = null;
    Client                = null;
  }

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
        var payload = Client.BlobService.CreateBlobAsync(Session,
                                                         "GetPriority",
                                                         BitConverter.GetBytes(priority))
                            .WaitSync();

        var results = Client.BlobService.CreateBlobsMetadataAsync(Session,
                                                                  ["Result" + priority])
                            .ToListAsync()
                            .WaitSync();
        allResults.Add(results[0]);
        taskNodes.Add(new TaskNodeExt
                      {
                        TaskOptions     = options,
                        Session         = Session,
                        Payload         = payload,
                        ExpectedOutputs = [results[0]],
                        DynamicLibrary  = TaskLibraryDefinition,
                      });
      }

      await Client.TasksService.SubmitTasksWithDllAsync(Session,
                                                        taskNodes,
                                                        DllBlob,
                                                        false,
                                                        CancellationToken.None);
    }

    await Client.EventsService.WaitForBlobsAsync(Session,
                                                 allResults,
                                                 CancellationToken.None);

    foreach (var blobInfo in allResults)
    {
      var result    = await Client.BlobService.DownloadBlobAsync(blobInfo);
      var strResult = Encoding.ASCII.GetString(result);
      var priority  = blobInfo.BlobName.Substring("Result".Length);

      Assert.That(strResult,
                  Is.EqualTo($"Payload is {priority} and TaskOptions.Priority is {priority}"));
    }
  }
}
