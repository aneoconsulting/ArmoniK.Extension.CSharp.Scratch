// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;

namespace UsageExample;

internal class Program
{
  private static IConfiguration   _configuration;
  private static ILogger<Program> _logger;

  private static async Task Main(string[] args)
  {
    Console.WriteLine("Hello Armonik New Extension !");


    Log.Logger = new LoggerConfiguration().MinimumLevel.Override("Microsoft",
                                                                 LogEventLevel.Information)
                                          .Enrich.FromLogContext()
                                          .WriteTo.Console()
                                          .CreateLogger();

    var factory = new LoggerFactory(new[]
                                    {
                                      new SerilogLoggerProvider(Log.Logger),
                                    },
                                    new LoggerFilterOptions().AddFilter("Grpc",
                                                                        LogLevel.Error));

    _logger = factory.CreateLogger<Program>();

    var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json",
                                                         false)
                                            .AddEnvironmentVariables();

    _configuration = builder.Build();

    var defaultTaskOptions = new TaskConfiguration(2,
                                                   1,
                                                   "subtasking",
                                                   TimeSpan.FromHours(1),
                                                   new Dictionary<string, string>
                                                   {
                                                     {
                                                       "UseCase", "Launch"
                                                     },
                                                   });

    var props = new Properties(_configuration);

    var client = new ArmoniKClient(props,
                                   factory,
                                   defaultTaskOptions);

    var sessionService = await client.GetSessionService();

    var session = await sessionService.CreateSessionAsync(defaultTaskOptions,

                    ["subtasking"]);

    Console.WriteLine($"sessionId: {session.SessionId}");

    var blobService = await client.GetBlobService();

    var tasksService = await client.GetTasksService();

    var eventsService = await client.GetEventsService();

    var payload = await blobService.CreateBlobAsync(session,
                                                    "Payload",
                                                    Encoding.ASCII.GetBytes("Hello"));

    Console.WriteLine($"payloadId: {payload.BlobId}");

    var results = blobService.CreateBlobsMetadataAsync(session,
                                                       new[]
                                                       {
                                                         "Result",
                                                       });

    var blobInfos = await results.ToListAsync();

    var result = blobInfos[0];

    Console.WriteLine($"resultId: {result.BlobId}");

    var task = await tasksService.SubmitTasksAsync(session,
                                                   new List<TaskNode>([new TaskNode
                                                                       {
                                                                         Payload = payload,
                                                                         ExpectedOutputs = new[]
                                                                                           {
                                                                                             result,
                                                                                           },
                                                                       },]));

    Console.WriteLine($"taskId: {task.Single().TaskId}");

    await eventsService.WaitForBlobsAsync(session,
                                          new List<BlobInfo>([result]));

    var download = await blobService.DownloadBlobAsync(result,
                                                       CancellationToken.None);
    var stringArray = Encoding.ASCII.GetString(download)
                              .Split(new[]
                                     {
                                       '\n',
                                     },
                                     StringSplitOptions.RemoveEmptyEntries);

    foreach (var returnString in stringArray)
    {
      Console.WriteLine($"{returnString}");
    }
  }
}
