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

using System.CommandLine;
using System.Text;

using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Library;
using ArmoniK.Extension.CSharp.Client.Services;

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

  internal static async Task RunAsync(string filePath)
  {
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
                                                   "dll",
                                                   TimeSpan.FromHours(1));


    var props = new Properties(_configuration);

    var client = new ArmoniKClient(props,
                                   factory,
                                   defaultTaskOptions);

    var dynamicLib = new DynamicLibrary
                     {
                       Symbol      = "LibraryExample.Worker",
                       LibraryPath = "publish/LibraryExample.dll",
                     };

    SessionInfo session = await client.SessionService.CreateSessionAsync(["dll"])
                                      .ConfigureAwait(false);

    Console.WriteLine($"sessionId: {session.SessionId}");

    var blobService = client.BlobService;

    var tasksService = client.TasksService;

    var eventsService = client.EventsService;

    var payload = await blobService.CreateBlobAsync(session,
                                                    "Payload",
                                                    Encoding.ASCII.GetBytes("Hello"))
                                   .ConfigureAwait(false);

    Console.WriteLine($"payloadId: {payload.BlobId}");

    var results = blobService.CreateBlobsMetadataAsync(session,
                                                       [("Result", false)]);

    var blobInfos = await results.ToListAsync()
                                 .ConfigureAwait(false);

    var result = blobInfos[0];

    var dllBlob = await blobService.SendDllBlobAsync(session,
                                                     dynamicLib,
                                                     filePath,
                                                     false,
                                                     CancellationToken.None)
                                   .ConfigureAwait(false);

    Console.WriteLine($"resultId: {result.BlobId}");
    Console.WriteLine($"libraryId: {dllBlob.BlobId}");

    defaultTaskOptions.AddDynamicLibrary(dynamicLib);

    var task = await tasksService.SubmitTasksWithDllAsync(session,
                                                          new List<TaskNodeExt>
                                                          {
                                                            new()
                                                            {
                                                              Payload         = payload,
                                                              ExpectedOutputs = [result],
                                                              TaskOptions     = defaultTaskOptions,
                                                              DynamicLibrary  = dynamicLib,
                                                            },
                                                          },
                                                          dllBlob,
                                                          false,
                                                          CancellationToken.None)
                                 .ConfigureAwait(false);

    Console.WriteLine($"taskId: {task.Single().TaskId}");

    await eventsService.WaitForBlobsAsync(session,
                                          new List<BlobInfo>([result]))
                       .ConfigureAwait(false);

    var download = await blobService.DownloadBlobAsync(result,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false);
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

  public static async Task<int> Main(string[] args)
  {
    // Define the options for the application with their description and default value
    var filePath = new Option<string>("--filepath",
                                      description: "FilePath to the zip file.",
                                      getDefaultValue: () => "library.zip");

    // Describe the application and its purpose
    var rootCommand = new RootCommand("Hello World demo for ArmoniK Extension.\n");

    // Add the options to the parser
    rootCommand.AddOption(filePath);

    // Configure the handler to call the function that will do the work
    rootCommand.SetHandler(RunAsync,
                           filePath);

    // Parse the command line parameters and call the function that represents the application
    return await rootCommand.InvokeAsync(args)
                            .ConfigureAwait(false);
  }
}
