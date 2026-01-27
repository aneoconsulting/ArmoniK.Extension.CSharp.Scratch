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

using System.CommandLine;
using System.Text;

using ArmoniK.Extensions.CSharp.Client;
using ArmoniK.Extensions.CSharp.Client.Common;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Services;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Common.Library;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Events;
using Serilog.Extensions.Logging;

namespace UsageExample;

internal class Program
{
  internal static async Task RunAsync(string filePath,
                                      string name)
  {
    Log.Logger = new LoggerConfiguration().MinimumLevel.Override("Microsoft",
                                                                 LogEventLevel.Information)
                                          .Enrich.FromLogContext()
                                          .WriteTo.Console()
                                          .CreateLogger();

    var factory = new LoggerFactory([new SerilogLoggerProvider(Log.Logger)],
                                    new LoggerFilterOptions().AddFilter("Grpc",
                                                                        LogLevel.Error));

    var logger = factory.CreateLogger<Program>();

    var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                            .AddJsonFile("appsettings.json",
                                                         false)
                                            .AddEnvironmentVariables();

    var configuration = builder.Build();

    var defaultTaskOptions = new TaskConfiguration(2,
                                                   1,
                                                   "dllworker",
                                                   TimeSpan.FromHours(1));

    var props = new Properties(configuration);

    var client = new ArmoniKClient(props,
                                   factory);

    var dynamicLib = new DynamicLibrary
                     {
                       Symbol      = "DynamicWorkerExample.HelloWorker",
                       LibraryPath = "DynamicWorkerExample/1.0.0.0/DynamicWorkerExample.dll",
                     };

    var sessionHandle = await client.CreateSessionAsync(["dllworker"],
                                                        defaultTaskOptions,
                                                        false)
                                    .ConfigureAwait(false);

    logger.LogInformation("sessionId: {SessionId}",
                          sessionHandle.SessionInfo.SessionId);

    var blobService = client.BlobService;

    var tasksService = client.TasksService;

    var eventsService = client.EventsService;

    await blobService.SendDllBlobAsync(sessionHandle,
                                       dynamicLib,
                                       filePath,
                                       false,
                                       CancellationToken.None)
                     .ConfigureAwait(false);
    logger.LogInformation("libraryId: {BlobId}",
                          dynamicLib.LibraryBlobId);

    var task = new TaskDefinition().WithLibrary(dynamicLib)
                                   .WithInput("name",
                                              BlobDefinition.FromString("name",
                                                                        name))
                                   .WithOutput("helloResult",
                                               BlobDefinition.CreateOutput("Result"))
                                   .WithTaskOptions(defaultTaskOptions);

    var taskHandle = await sessionHandle.SubmitAsync([task],
                                                     CancellationToken.None)
                                        .ConfigureAwait(false);

    BlobInfo resultBlobInfo = task.Outputs.Values.First()
                                  .BlobHandle!;
    logger.LogInformation("resultId: {ResultId}",
                          resultBlobInfo.BlobId);
    logger.LogInformation("taskId: {TaskId}",
                          ((TaskInfos)taskHandle.First()).TaskId);

    await eventsService.WaitForBlobsAsync(sessionHandle,
                                          [resultBlobInfo])
                       .ConfigureAwait(false);

    var download = await blobService.DownloadBlobAsync(resultBlobInfo,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false);
    var hello = Encoding.UTF8.GetString(download);

    logger.LogInformation("Downloaded: {Hello}",
                          hello);
  }

  public static async Task<int> Main(string[] args)
  {
    // Define the options for the application with their description and default value
    var name = new Option<string>("--name",
                                  description: "your name.",
                                  getDefaultValue: () => "unknown user");

    var filePath = new Option<string>("--filepath",
                                      description: "FilePath to the zip file.",
                                      getDefaultValue: () => @"..\..\..\..\DynamicWorkerExample\packages\DynamicWorkerExample-v1.0.0.0.zip");

    // Describe the application and its purpose
    var rootCommand = new RootCommand("Hello World demo for ArmoniK Extensions.CSharp.\n");

    // Add the options to the parser
    rootCommand.AddOption(filePath);
    rootCommand.AddOption(name);

    // Configure the handler to call the function that will do the work
    rootCommand.SetHandler(RunAsync,
                           filePath,
                           name);

    // Parse the command line parameters and call the function that represents the application
    return await rootCommand.InvokeAsync(args)
                            .ConfigureAwait(false);
  }
}
