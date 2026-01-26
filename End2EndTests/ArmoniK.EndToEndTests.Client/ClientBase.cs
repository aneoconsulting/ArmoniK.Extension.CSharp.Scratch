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

using ArmoniK.Extensions.CSharp.Client;
using ArmoniK.Extensions.CSharp.Client.Common;
using ArmoniK.Extensions.CSharp.Client.Handles;
using ArmoniK.Extensions.CSharp.Client.Services;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Common.Library;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Client;

public class ClientBase
{
  protected const string             Partition = "dllworker";
  protected       Properties?        Properties        { get; set; }
  protected       TaskConfiguration? TaskConfiguration { get; set; }
  protected       SessionHandle?     SessionHandle     { get; set; }
  protected       ArmoniKClient?     Client            { get; set; }
  protected       DynamicLibrary?    WorkerLibrary     { get; set; }

  protected async Task SetupBaseAsync(string workerName)
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
    WorkerLibrary = new DynamicLibrary
                    {
                      Symbol      = "ArmoniK.EndToEndTests.Worker.Tests." + workerName,
                      LibraryPath = @"ArmoniK.EndToEndTests.Worker/1.0.0-100/ArmoniK.EndToEndTests.Worker.dll",
                    };

    Client = new ArmoniKClient(properties,
                               loggerFactory);

    SessionHandle = await Client.CreateSessionAsync([Partition],
                                                    TaskConfiguration,
                                                    true)
                                .ConfigureAwait(false);

    var filePath = Path.Join(AppContext.BaseDirectory,
                             @"..\..\..\..\..\packages\ArmoniK.EndToEndTests.Worker-v1.0.0-100.zip");
    await Client.BlobService.SendDllBlobAsync(SessionHandle,
                                              WorkerLibrary,
                                              filePath,
                                              false,
                                              CancellationToken.None)
                .ConfigureAwait(false);
  }

  protected async Task TearDownBaseAsync()
  {
    await SessionHandle!.DisposeAsync()
                        .ConfigureAwait(false);
    await Client!.SessionService.PurgeSessionAsync(SessionHandle);
    await Client.SessionService.DeleteSessionAsync(SessionHandle);
    Properties        = null;
    TaskConfiguration = null;
    SessionHandle     = null;
    Client            = null;
  }
}
