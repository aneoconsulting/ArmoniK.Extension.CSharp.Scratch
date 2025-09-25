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

using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.DllHelper;
using ArmoniK.Extension.CSharp.Client.DllHelper.Common;
using ArmoniK.Extension.CSharp.Client.Handles;
using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Client;

public class ClientBase
{
  protected const string                Partition = "dllworker";
  protected       Properties            Properties            { get; set; }
  protected       TaskConfiguration     TaskConfiguration     { get; set; }
  protected       SessionInfo           Session               { get; set; }
  protected       SessionHandle         SessionHandle         { get; set; }
  protected       DllBlob               DllBlob               { get; set; }
  protected       TaskLibraryDefinition TaskLibraryDefinition { get; set; }
  protected       ArmoniKClient         Client                { get; set; }

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
    SessionHandle = new SessionHandle(Session,
                                      Client);

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
                                                      workerName);
  }

  protected async Task TearDownBaseAsync()
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
}
