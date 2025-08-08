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

using ArmoniK.EndToEndTests.Common;
using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Client.Tests;

public class CheckOptionsClient : ClientBaseTest<CheckOptionsClient>
{
  public CheckOptionsClient(IConfiguration configuration,
                            ILoggerFactory loggerFactory)
    : base(configuration,
           loggerFactory)
  {
  }

  [EntryPoint]
  public override async Task EntryPoint()
  {
    var taskConfiguration = InitializeTaskConfiguration();
    var client = new ArmoniKClient(Properties,
                                   LoggerFactory,
                                   taskConfiguration);

    Log.LogInformation("Configure taskOptions");
    taskConfiguration.Options["key1"] = "value1";
    taskConfiguration.Options["key2"] = "value2";
    taskConfiguration.Options["key3"] = "value3";

    var session = await client.SessionService.CreateSessionAsync(taskConfiguration,
                                                                 [""],
                                                                 CancellationToken.None)
                              .ConfigureAwait(false);
    var payload = await client.BlobService.CreateBlobAsync(session,
                                                           "Payload",
                                                           Encoding.ASCII.GetBytes("Hello"))
                              .ConfigureAwait(false);

    var results = client.BlobService.CreateBlobsMetadataAsync(session,
                                                              ["Result"]);

    var blobInfos = await results.ToListAsync()
                                 .ConfigureAwait(false);

    var result = blobInfos[0];

    var task = new TaskNode
               {
                 TaskOptions     = taskConfiguration,
                 Session         = session,
                 Payload         = payload,
                 ExpectedOutputs = blobInfos,
               };
    var tasks = await client.TasksService.SubmitTasksAsync(session,
                                                           [task]);

    await client.EventsService.WaitForBlobsAsync(session,
                                                 blobInfos,
                                                 CancellationToken.None);
    var download = await client.BlobService.DownloadBlobAsync(result,
                                                              CancellationToken.None)
                               .ConfigureAwait(false);
  }
}
