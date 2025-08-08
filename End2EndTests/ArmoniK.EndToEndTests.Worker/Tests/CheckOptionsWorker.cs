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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.DllCommon;
using ArmoniK.Extension.CSharp.Worker;

namespace ArmoniK.EndToEndTests.Worker.Tests;

using Error = Output.Types.Error;

public class CheckOptionsWorker : ILibraryWorker
{
  public Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                                   ILibraryLoader    libraryLoader,
                                   string            libraryContext,
                                   CancellationToken cancellationToken)
  {
    Error? error = null;

    try
    {
      if (taskHandler.TaskOptions.Options.Count   != 3 || taskHandler.TaskOptions.Options["key1"] != "value1" || taskHandler.TaskOptions.Options["key2"] != "value2" ||
          taskHandler.TaskOptions.Options["key3"] != "value3")
      {
        error = new Error
                {
                  Details = "Bad TaskOptions",
                };
      }
    }
    catch (Exception e)
    {
      error = new Error
              {
                Details = e.Message,
              };
    }

    var result = new CreateResultsRequest.Types.ResultCreate
                 {
                   Name = "Result",
                 };
    taskHandler.CreateResultsAsync([result]);
    taskHandler.SendResult("Result",
                           [42]);

    if (error == null)
    {
      return Task.FromResult(new Output
                             {
                               Ok = new Empty(),
                             });
    }

    return Task.FromResult(new Output
                           {
                             Error = error,
                           });
  }
}
