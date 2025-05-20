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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Logging;

namespace LibraryExample;

public class Worker : IWorker
{
  public async Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                                         ILogger           logger,
                                         CancellationToken cancellationToken)
  {
    var resultId = taskHandler.ExpectedResults.Single();

    logger.LogWarning("Sending the following resultId: {resultId}",
                      resultId);
    await taskHandler.SendResult(resultId,
                                 Encoding.ASCII.GetBytes($"World_ {resultId}"))
                     .ConfigureAwait(false);
    return new Output
           {
             Ok = new Empty(),
           };
  }
}
