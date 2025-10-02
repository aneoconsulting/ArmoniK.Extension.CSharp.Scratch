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
using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Worker.Tests;

public class PriorityWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public async Task<Output> ExecuteAsync(UserTaskHandler   taskHandler,
                                         ILogger           logger,
                                         CancellationToken cancellationToken)
  {
    try
    {
      var priority  = taskHandler.GetIntDependency("Priority");
      var strResult = $"Payload is {priority} and TaskOptions.Priority is {taskHandler.TaskOptions.Priority}";

      var name = taskHandler.Outputs.Single()
                            .Value;
      logger.LogInformation($"Sending result: {strResult}. Task Id: {taskHandler.TaskId}");
      await taskHandler.SendResult(name,
                                   Encoding.ASCII.GetBytes(strResult))
                       .ConfigureAwait(false);
      return new Output
             {
               Ok = new Empty(),
             };
    }
    catch (Exception ex)
    {
      logger.LogError(ex,
                      ex.Message);
      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message,
                       },
             };
    }
  }
}
