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

using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Worker.Tests;

public class FizzBuzzWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public async Task<TaskResult> ExecuteAsync(SdkTaskHandler    taskHandler,
                                             ILogger           logger,
                                             CancellationToken cancellationToken)
  {
    var valueString = taskHandler.GetStringDependency("value");
    var result      = valueString + " -> ";

    if (!int.TryParse(valueString,
                      out var value))
    {
      return TaskResult.Failure("Invalid 'value' input: " + valueString);
    }

    if (value > 0)
    {
      if (value % 3 == 0)
      {
        result += "Fizz";
      }

      if (value % 5 == 0)
      {
        result += "Buzz";
      }

      if (!result.Contains("Fizz") && !result.Contains("Buzz"))
      {
        result += value;
      }
    }
    else
    {
      result += "invalid input";
    }

    // Send the input as results as is.
    await taskHandler.SendResultByNameAsync("result",
                                            Encoding.ASCII.GetBytes(result))
                     .ConfigureAwait(false);
    return TaskResult.Success;
  }
}
