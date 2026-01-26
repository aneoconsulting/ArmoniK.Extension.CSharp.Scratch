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

using ArmoniK.Extensions.CSharp.Worker.Interfaces;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Task;

using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Worker.Tests;

public class FailingTaskWorker : IWorker
{
  public Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => Task.FromResult(HealthCheckResult.Healthy());

  public Task<TaskResult> ExecuteAsync(ISdkTaskHandler   taskHandler,
                                       ILogger           logger,
                                       CancellationToken cancellationToken)
    => Task.FromResult(TaskResult.Failure("Intentional failure for testing purposes"));
}
