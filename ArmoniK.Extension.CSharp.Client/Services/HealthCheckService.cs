// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

using ArmoniK.Api.gRPC.V1.HealthChecks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Health;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using HealthStatusEnum = ArmoniK.Extension.CSharp.Client.Common.Domain.Health.HealthStatusEnum;

namespace ArmoniK.Extension.CSharp.Client.Services;

internal class HealthCheckService : IHealthCheckService
{
  private readonly ObjectPool<ChannelBase>     _channelPool;
  private readonly ILogger<HealthCheckService> _logger;

  public HealthCheckService(ObjectPool<ChannelBase> channel,
                            ILoggerFactory          loggerFactory)
  {
    _channelPool = channel;
    _logger      = loggerFactory.CreateLogger<HealthCheckService>();
  }

  public async IAsyncEnumerable<Health> GetHealth([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await using var channel = await _channelPool.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var healthClient = new HealthChecksService.HealthChecksServiceClient(channel);

    var healthResponse = await healthClient.CheckHealthAsync(new CheckHealthRequest());

    foreach (var health in healthResponse.Services)
    {
      yield return new Health
                   {
                     Name    = health.Name,
                     Message = health.Message,
                     Status  = (HealthStatusEnum)health.Healthy,
                   };
    }
  }
}
