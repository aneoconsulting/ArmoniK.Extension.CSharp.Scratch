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

using ArmoniK.Api.gRPC.V1.HealthChecks;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Services;

public class HealthChecksServiceTests
{
  [Test]
  public async Task GetHealthReturnsHealthObject()
  {
    var client = new MockedArmoniKClient();

    var serviceHealth = new CheckHealthResponse.Types.ServiceHealth
                        {
                          Name    = "Hello",
                          Message = "It is healthy",
                          Healthy = HealthStatusEnum.Healthy,
                        };

    var healthResponse = new CheckHealthResponse();
    healthResponse.Services.Add(serviceHealth);

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CheckHealthRequest, CheckHealthResponse>(healthResponse);

    var results = await client.HealthCheckService.GetHealthAsync(CancellationToken.None)
                              .ToListAsync()
                              .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(results.Select(r => r.Name),
                                  Is.EqualTo(new[]
                                             {
                                               "Hello",
                                             }));
                      Assert.That(results.Select(r => r.Message),
                                  Is.EqualTo(new[]
                                             {
                                               "It is healthy",
                                             }));
                      Assert.That(results.Select(r => r.Status),
                                  Is.EqualTo(new[]
                                             {
                                               ArmoniK.Extension.CSharp.Client.Common.Domain.Health.HealthStatusEnum.Healthy,
                                             }));

                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<CheckHealthRequest, CheckHealthResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.IsAny<CheckHealthRequest>()),
                                                    Times.Once,
                                                    "AsyncUnaryCall should be called exactly once");
                    });
  }
}
