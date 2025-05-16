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


using ArmoniK.Api.gRPC.V1.HealthChecks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Health;

using Grpc.Core;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Helpers;

namespace Tests.Services;

public class HealthChecksServiceTests
{
    [Test]
    public async Task CreateHealthCheck_ReturnsNewHealth()
    {
        var mockCallInvoker = new Mock<CallInvoker>();
        var responseAsync = new Health
        {
            Name = "TestService",
            Message = "Service is healthy",
            Status = ArmoniK.Extension.CSharp.Client.Common.Domain.Health.HealthStatusEnum.Healthy
        };
        var healthResponse = new CheckHealthResponse
            {
                Services =
                {
                    new Health
                    {
                        Name = "TestService",
                        Message = "Service is healthy",
                        Status = ArmoniK.Extension.CSharp.Client.Common.Domain.Health.HealthStatusEnum.Healthy
                    }
                }
            };

        mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CheckHealthRequest, CheckHealthResponse>(healthResponse);


        var healthService = MockHelper.GetHealthCheckServiceMock(mockCallInvoker, responseAsync);
        var results = healthService.GetHealth(CancellationToken.None);
        var healthObjects = new List<Health>();
        await foreach (var health in results)
        {
            healthObjects.Add(health);
        }
        // Assert.IsNotNull(healthObjects);

    
        
        // // ClassicAssert.AreEqual("TestService", healthInfos[0].Name);
        // // ClassicAssert.AreEqual("Service is healthy", healthInfos[0].Message);
        // // ClassicAssert.AreEqual(ArmoniK.Extension.CSharp.Client.Common.Domain.Health.HealthStatusEnum.Healthy,
        // //                        healthInfos[0].Status);



    }
}