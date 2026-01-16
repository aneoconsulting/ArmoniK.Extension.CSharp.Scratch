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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Health;

using NUnit.Framework;

namespace Tests.Common.Domain;

[TestFixture]
public class HealthTests
{
  [Test]
  public void CreateHealthTest()
  {
    var health = new Health
                 {
                   Name    = "ComponentName",
                   Message = "Component is healthy",
                   Status  = HealthStatusEnum.Healthy,
                 };

    Assert.That(health.Name,
                Is.EqualTo("ComponentName"));
    Assert.That(health.Message,
                Is.EqualTo("Component is healthy"));
    Assert.That(health.Status,
                Is.EqualTo(HealthStatusEnum.Healthy));
  }

  [TestCase(HealthStatusEnum.Unspecified)]
  [TestCase(HealthStatusEnum.Healthy)]
  [TestCase(HealthStatusEnum.Degraded)]
  [TestCase(HealthStatusEnum.Unhealthy)]
  [TestCase((HealthStatusEnum)99)]
  public void TestHealthStatus(HealthStatusEnum status)
  {
    var health = new Health
                 {
                   Status = status,
                 };

    Assert.That(health.Status,
                Is.EqualTo(status));

    switch (status)
    {
      case HealthStatusEnum.Unspecified:
      case HealthStatusEnum.Healthy:
      case HealthStatusEnum.Degraded:
      case HealthStatusEnum.Unhealthy:

        var grpcStatus = status.ToGrpcStatus();
        Assert.That(grpcStatus.ToString(),
                    Is.EqualTo(status.ToString()));

        var internalStatus = grpcStatus.ToInternalStatus();
        Assert.That(internalStatus,
                    Is.EqualTo(status));
        break;
      default:
        Assert.That(() => status.ToGrpcStatus(),
                    Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        break;
    }
  }
}
