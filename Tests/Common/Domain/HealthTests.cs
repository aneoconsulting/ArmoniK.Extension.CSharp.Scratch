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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Health;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common.Domain;

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

    ClassicAssert.AreEqual("ComponentName",
                           health.Name);
    ClassicAssert.AreEqual("Component is healthy",
                           health.Message);
    ClassicAssert.AreEqual(HealthStatusEnum.Healthy,
                           health.Status);
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

    ClassicAssert.AreEqual(status,
                           health.Status);

    switch (status)
    {
      case HealthStatusEnum.Unspecified:
      case HealthStatusEnum.Healthy:
      case HealthStatusEnum.Degraded:
      case HealthStatusEnum.Unhealthy:

        var grpcStatus = status.ToGrpcStatus();
        ClassicAssert.AreEqual(status.ToString(),
                               grpcStatus.ToString());

        var internalStatus = grpcStatus.ToInternalStatus();
        ClassicAssert.AreEqual(status,
                               internalStatus);
        break;
      default:
        Assert.Throws<ArgumentOutOfRangeException>(() =>
                                                   {
                                                     status.ToGrpcStatus();
                                                   });
        break;
    }
  }
}
