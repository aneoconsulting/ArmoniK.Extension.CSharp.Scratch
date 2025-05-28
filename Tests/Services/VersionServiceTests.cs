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

using ArmoniK.Api.gRPC.V1.Versions;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Helpers;

namespace Tests.Services;

public class VersionServiceTests
{
  [Test]
  public async Task GetVersionAsyncShouldReturnVersion()
  {
    var response = new ListVersionsResponse
                   {
                     Api  = "1.0.0",
                     Core = "1.0.0",
                   };

    var mockCallInvoker = new Mock<CallInvoker>();
    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<ListVersionsRequest, ListVersionsResponse>(response);


    var versionsService = mockCallInvoker.GetVersionsServiceMock();
    var result          = await versionsService.GetVersionsAsync(CancellationToken.None);
    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.Not.Null);
                      Assert.That(result.Api,
                                  Is.EqualTo("1.0.0"));
                      Assert.That(result.Core,
                                  Is.EqualTo("1.0.0"));
                    });
  }
}
