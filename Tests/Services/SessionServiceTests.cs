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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Helpers;

namespace Tests.Services;

public class SessionServiceTests
{
  private readonly List<string> defaultPartitionsIds = new()
                                                       {
                                                         "subtasking",
                                                       };

  private readonly Properties        defaultProperties_;
  private readonly TaskConfiguration defaultTaskConfiguration_;

  public SessionServiceTests()
  {
    var configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                  .AddJsonFile("appsettings.tests.json",
                                                               false)
                                                  .AddEnvironmentVariables()
                                                  .Build();

    defaultTaskConfiguration_ = new TaskConfiguration(2,
                                                      1,
                                                      defaultPartitionsIds[0],
                                                      TimeSpan.FromHours(1));
    defaultProperties_ = new Properties(configuration);
  }

  [Test]
  public async Task CreateSession_ReturnsNewSessionWithId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var createSessionReply = new CreateSessionReply
                             {
                               SessionId = "12345",
                             };

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateSessionRequest, CreateSessionReply>(createSessionReply);

    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    // Act
    var result = await sessionService.CreateSessionAsync(defaultTaskConfiguration_,
                                                         defaultPartitionsIds);
    // Assert
    ClassicAssert.AreEqual("12345",
                           result.SessionId);
  }
}
