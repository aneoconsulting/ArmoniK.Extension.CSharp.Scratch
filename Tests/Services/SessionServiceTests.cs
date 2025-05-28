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
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using Moq;

using NUnit.Framework;

using Tests.Helpers;

namespace Tests.Services;

public class SessionServiceTests
{
  private readonly List<string> defaultPartitionsIds_ = new()
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
                                                      defaultPartitionsIds_[0],
                                                      TimeSpan.FromHours(1));
    defaultProperties_ = new Properties(configuration);
  }

  [Test]
  public async Task CreateSessionReturnsNewSessionWithId()
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
    var result = await sessionService.CreateSessionAsync(defaultTaskConfiguration_,
                                                         defaultPartitionsIds_)
                                     .ConfigureAwait(false);
    Assert.That(result.SessionId,
                Is.EqualTo("12345"));
  }

  [Test]
  public async Task CloseSessionCallsGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CloseSessionRequest, CloseSessionResponse>(new CloseSessionResponse());

    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);

    var sessionInfo = new SessionInfo(sessionId);

    await sessionService.CloseSessionAsync(sessionInfo)
                        .ConfigureAwait(false);

    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CloseSessionRequest, CloseSessionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<CloseSessionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }

  [Test]
  public async Task PauseSessionCallGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<PauseSessionRequest, PauseSessionResponse>(new PauseSessionResponse());
    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    var sessionInfo = new SessionInfo(sessionId);
    await sessionService.PauseSessionAsync(sessionInfo)
                        .ConfigureAwait(false);
    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<PauseSessionRequest, PauseSessionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<PauseSessionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }

  [Test]
  public async Task ResumeSessionCallGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<ResumeSessionRequest, ResumeSessionResponse>(new ResumeSessionResponse());
    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    var sessionInfo = new SessionInfo(sessionId);
    await sessionService.ResumeSessionAsync(sessionInfo)
                        .ConfigureAwait(false);
    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<ResumeSessionRequest, ResumeSessionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<ResumeSessionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }

  [Test]
  public async Task StopSubmissionCallGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<StopSubmissionRequest, StopSubmissionResponse>(new StopSubmissionResponse());
    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    var sessionInfo = new SessionInfo(sessionId);
    await sessionService.StopSubmissionAsync(sessionInfo)
                        .ConfigureAwait(false);
    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<StopSubmissionRequest, StopSubmissionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<StopSubmissionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }

  [Test]
  public async Task PurgeSessionCallGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";
    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<PurgeSessionRequest, PurgeSessionResponse>(new PurgeSessionResponse());
    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    var sessionInfo = new SessionInfo(sessionId);
    await sessionService.PurgeSessionAsync(sessionInfo)
                        .ConfigureAwait(false);
    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<PurgeSessionRequest, PurgeSessionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<PurgeSessionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }

  [Test]
  public async Task DeleteSessionCallGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";
    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<DeleteSessionRequest, DeleteSessionResponse>(new DeleteSessionResponse());
    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    var sessionInfo = new SessionInfo(sessionId);
    await sessionService.DeleteSessionAsync(sessionInfo)
                        .ConfigureAwait(false);
    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<DeleteSessionRequest, DeleteSessionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<DeleteSessionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }

  [Test]
  public async Task CancelSessionCallGrpcWithCorrectSessionId()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var sessionId       = "12345";
    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CancelSessionRequest, CancelSessionResponse>(new CancelSessionResponse());
    var sessionService = MockHelper.GetSessionServiceMock(defaultProperties_,
                                                          defaultTaskConfiguration_,
                                                          mockCallInvoker);
    var sessionInfo = new SessionInfo(sessionId);
    await sessionService.CancelSessionAsync(sessionInfo)
                        .ConfigureAwait(false);
    mockCallInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CancelSessionRequest, CancelSessionResponse>>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CallOptions>(),
                                                             It.Is<CancelSessionRequest>(req => req.SessionId == sessionId)),
                           Times.Once);
  }
}
