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
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Services;

public class SessionServiceTests
{
  private readonly List<string> defaultPartitionsIds_ = ["subtasking"];

  [Test]
  public async Task CreateSessionReturnsNewSessionWithId()
  {
    var client = new MockedArmoniKClient();
    var createSessionReply = new CreateSessionReply
                             {
                               SessionId = "12345",
                             };
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateSessionRequest, CreateSessionReply>(createSessionReply);

    var result = await client.SessionService.CreateSessionAsync(client.TaskOptions,
                                                                defaultPartitionsIds_)
                             .ConfigureAwait(false);
    Assert.That(result.SessionId,
                Is.EqualTo("12345"));
  }

  [Test]
  public void CompareTwoSessionsWithSameIdReturnsTrue()
  {
    var session1 = new SessionInfo("12345");
    var session2 = new SessionInfo("12345");

    Assert.That(session1,
                Is.EqualTo(session2));
  }

  [Test]
  public async Task CloseSessionCallsGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CloseSessionRequest, CloseSessionResponse>(new CloseSessionResponse());

    var sessionInfo = new SessionInfo(sessionId);

    await client.SessionService.CloseSessionAsync(sessionInfo)
                .ConfigureAwait(false);

    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CloseSessionRequest, CloseSessionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<CloseSessionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }

  [Test]
  public async Task PauseSessionCallGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<PauseSessionRequest, PauseSessionResponse>(new PauseSessionResponse());
    var sessionInfo = new SessionInfo(sessionId);
    await client.SessionService.PauseSessionAsync(sessionInfo)
                .ConfigureAwait(false);
    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<PauseSessionRequest, PauseSessionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<PauseSessionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }

  [Test]
  public async Task ResumeSessionCallGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ResumeSessionRequest, ResumeSessionResponse>(new ResumeSessionResponse());
    var sessionInfo = new SessionInfo(sessionId);
    await client.SessionService.ResumeSessionAsync(sessionInfo)
                .ConfigureAwait(false);
    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<ResumeSessionRequest, ResumeSessionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<ResumeSessionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }

  [Test]
  public async Task StopSubmissionCallGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<StopSubmissionRequest, StopSubmissionResponse>(new StopSubmissionResponse());
    var sessionInfo = new SessionInfo(sessionId);
    await client.SessionService.StopSubmissionAsync(sessionInfo)
                .ConfigureAwait(false);
    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<StopSubmissionRequest, StopSubmissionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<StopSubmissionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }

  [Test]
  public async Task PurgeSessionCallGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<PurgeSessionRequest, PurgeSessionResponse>(new PurgeSessionResponse());
    var sessionInfo = new SessionInfo(sessionId);
    await client.SessionService.PurgeSessionAsync(sessionInfo)
                .ConfigureAwait(false);
    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<PurgeSessionRequest, PurgeSessionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<PurgeSessionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }

  [Test]
  public async Task DeleteSessionCallGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<DeleteSessionRequest, DeleteSessionResponse>(new DeleteSessionResponse());
    var sessionInfo = new SessionInfo(sessionId);
    await client.SessionService.DeleteSessionAsync(sessionInfo)
                .ConfigureAwait(false);
    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<DeleteSessionRequest, DeleteSessionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<DeleteSessionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }

  [Test]
  public async Task CancelSessionCallGrpcWithCorrectSessionId()
  {
    var client    = new MockedArmoniKClient();
    var sessionId = "12345";
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CancelSessionRequest, CancelSessionResponse>(new CancelSessionResponse());
    var sessionInfo = new SessionInfo(sessionId);
    await client.SessionService.CancelSessionAsync(sessionInfo)
                .ConfigureAwait(false);
    client.CallInvokerMock.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CancelSessionRequest, CancelSessionResponse>>(),
                                                                    It.IsAny<string>(),
                                                                    It.IsAny<CallOptions>(),
                                                                    It.Is<CancelSessionRequest>(req => req.SessionId == sessionId)),
                                  Times.Once);
  }
}
