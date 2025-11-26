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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Services;

public class EventsServiceTests
{
  [Test]
  public async Task CreateSessionReturnsNewSessionWithId()
  {
    var client = new MockedArmoniKClient();
    var responses = new EventSubscriptionResponse
                    {
                      SessionId = "1234",
                      NewResult = new EventSubscriptionResponse.Types.NewResult
                                  {
                                    ResultId = "1234",
                                    Status   = ResultStatus.Completed,
                                  },
                    };

    client.CallInvokerMock.SetupAsyncServerStreamingCallInvokerMock<EventSubscriptionRequest, EventSubscriptionResponse>(responses);

    var blobId    = "1234";
    var sessionId = "sessionId";

    var sessionInfo = new SessionInfo(sessionId);
    var blobInfos = new[]
                    {
                      new BlobInfo
                      {
                        BlobName  = "",
                        BlobId    = blobId,
                        SessionId = sessionId,
                      },
                    };
    await client.EventsService.WaitForBlobsAsync(sessionInfo,
                                                 blobInfos)
                .ConfigureAwait(false);

    client.CallInvokerMock.Verify(x => x.AsyncServerStreamingCall(It.IsAny<Method<EventSubscriptionRequest, EventSubscriptionResponse>>(),
                                                                  It.IsAny<string>(),
                                                                  It.IsAny<CallOptions>(),
                                                                  It.IsAny<EventSubscriptionRequest>()),
                                  Times.Once,
                                  "AsyncServerStreamingCall should be called exactly once");
  }
}
