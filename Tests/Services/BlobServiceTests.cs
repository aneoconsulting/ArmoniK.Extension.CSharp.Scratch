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
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Configuration;
using Tests.Helpers;

using Empty = ArmoniK.Api.gRPC.V1.Empty;

namespace Tests.Services;

public class BlobServiceTests
{
  [Test]
  public async Task CreateBlob_ReturnsNewBlobInfo()
  {
    var client = new MockedArmoniKClient();

    var responseAsync = new CreateResultsMetaDataResponse
                        {
                          Results =
                          {
                            new ResultRaw
                            {
                              CompletedAt = DateTime.UtcNow.ToTimestamp(), // Use UtcNow for consistency
                              Status      = ResultStatus.Created,
                              Name        = "blobName",
                              ResultId    = "blodId",
                              SessionId   = "sessionId",
                            },
                          },
                        };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var results = client.BlobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                              ["blobName"]);

    var blobInfos = await results.ToListAsync();
    ClassicAssert.AreEqual("blobName",
                           blobInfos[0].BlobName);
  }


  [Test]
  public async Task CreateBlob_WithName_ReturnsNewBlobInfo()
  {
    var client = new MockedArmoniKClient();
    var name   = "blobName";

    var responseAsync = new CreateResultsMetaDataResponse
                        {
                          Results =
                          {
                            new ResultRaw
                            {
                              CompletedAt = DateTime.Now.ToUniversalTime()
                                                    .ToTimestamp(),
                              Status    = ResultStatus.Created,
                              Name      = name,
                              ResultId  = "blodId",
                              SessionId = "sessionId",
                            },
                          },
                        };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var result = client!.BlobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                              [name]);

    var blobInfos = await result.ToListAsync();

    ClassicAssert.AreEqual("sessionId",
                           blobInfos[0].SessionId);
    ClassicAssert.AreEqual(name,
                           blobInfos[0].BlobName);
  }

  [Test]
  public async Task CreateBlobAsync_WithContent_CreatesBlobAndUploadsContent()
  {
    var client = new MockedArmoniKClient();
    var name   = "blobName";
    var contents = new ReadOnlyMemory<byte>(Enumerable.Range(1,
                                                             20)
                                                      .Select(x => (byte)x)
                                                      .ToArray());

    var serviceConfigurationResponse = new ResultsServiceConfigurationResponse
                                       {
                                         DataChunkMaxSize = 500,
                                       };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfigurationResponse);

    var metadataCreationResponse = new CreateResultsMetaDataResponse
                                   {
                                     Results =
                                     {
                                       new ResultRaw
                                       {
                                         CompletedAt = DateTime.Now.ToUniversalTime()
                                                               .ToTimestamp(),
                                         Status    = ResultStatus.Created,
                                         Name      = name,
                                         ResultId  = "blodId",
                                         SessionId = "sessionId",
                                       },
                                     },
                                   };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(metadataCreationResponse);

    var createResultResponse = new CreateResultsResponse
                               {
                                 Results =
                                 {
                                   new ResultRaw
                                   {
                                     CompletedAt = DateTime.Now.ToUniversalTime()
                                                           .ToTimestamp(),
                                     Status    = ResultStatus.Created,
                                     Name      = name,
                                     ResultId  = "blodId",
                                     SessionId = "sessionId",
                                   },
                                 },
                               };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(createResultResponse);

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();

    var responseTask = new UploadResultDataResponse
                       {
                         Result = new ResultRaw
                                  {
                                    Name     = "anyResult",
                                    ResultId = "anyResultId",
                                  },
                       };

    client.CallInvokerMock.SetupAsyncClientStreamingCall(responseTask,
                                                         mockStream.Object);

    var result = await client.BlobService.CreateBlobAsync(new SessionInfo("sessionId"),
                                                          name,
                                                          contents);

    ClassicAssert.AreEqual("sessionId",
                           result.SessionId);
    ClassicAssert.AreEqual(name,
                           result.BlobName);
  }

  [Test]
  public async Task CreateBlobAsync_WithBigContent_CreatesBlobAndUploadsContent()
  {
    var client = new MockedArmoniKClient();
    var name   = "blobName";
    var contents = new ReadOnlyMemory<byte>(Enumerable.Range(1,
                                                             500)
                                                      .Select(x => (byte)x)
                                                      .ToArray());

    var serviceConfigurationResponse = new ResultsServiceConfigurationResponse
                                       {
                                         DataChunkMaxSize = 20,
                                       };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfigurationResponse);

    var metadataCreationResponse = new CreateResultsMetaDataResponse
                                   {
                                     Results =
                                     {
                                       new ResultRaw
                                       {
                                         CompletedAt = DateTime.Now.ToUniversalTime()
                                                               .ToTimestamp(),
                                         Status    = ResultStatus.Created,
                                         Name      = name,
                                         ResultId  = "blodId",
                                         SessionId = "sessionId",
                                       },
                                     },
                                   };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(metadataCreationResponse);

    var createResultResponse = new CreateResultsResponse
                               {
                                 Results =
                                 {
                                   new ResultRaw
                                   {
                                     CompletedAt = DateTime.Now.ToUniversalTime()
                                                           .ToTimestamp(),
                                     Status    = ResultStatus.Created,
                                     Name      = name,
                                     ResultId  = "blodId",
                                     SessionId = "sessionId",
                                   },
                                 },
                               };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(createResultResponse);

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();

    var responseTask = new UploadResultDataResponse
                       {
                         Result = new ResultRaw
                                  {
                                    Name     = "anyResult",
                                    ResultId = "anyResultId",
                                  },
                       };

    client.CallInvokerMock.SetupAsyncClientStreamingCall(responseTask,
                                                         mockStream.Object);

    var result = await client.BlobService.CreateBlobAsync(new SessionInfo("sessionId"),
                                                          name,
                                                          contents);

    ClassicAssert.AreEqual("sessionId",
                           result.SessionId);
    ClassicAssert.AreEqual(name,
                           result.BlobName);
  }
}
