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
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Helpers;

using Empty = ArmoniK.Api.gRPC.V1.Empty;

namespace Tests.Services;

public class BlobServiceTests
{
  [Test]
  public async Task CreateBlob_ReturnsNewBlobInfo()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

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

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var results = blobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                       new[]
                                                       {
                                                         "blobName",
                                                       });

    var blobInfos = await results.ToListAsync();
    ClassicAssert.AreEqual("blobName",
                           blobInfos[0].BlobName);
  }


  [Test]
  public async Task CreateBlob_WithName_ReturnsNewBlobInfo()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var name = "blobName";

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

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var result = blobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                      new[]
                                                      {
                                                        name,
                                                      });

    var blobInfos = await result.ToListAsync();

    ClassicAssert.AreEqual("sessionId",
                           blobInfos[0].SessionId);
    ClassicAssert.AreEqual(name,
                           blobInfos[0].BlobName);
  }

  [Test]
  public async Task CreateBlobAsync_WithContent_CreatesBlobAndUploadsContent()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var name = "blobName";
    var contents = new ReadOnlyMemory<byte>(Enumerable.Range(1,
                                                             20)
                                                      .Select(x => (byte)x)
                                                      .ToArray());

    var serviceConfigurationResponse = new ResultsServiceConfigurationResponse
                                       {
                                         DataChunkMaxSize = 500,
                                       };

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfigurationResponse);

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

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(metadataCreationResponse);

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

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(createResultResponse);

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();

    var responseTask = new UploadResultDataResponse
                       {
                         Result = new ResultRaw
                                  {
                                    Name     = "anyResult",
                                    ResultId = "anyResultId",
                                  },
                       };

    mockCallInvoker.SetupAsyncClientStreamingCall(responseTask,
                                                  mockStream.Object);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var result = await blobService.CreateBlobAsync(new SessionInfo("sessionId"),
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
    var mockCallInvoker = new Mock<CallInvoker>();

    var name = "blobName";
    var contents = new ReadOnlyMemory<byte>(Enumerable.Range(1,
                                                             500)
                                                      .Select(x => (byte)x)
                                                      .ToArray());

    var serviceConfigurationResponse = new ResultsServiceConfigurationResponse
                                       {
                                         DataChunkMaxSize = 20,
                                       };

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfigurationResponse);

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

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(metadataCreationResponse);

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

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(createResultResponse);

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();

    var responseTask = new UploadResultDataResponse
                       {
                         Result = new ResultRaw
                                  {
                                    Name     = "anyResult",
                                    ResultId = "anyResultId",
                                  },
                       };

    mockCallInvoker.SetupAsyncClientStreamingCall(responseTask,
                                                  mockStream.Object);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var result = await blobService.CreateBlobAsync(new SessionInfo("sessionId"),
                                                   name,
                                                   contents);

    ClassicAssert.AreEqual("sessionId",
                           result.SessionId);
    ClassicAssert.AreEqual(name,
                           result.BlobName);
  }

  [Test]
  public async Task CreateBlobAsync_ImportBlobData()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var sessionInfo = new SessionInfo("session1");
    var blobInfo = new BlobInfo
                   {
                     BlobId    = "blob1",
                     BlobName  = "myBlob",
                     SessionId = sessionInfo.SessionId,
                   };
    var opaqueId = new byte[]
                   {
                     1,
                     2,
                     3,
                   };
    var blobDescs = new List<KeyValuePair<BlobInfo, byte[]>>
                    {
                      new(blobInfo,
                          opaqueId),
                    };

    var expectedResponse = new ImportResultsDataResponse
                           {
                             Results =
                             {
                               new ResultRaw
                               {
                                 Name      = "myBlob",
                                 ResultId  = "blob1",
                                 Status    = ResultStatus.Completed,
                                 SessionId = sessionInfo.SessionId,
                                 OpaqueId  = ByteString.CopyFrom(opaqueId),
                                 CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                               },
                             },
                           };
    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<ImportResultsDataRequest, ImportResultsDataResponse>(expectedResponse);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);
    var result = await blobService.ImportBlobDataAsync(sessionInfo,
                                                       blobDescs);

    var blobState = result.Single();
    ClassicAssert.AreEqual("session1",
                           blobState.SessionId);
    ClassicAssert.AreEqual("blob1",
                           blobState.BlobId);
    ClassicAssert.AreEqual(BlobStatus.Completed,
                           blobState.Status);
    ClassicAssert.AreEqual("myBlob",
                           blobState.BlobName);
    ClassicAssert.AreEqual(opaqueId,
                           blobState.OpaqueId);
  }
}
