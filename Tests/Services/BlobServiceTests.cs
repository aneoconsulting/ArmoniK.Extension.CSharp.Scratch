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
using ArmoniK.Extension.CSharp.Client.Common.Enum;

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
  public async Task CreateBlobAsyncWithContentCreatesBlobAndUploadsContent()
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
  public async Task CreateBlobAsyncWithBigContentCreatesBlobAndUploadsContent()
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
  public async Task GetBlobStateAsyncWithNonExistentBlobReturnsNotFoundStatus()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var response = new GetResultResponse
                   {
                     Result = new ResultRaw
                              {
                                Status      = ResultStatus.Notfound,
                                ResultId    = "nonExistentBlobId",
                                SessionId   = "sessionId",
                                Name        = "nonExistentBlob",
                                CreatedAt   = DateTime.UtcNow.ToTimestamp(),
                                CompletedAt = DateTime.UtcNow.ToTimestamp(),
                              },
                   };

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<GetResultRequest, GetResultResponse>(response);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var blobInfo = new BlobInfo
                   {
                     BlobName  = "nonExistentBlob",
                     BlobId    = "nonExistentBlobId",
                     SessionId = "sessionId",
                   };

    var result = await blobService.GetBlobStateAsync(blobInfo);

    ClassicAssert.AreEqual(BlobStatus.Notfound,
                           result.Status);
    ClassicAssert.AreEqual(response.Result.ResultId,
                           result.BlobId);
    ClassicAssert.AreEqual(response.Result.SessionId,
                           result.SessionId);
    ClassicAssert.AreEqual(response.Result.Name,
                           result.BlobName);
    ClassicAssert.AreEqual(response.Result.CreatedAt.ToDateTime(),
                           result.CreateAt);
    ClassicAssert.AreEqual(response.Result.CompletedAt.ToDateTime(),
                           result.CompletedAt);
  }

  //TODO Understand why we have a null reference
  // [Test]
  // public async Task UploadBlobAsync_WithValidContent_UploadsBlob()
  // {
  //   var mockCallInvoker = new Mock<CallInvoker>();

  //   var contents = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3, 4, 5 });

  //   var response = new UploadResultDataResponse()
  //   {
  //     Result = new ResultRaw
  //     {
  //       Name = "validBlob",
  //       ResultId = "validBlobId",
  //       SessionId = "sessionId",
  //       Status = ResultStatus.Created,
  //       CompletedAt = DateTime.Now.ToUniversalTime()
  //                                                   .ToTimestamp(),
  //       CreatedAt = DateTime.Now.ToUniversalTime()
  //                                                   .ToTimestamp(),
  //     }
  //   };

  //   // Configuration du mock pour l'appel unary
  //   mockCallInvoker.SetupAsyncUnaryCallInvokerMock<UploadResultDataRequest, UploadResultDataResponse>(response);

  //   var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

  //   var blobInfo = new BlobInfo
  //   {
  //     BlobName = "validBlob",
  //     BlobId = "validBlobId",
  //     SessionId = "sessionId",
  //   };

  //   // Appel de la méthode UploadBlobAsync
  //   await blobService.UploadBlobAsync(blobInfo, contents, CancellationToken.None);

  //   // Vérification que le blob a été téléversé sans erreur
  //   Assert.Pass();
  // }
  [Test]
  public async Task DownloadBlobWithChunksAsyncWithValidBlobReturnsBlobChunks()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var chunks = new List<DownloadResultDataResponse>
                 {
                   new()
                   {
                     DataChunk = ByteString.CopyFrom(1,
                                                     2,
                                                     3),
                   },
                   new()
                   {
                     DataChunk = ByteString.CopyFrom(4,
                                                     5,
                                                     6),
                   },
                 };

    mockCallInvoker.SetupAsyncServerStreamingCallInvokerMock<DownloadResultDataRequest, DownloadResultDataResponse>(chunks[0]);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var blobInfo = new BlobInfo
                   {
                     BlobName  = "validBlob",
                     BlobId    = "validBlobId",
                     SessionId = "sessionId",
                   };

    var resultChunks = new List<byte[]>();
    await foreach (var chunk in blobService.DownloadBlobWithChunksAsync(blobInfo))
    {
      resultChunks.Add(chunk);
    }

    ClassicAssert.AreEqual(1,
                           resultChunks.Count);
    CollectionAssert.AreEqual(new byte[]
                              {
                                1,
                                2,
                                3,
                              },
                              resultChunks[0]);
  }

  [Test]
  public async Task ListBlobsAsyncWithPaginationReturnsBlobs()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var response = new ListResultsResponse
                   {
                     Results =
                     {
                       new ResultRaw
                       {
                         ResultId    = "blob1Id",
                         Name        = "blob1",
                         SessionId   = "sessionId",
                         Status      = ResultStatus.Completed,
                         CreatedAt   = DateTime.UtcNow.ToTimestamp(),
                         CompletedAt = DateTime.UtcNow.ToTimestamp(),
                       },
                       new ResultRaw
                       {
                         ResultId    = "blob2Id",
                         Name        = "blob2",
                         SessionId   = "sessionId",
                         Status      = ResultStatus.Completed,
                         CreatedAt   = DateTime.UtcNow.ToTimestamp(),
                         CompletedAt = DateTime.UtcNow.ToTimestamp(),
                       },
                     },
                     Total = 2,
                   };

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);

    var blobPagination = new BlobPagination
                         {
                           Page          = 1,
                           PageSize      = 10,
                           SortDirection = SortDirection.Asc,
                           Filter        = new Filters(),
                         };

    var resultBlobs = new List<BlobPage>();
    await foreach (var blobPage in blobService.ListBlobsAsync(blobPagination))
    {
      resultBlobs.Add(blobPage);
    }

    ClassicAssert.AreEqual(2,
                           resultBlobs.Count);
    ClassicAssert.AreEqual("blob1",
                           resultBlobs[0].BlobDetails.BlobName);
    ClassicAssert.AreEqual("blob2",
                           resultBlobs[1].BlobDetails.BlobName);
  }
}
