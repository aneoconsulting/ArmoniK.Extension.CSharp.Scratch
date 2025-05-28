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

using Tests.Helpers;

using Empty = ArmoniK.Api.gRPC.V1.Empty;

namespace Tests.Services;

public class BlobServiceTests
{
  [Test]
  public async Task CreateBlobReturnsNewBlobInfo()
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
                              ResultId    = "blobId",
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
    Assert.That(blobInfos,
                Is.EqualTo(new BlobInfo[]
                           {
                             new()
                             {
                               SessionId = "sessionId",
                               BlobName  = "blobName",
                               BlobId    = "blobId",
                             },
                           }));
  }


  [Test]
  public async Task CreateBlobWithNameReturnsNewBlobInfo()
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
                              ResultId  = "blobId",
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

    Assert.That(blobInfos,
                Is.EqualTo(new BlobInfo[]
                           {
                             new()
                             {
                               SessionId = "sessionId",
                               BlobName  = name,
                               BlobId    = "blobId",
                             },
                           }));
  }

// Assert.That(blobInfos.Select(b=> b.BlobName), Is.EqualTo((string[])new[] { "blobname" }));
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
    Assert.That(new[]
                {
                  result,
                }.Select(r => r.SessionId),
                Is.EqualTo(new[]
                           {
                             "sessionId",
                           }));
    Assert.That(new[]
                {
                  result,
                }.Select(r => r.BlobName),
                Is.EqualTo(new[]
                           {
                             name,
                           }));
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

    Assert.That(result.SessionId,
                Is.EqualTo("sessionId"));
    Assert.That(result.BlobName,
                Is.EqualTo(name));
  }

  [Test]
  public async Task GetBlobStateAsyncWithNonExistentBlobReturnsNotFoundStatus()
  {
    var mockCallInvoker = new Mock<CallInvoker>();

    var response = new GetResultResponse
                   {
                     Result = new ResultRaw
                              {
                                Status    = ResultStatus.Notfound,
                                ResultId  = "nonExistentBlobId",
                                SessionId = "sessionId",
                                Name      = "nonExistentBlob",
                                CompletedAt = DateTime.Now.ToUniversalTime()
                                                      .ToTimestamp(),
                                CreatedAt = DateTime.Now.ToUniversalTime()
                                                    .ToTimestamp(),
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

    Assert.Multiple(() =>
                    {
                      Assert.That(result.Status,
                                  Is.EqualTo(BlobStatus.Notfound),
                                  "Status should be NotFound");

                      Assert.That(result.BlobId,
                                  Is.EqualTo(response.Result.ResultId),
                                  "BlobId should match the requested ID");

                      Assert.That(result.SessionId,
                                  Is.EqualTo(response.Result.SessionId),
                                  "SessionId should match");

                      Assert.That(result.BlobName,
                                  Is.EqualTo(response.Result.Name),
                                  "BlobName should match");
                    });
  }

  [Test]
  public async Task UploadBlobAsyncWithValidContentUploadsBlob()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var contents = new ReadOnlyMemory<byte>(new byte[]
                                            {
                                              1,
                                              2,
                                              3,
                                              4,
                                              5,
                                            });

    var serviceConfig = new ResultsServiceConfigurationResponse
                        {
                          DataChunkMaxSize = 1000,
                        };
    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfig);

    var uploadResponse = new UploadResultDataResponse
                         {
                           Result = new ResultRaw
                                    {
                                      ResultId  = "testBlobId",
                                      SessionId = "sessionId",
                                      Status    = ResultStatus.Completed,
                                    },
                         };

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();
    mockCallInvoker.SetupAsyncClientStreamingCall(uploadResponse,
                                                  mockStream.Object);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);
    var blobInfo = new BlobInfo
                   {
                     BlobName  = "testBlob",
                     BlobId    = "testBlobId",
                     SessionId = "sessionId",
                   };

    await blobService.UploadBlobAsync(blobInfo,
                                      contents,
                                      CancellationToken.None);

    mockCallInvoker.Verify(x => x.AsyncClientStreamingCall(It.IsAny<Method<UploadResultDataRequest, UploadResultDataResponse>>(),
                                                           It.IsAny<string>(),
                                                           It.IsAny<CallOptions>()),
                           Times.Once);
  }

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
                                                     3,
                                                     4,
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

    var expectedChunk = new byte[]
                        {
                          1,
                          2,
                          3,
                          4,
                          5,
                          6,
                        };

    Assert.Multiple(() =>
                    {
                      Assert.That(resultChunks,
                                  Has.Count.EqualTo(1),
                                  "Should receive exactly 1 chunk");

                      Assert.That(resultChunks.Select(c => c),
                                  Is.EqualTo(new[]
                                             {
                                               expectedChunk,
                                             }));
                    });
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

    Assert.Multiple(() =>
                    {
                      Assert.That(response.Results.Count,
                                  Is.EqualTo(2));
                      Assert.That(resultBlobs.Select(b => b.BlobDetails.BlobName),
                                  Is.EqualTo(new[]
                                             {
                                               "blob1",
                                               "blob2",
                                             }));
                    });
  }

  [Test]
  public async Task GetBlobStateAsyncWithExistingBlobReturnsCorrectState()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var createdAt       = DateTime.UtcNow;
    var completedAt     = DateTime.UtcNow.AddMinutes(5);

    mockCallInvoker.SetupAsyncUnaryCallInvokerMock<GetResultRequest, GetResultResponse>(new GetResultResponse
                                                                                        {
                                                                                          Result = new ResultRaw
                                                                                                   {
                                                                                                     ResultId    = "existingBlobId",
                                                                                                     SessionId   = "sessionId",
                                                                                                     Name        = "existingBlob",
                                                                                                     Status      = ResultStatus.Completed,
                                                                                                     CreatedAt   = createdAt.ToTimestamp(),
                                                                                                     CompletedAt = completedAt.ToTimestamp(),
                                                                                                   },
                                                                                        });

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);
    var blobInfo = new BlobInfo
                   {
                     BlobId    = "existingBlobId",
                     SessionId = "sessionId",
                     BlobName  = "existingBlob",
                   };

    var result = await blobService.GetBlobStateAsync(blobInfo);

    Assert.Multiple(() =>
                    {
                      Assert.That(result.Status,
                                  Is.EqualTo(BlobStatus.Completed));
                      Assert.That(result.BlobId,
                                  Is.EqualTo("existingBlobId"));
                      Assert.That(result.CreateAt,
                                  Is.EqualTo(createdAt)
                                    .Within(TimeSpan.FromSeconds(1)));
                      Assert.That(result.CompletedAt,
                                  Is.EqualTo(completedAt)
                                    .Within(TimeSpan.FromSeconds(1)));
                    });
  }

  [Test]
  public async Task DownloadBlobAsyncWithValidBlobReturnsContent()
  {
    var mockCallInvoker = new Mock<CallInvoker>();
    var expectedContent = new byte[]
                          {
                            1,
                            2,
                            3,
                            4,
                            5,
                          };

    var downloadResponse = new DownloadResultDataResponse
                           {
                             DataChunk = ByteString.CopyFrom(expectedContent),
                           };

    mockCallInvoker.SetupAsyncServerStreamingCallInvokerMock<DownloadResultDataRequest, DownloadResultDataResponse>(downloadResponse);

    var blobService = MockHelper.GetBlobServiceMock(mockCallInvoker);
    var blobInfo = new BlobInfo
                   {
                     BlobId    = "testId",
                     SessionId = "sessionId",
                     BlobName  = "test",
                   };

    var result = await blobService.DownloadBlobAsync(blobInfo);

    Assert.That(result,
                Is.EqualTo(expectedContent));
  }
}
