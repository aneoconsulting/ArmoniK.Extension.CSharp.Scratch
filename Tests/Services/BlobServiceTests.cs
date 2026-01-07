// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

using Empty = ArmoniK.Api.gRPC.V1.Empty;

namespace Tests.Services;

public class BlobServiceTests
{
  [Test]
  public async Task CreateBlobReturnsNewBlobInfo()
  {
    var client = new MockedArmoniKClient();

    var responseAsync = new CreateResultsMetaDataResponse
                        {
                          Results =
                          {
                            new ResultRaw
                            {
                              CompletedAt = DateTime.UtcNow.ToTimestamp(),
                              Status      = ResultStatus.Created,
                              Name        = "blobName",
                              ResultId    = "blobId",
                              SessionId   = "sessionId",
                            },
                          },
                        };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var results = client.BlobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                              ["blobName"]);

    var blobInfos = await results.ToListAsync()
                                 .ConfigureAwait(false);
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
    client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CallOptions>(),
                                                        It.IsAny<CreateResultsMetaDataRequest>()),
                                  Times.Once,
                                  "AsyncUnaryCall for CreateResultsMetaDataRequest should be called exactly once");
  }


  [Test]
  public async Task CreateBlobWithNameReturnsNewBlobInfo()
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
                              ResultId  = "blobId",
                              SessionId = "sessionId",
                            },
                          },
                        };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var result = client.BlobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                             [name]);

    var blobInfos = await result.ToListAsync()
                                .ConfigureAwait(false);

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
    client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CallOptions>(),
                                                        It.IsAny<CreateResultsMetaDataRequest>()),
                                  Times.Once,
                                  "AsyncUnaryCall for CreateResultsMetaDataRequest should be called exactly once");
  }

  [Test]
  public async Task CreateBlobAsyncWithContentCreatesBlobAndUploadsContent()
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
                                         ResultId  = "blobId",
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
                                     ResultId  = "blobId",
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
                                    Name      = "anyResult",
                                    ResultId  = "anyResultId",
                                    SessionId = "sessionId",
                                  },
                       };

    client.CallInvokerMock.Setup(x => x.AsyncClientStreamingCall(It.IsAny<Method<UploadResultDataRequest, UploadResultDataResponse>>(),
                                                                 It.IsAny<string>(),
                                                                 It.IsAny<CallOptions>()))
          .Returns(new AsyncClientStreamingCall<UploadResultDataRequest, UploadResultDataResponse>(Mock.Of<IClientStreamWriter<UploadResultDataRequest>>(),
                                                                                                   Task.FromResult(responseTask),
                                                                                                   Task.FromResult(new Metadata()),
                                                                                                   () => Status.DefaultSuccess,
                                                                                                   () => new Metadata(),
                                                                                                   () =>
                                                                                                   {
                                                                                                   }));
    var result = await client.BlobService.CreateBlobAsync(new SessionInfo("sessionId"),
                                                          name,
                                                          contents);
    Assert.That(result.SessionId,
                Is.EqualTo("sessionId"));
    Assert.That(result.BlobName,
                Is.EqualTo(name));
  }

  [Test]
  public async Task CreateBlobAsyncWithBigContentCreatesBlobAndUploadsContent()
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
                                         ResultId  = "blobId",
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
                                     ResultId  = "blobId",
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
                                                          contents)
                             .ConfigureAwait(false);

    Assert.That(result.SessionId,
                Is.EqualTo("sessionId"));
    Assert.That(result.BlobName,
                Is.EqualTo(name));
    Assert.That(result,
                Is.EqualTo(new BlobInfo
                           {
                             SessionId = "sessionId",
                             BlobName  = name,
                             BlobId    = "blobId",
                           }));
    client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CallOptions>(),
                                                        It.IsAny<CreateResultsMetaDataRequest>()),
                                  Times.Once,
                                  "AsyncUnaryCall for CreateResultsMetaDataRequest should be called exactly once");
  }

  [Test]
  public async Task GetBlobStateAsyncWithNonExistentBlobReturnsNotFoundStatus()
  {
    var client = new MockedArmoniKClient();

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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<GetResultRequest, GetResultResponse>(response);

    var blobInfo = new BlobInfo
                   {
                     BlobName  = "nonExistentBlob",
                     BlobId    = "nonExistentBlobId",
                     SessionId = "sessionId",
                   };

    var result = await client.BlobService.GetBlobStateAsync(blobInfo);

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
                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<GetResultRequest, GetResultResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.IsAny<GetResultRequest>()),
                                                    Times.Once,
                                                    "AsyncUnaryCall should be called exactly once");
                    });
  }

  [Test]
  public async Task UploadBlobAsyncWithValidContentUploadsBlob()
  {
    var client   = new MockedArmoniKClient();
    var contents = new ReadOnlyMemory<byte>([1, 2, 3, 4, 5]);

    var serviceConfig = new ResultsServiceConfigurationResponse
                        {
                          DataChunkMaxSize = 1000,
                        };
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfig);

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
    client.CallInvokerMock.SetupAsyncClientStreamingCall(uploadResponse,
                                                         mockStream.Object);

    var blobInfo = new BlobInfo
                   {
                     BlobName  = "testBlob",
                     BlobId    = "testBlobId",
                     SessionId = "sessionId",
                   };

    await client.BlobService.UploadBlobAsync(blobInfo,
                                             contents,
                                             CancellationToken.None);

    Assert.Multiple(() =>
                    {
                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<Empty, ResultsServiceConfigurationResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.IsAny<Empty>()),
                                                    Times.Once,
                                                    "Service configuration should be called");

                      client.CallInvokerMock.Verify(x => x.AsyncClientStreamingCall(It.IsAny<Method<UploadResultDataRequest, UploadResultDataResponse>>(),
                                                                                    It.IsAny<string>(),
                                                                                    It.IsAny<CallOptions>()),
                                                    Times.Once,
                                                    "Upload streaming should be called");
                    });
  }

  [Test]
  public async Task DownloadBlobWithChunksAsyncWithValidBlobReturnsBlobChunks()
  {
    var client = new MockedArmoniKClient();

    var expectedChunks = new List<byte[]>
                         {
                           new byte[]
                           {
                             0x01,
                             0x02,
                             0x03,
                           },
                           new byte[]
                           {
                             0x04,
                             0x05,
                             0x06,
                           },
                         };

    // Create a mock for the asynchronous stream reader to simulate streaming data
    var responseStreamMock = new Mock<IAsyncStreamReader<DownloadResultDataResponse>>();

    // Configure the sequence of MoveNext and Current calls to simulate the streaming behavior
    var callCount = 0; // Tracks the number of times MoveNext is called

    responseStreamMock.Setup(x => x.MoveNext(It.IsAny<CancellationToken>()))
                      .Returns(() => Task.FromResult(callCount < expectedChunks.Count)) // Returns true until all chunks are read
                      .Callback(() => callCount++);                                     // Increment callCount each time MoveNext is called

    // Setup the Current property to return the appropriate chunk based on callCount
    responseStreamMock.Setup(x => x.Current)
                      .Returns(() => new DownloadResultDataResponse
                                     {
                                       DataChunk = ByteString.CopyFrom(expectedChunks[callCount - 1]), // Return the chunk corresponding to the current callCount
                                     });

    // Setup the mock CallInvoker to return our mock stream when AsyncServerStreamingCall is invoked
    client.CallInvokerMock.Setup(x => x.AsyncServerStreamingCall(It.IsAny<Method<DownloadResultDataRequest, DownloadResultDataResponse>>(),
                                                                 It.IsAny<string>(),
                                                                 It.IsAny<CallOptions>(),
                                                                 It.IsAny<DownloadResultDataRequest>()))
          .Returns(new AsyncServerStreamingCall<DownloadResultDataResponse>(responseStreamMock.Object,
                                                                            Task.FromResult(new Metadata()),
                                                                            () => Status.DefaultSuccess,
                                                                            () => new Metadata(),
                                                                            () =>
                                                                            {
                                                                            })); // Return the mock streaming call setup


    var blobInfo = new BlobInfo
                   {
                     BlobId    = "testBlobId",
                     SessionId = "testSessionId",
                   };

    // Collect the chunks of data returned by the DownloadBlobWithChunksAsync method
    var resultChunks = new List<byte[]>();
    await foreach (var chunk in client.BlobService.DownloadBlobWithChunksAsync(blobInfo))
    {
      resultChunks.Add(chunk);
    }


    // Verify that the chunks received match the expected chunks
    Assert.That(expectedChunks,
                Is.EqualTo(resultChunks));

    // Verify that the AsyncServerStreamingCall method was called exactly once
    client.CallInvokerMock.Verify(x => x.AsyncServerStreamingCall(It.IsAny<Method<DownloadResultDataRequest, DownloadResultDataResponse>>(),
                                                                  It.IsAny<string>(),
                                                                  It.IsAny<CallOptions>(),
                                                                  It.IsAny<DownloadResultDataRequest>()),
                                  Times.Once,
                                  "Download streaming should be called");
  }


  [Test]
  public async Task ListBlobsAsyncWithPaginationReturnsBlobs()
  {
    var client = new MockedArmoniKClient();

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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    var blobPagination = new BlobPagination
                         {
                           Page          = 1,
                           PageSize      = 10,
                           SortDirection = SortDirection.Asc,
                           Filter        = new Filters(),
                         };

    var resultBlobs = new List<BlobState>();
    var blobPage    = await client.BlobService.ListBlobsAsync(blobPagination);
    foreach (var bp in blobPage.Blobs)
    {
      resultBlobs.Add(bp);
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(response.Results.Count,
                                  Is.EqualTo(2));
                      Assert.That(resultBlobs.Select(b => b.BlobName),
                                  Is.EqualTo(new[]
                                             {
                                               "blob1",
                                               "blob2",
                                             }));
                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<ListResultsRequest, ListResultsResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.IsAny<ListResultsRequest>()),
                                                    Times.Once,
                                                    "AsyncUnaryCall should be called exactly once");
                    });
  }

  [Test]
  public async Task GetBlobStateAsyncWithExistingBlobReturnsCorrectState()
  {
    var client      = new MockedArmoniKClient();
    var createdAt   = DateTime.UtcNow;
    var completedAt = DateTime.UtcNow.AddMinutes(5);

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<GetResultRequest, GetResultResponse>(new GetResultResponse
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

    var blobInfo = new BlobInfo
                   {
                     BlobId    = "existingBlobId",
                     SessionId = "sessionId",
                     BlobName  = "existingBlob",
                   };

    var result = await client.BlobService.GetBlobStateAsync(blobInfo);

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
                      Assert.That(result,
                                  Is.EqualTo(new BlobState
                                             {
                                               Status      = BlobStatus.Completed,
                                               BlobId      = "existingBlobId",
                                               SessionId   = "sessionId",
                                               BlobName    = "existingBlob",
                                               CreateAt    = createdAt,
                                               CompletedAt = completedAt,
                                             }));
                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<GetResultRequest, GetResultResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.IsAny<GetResultRequest>()),
                                                    Times.Once,
                                                    "AsyncUnaryCall should be called exactly once");
                    });
  }

  [Test]
  public async Task DownloadBlobAsyncWithValidBlobReturnsContent()
  {
    var client = new MockedArmoniKClient();
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

    client.CallInvokerMock.SetupAsyncServerStreamingCallInvokerMock<DownloadResultDataRequest, DownloadResultDataResponse>(downloadResponse);

    var blobInfo = new BlobInfo
                   {
                     BlobId    = "testId",
                     SessionId = "sessionId",
                     BlobName  = "test",
                   };

    var result = await client.BlobService.DownloadBlobAsync(blobInfo);
    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.EqualTo(expectedContent));

                      client.CallInvokerMock.Verify(x => x.AsyncServerStreamingCall(It.IsAny<Method<DownloadResultDataRequest, DownloadResultDataResponse>>(),
                                                                                    It.IsAny<string>(),
                                                                                    It.IsAny<CallOptions>(),
                                                                                    It.IsAny<DownloadResultDataRequest>()),
                                                    Times.Once,
                                                    "AsyncServerStreamingCall should be called exactly once");
                    });
  }

  [Test]
  public async Task CreateBlobAsync_ImportBlobData()
  {
    var client = new MockedArmoniKClient();

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

    var now = DateTime.UtcNow;
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
                                 CreatedAt = Timestamp.FromDateTime(now),
                               },
                             },
                           };
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ImportResultsDataRequest, ImportResultsDataResponse>(expectedResponse);

    var result = await client.BlobService.ImportBlobDataAsync(sessionInfo,
                                                              blobDescs)
                             .ConfigureAwait(false);

    var blobState = result.Single();
    Assert.That(blobState,
                Is.EqualTo(new BlobState
                           {
                             SessionId = sessionInfo.SessionId,
                             BlobId    = "blob1",
                             Status    = BlobStatus.Completed,
                             BlobName  = "myBlob",
                             OpaqueId  = opaqueId,
                             CreateAt  = now,
                           }));
  }
}
