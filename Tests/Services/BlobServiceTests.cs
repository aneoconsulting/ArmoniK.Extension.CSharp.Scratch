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
using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Configuration;
using Tests.Helpers;

using Empty = ArmoniK.Api.gRPC.V1.Empty;

namespace Tests.Services;

public class BlobServiceTests
{
  private ArmoniKClient?        client_;
  private Properties?           defaultProperties_;
  private TaskConfiguration?    defaultTaskOptions_;
  private Mock<ILoggerFactory>? loggerFactoryMock_;
  private Mock<CallInvoker>?    mockCallInvoker_;

  [SetUp]
  public void SetUp()
  {
    IConfiguration configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                             .AddJsonFile("appsettings.tests.json",
                                                                          false)
                                                             .AddEnvironmentVariables()
                                                             .Build();
    defaultTaskOptions_ = new TaskConfiguration(2,
                                                1,
                                                "subtasking",
                                                TimeSpan.FromHours(1));

    defaultProperties_ = new Properties(configuration);

    loggerFactoryMock_ = new Mock<ILoggerFactory>();
    mockCallInvoker_   = new Mock<CallInvoker>();

    client_ = new ArmoniKClient(defaultProperties_,
                                loggerFactoryMock_.Object,
                                defaultTaskOptions_,
                                new MockedServicesConfiguration(mockCallInvoker_));
  }

  [Test]
  public async Task CreateBlob_ReturnsNewBlobInfo()
  {
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

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var results = client_!.BlobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
                                                                ["blobName"]);

    var blobInfos = await results.ToListAsync();
    ClassicAssert.AreEqual("blobName",
                           blobInfos[0].BlobName);
  }


  [Test]
  public async Task CreateBlob_WithName_ReturnsNewBlobInfo()
  {
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

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(responseAsync);

    var result = client_!.BlobService.CreateBlobsMetadataAsync(new SessionInfo("sessionId"),
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
    var name = "blobName";
    var contents = new ReadOnlyMemory<byte>(Enumerable.Range(1,
                                                             20)
                                                      .Select(x => (byte)x)
                                                      .ToArray());

    var serviceConfigurationResponse = new ResultsServiceConfigurationResponse
                                       {
                                         DataChunkMaxSize = 500,
                                       };

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfigurationResponse);

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

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(metadataCreationResponse);

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

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(createResultResponse);

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();

    var responseTask = new UploadResultDataResponse
                       {
                         Result = new ResultRaw
                                  {
                                    Name     = "anyResult",
                                    ResultId = "anyResultId",
                                  },
                       };

    mockCallInvoker_!.SetupAsyncClientStreamingCall(responseTask,
                                                    mockStream.Object);

    var result = await client_!.BlobService.CreateBlobAsync(new SessionInfo("sessionId"),
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
    var name = "blobName";
    var contents = new ReadOnlyMemory<byte>(Enumerable.Range(1,
                                                             500)
                                                      .Select(x => (byte)x)
                                                      .ToArray());

    var serviceConfigurationResponse = new ResultsServiceConfigurationResponse
                                       {
                                         DataChunkMaxSize = 20,
                                       };

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(serviceConfigurationResponse);

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

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(metadataCreationResponse);

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

    mockCallInvoker_!.SetupAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(createResultResponse);

    var mockStream = new Mock<IClientStreamWriter<UploadResultDataRequest>>();

    var responseTask = new UploadResultDataResponse
                       {
                         Result = new ResultRaw
                                  {
                                    Name     = "anyResult",
                                    ResultId = "anyResultId",
                                  },
                       };

    mockCallInvoker_!.SetupAsyncClientStreamingCall(responseTask,
                                                    mockStream.Object);

    var result = await client_!.BlobService.CreateBlobAsync(new SessionInfo("sessionId"),
                                                            name,
                                                            contents);

    ClassicAssert.AreEqual("sessionId",
                           result.SessionId);
    ClassicAssert.AreEqual(name,
                           result.BlobName);
  }
}
