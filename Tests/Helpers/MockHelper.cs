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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

using Grpc.Core;

using Moq;
using Moq.Language;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace Tests.Helpers;

internal static class MockHelper
{
  public static void ConfigureBlobService(this Mock<CallInvoker> mockInvoker,
                                          int                    dataChunkMaxSize = 1024)
  {
    var submitConfigurationResponse = new ResultsServiceConfigurationResponse
                                      {
                                        DataChunkMaxSize = dataChunkMaxSize,
                                      };
    mockInvoker.SetupAsyncUnaryCallInvokerMock<Empty, ResultsServiceConfigurationResponse>(submitConfigurationResponse);
  }

  public static void ConfigureBlobMetadataCreationResponse(this   Mock<CallInvoker>                                    mockInvoker,
                                                           params (string sessionId, string blobId, string blobName)[] blobs)
  {
    var blobInfos = blobs.Select(b => new ResultRaw
                                      {
                                        SessionId = b.sessionId,
                                        ResultId  = b.blobId,
                                        Name      = b.blobName,
                                      });
    var submitResultMetadataResponse = new CreateResultsMetaDataResponse
                                       {
                                         Results =
                                         {
                                           blobInfos,
                                         },
                                       };
    mockInvoker.SetupAsyncUnaryCallInvokerMock<CreateResultsMetaDataRequest, CreateResultsMetaDataResponse>(submitResultMetadataResponse);
  }

  public static void ConfigureSubmitTaskResponse(this   Mock<CallInvoker>                                                        mockInvoker,
                                                 params (string taskId, string payloadId, string[]? inputs, string[]? outputs)[] tasks)
  {
    var taskInfos = tasks.Select(t => new SubmitTasksResponse.Types.TaskInfo
                                      {
                                        TaskId    = t.taskId,
                                        PayloadId = t.payloadId,
                                        DataDependencies =
                                        {
                                          t.inputs?.ToList() ?? new List<string>(),
                                        },
                                        ExpectedOutputIds =
                                        {
                                          t.outputs?.ToList() ?? new List<string>(),
                                        },
                                      });
    var submitTaskResponse = new SubmitTasksResponse
                             {
                               TaskInfos =
                               {
                                 taskInfos,
                               },
                             };
    mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(submitTaskResponse);
  }

  public static ISetupSequentialResult<AsyncUnaryCall<TRes>> InitAsyncUnaryCallInvokerMock<TReq, TRes>(this Mock<CallInvoker> mockInvoker,
                                                                                                       TRes                   returnData)
    where TReq : class
    where TRes : class
    => mockInvoker.SetupSequence(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<TReq, TRes>>(),
                                                                   It.IsAny<string>(),
                                                                   It.IsAny<CallOptions>(),
                                                                   It.IsAny<TReq>()))
                  .Returns(new AsyncUnaryCall<TRes>(Task.FromResult(returnData),
                                                    Task.FromResult(new Metadata()),
                                                    () => Status.DefaultSuccess,
                                                    () => new Metadata(),
                                                    () =>
                                                    {
                                                    }));

  public static ISetupSequentialResult<AsyncUnaryCall<TRes>> AddAsyncUnaryCallInvokerMock<TReq, TRes>(this ISetupSequentialResult<AsyncUnaryCall<TRes>> sequentialResult,
                                                                                                      TRes                                              returnData)
    where TReq : class
    where TRes : class
    => sequentialResult.Returns(new AsyncUnaryCall<TRes>(Task.FromResult(returnData),
                                                         Task.FromResult(new Metadata()),
                                                         () => Status.DefaultSuccess,
                                                         () => new Metadata(),
                                                         () =>
                                                         {
                                                         }));

  public static Mock<CallInvoker> SetupAsyncUnaryCallInvokerMock<TReq, TRes>(this Mock<CallInvoker> mockInvoker,
                                                                             TRes                   returnData)
    where TReq : class
    where TRes : class
  {
    var responseTask        = Task.FromResult(returnData);
    var responseHeadersTask = Task.FromResult(new Metadata());

    mockInvoker.Setup(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<TReq, TRes>>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CallOptions>(),
                                                        It.IsAny<TReq>()))
               .Returns(new AsyncUnaryCall<TRes>(responseTask,
                                                 responseHeadersTask,
                                                 () => Status.DefaultSuccess,
                                                 () => new Metadata(),
                                                 () =>
                                                 {
                                                 }));
    return mockInvoker;
  }

  public static Mock<CallInvoker> SetupAsyncClientStreamingCall<TReq, TRes>(this Mock<CallInvoker>    mockInvoker,
                                                                            TRes                      returnData,
                                                                            IClientStreamWriter<TReq> stream)
    where TReq : class
    where TRes : class
  {
    var responseTask = Task.FromResult(returnData);
    mockInvoker.Setup(invoker => invoker.AsyncClientStreamingCall(It.IsAny<Method<TReq, TRes>>(),
                                                                  It.IsAny<string>(),
                                                                  It.IsAny<CallOptions>()))
               .Returns(new AsyncClientStreamingCall<TReq, TRes>(stream,
                                                                 responseTask,
                                                                 Task.FromResult(new Metadata()),
                                                                 () => Status.DefaultSuccess,
                                                                 () => new Metadata(),
                                                                 () =>
                                                                 {
                                                                 }));
    return mockInvoker;
  }

  public static Mock<CallInvoker> SetupAsyncServerStreamingCallInvokerMock<TReq, TRes>(this Mock<CallInvoker> mockInvoker,
                                                                                       TRes                   returnData)
    where TReq : class
    where TRes : class
  {
    var streamReaderMock = new Mock<IAsyncStreamReader<TRes>>();

    streamReaderMock.SetupSequence(x => x.MoveNext(It.IsAny<CancellationToken>()))
                    .Returns(() => Task.FromResult(true))
                    .Returns(() => Task.FromResult(false));

    streamReaderMock.SetupGet(x => x.Current)
                    .Returns(() => returnData);

    mockInvoker.Setup(invoker => invoker.AsyncServerStreamingCall(It.IsAny<Method<TReq, TRes>>(),
                                                                  It.IsAny<string>(),
                                                                  It.IsAny<CallOptions>(),
                                                                  It.IsAny<TReq>()))
               .Returns(new AsyncServerStreamingCall<TRes>(streamReaderMock.Object,
                                                           Task.FromResult(new Metadata()),
                                                           () => Status.DefaultSuccess,
                                                           () => new Metadata(),
                                                           () =>
                                                           {
                                                           }));

    return mockInvoker;
  }

  public static Mock<IBlobService> SetupCreateBlobMock(this Mock<IBlobService> blobService,
                                                       List<BlobInfo>          returnData)
  {
    blobService.Setup(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                              It.IsAny<IEnumerable<(string name, ReadOnlyMemory<byte> content, bool manualDeletion)>>(),
                                              It.IsAny<CancellationToken>()))
               .Returns(returnData.ToAsyncEnumerable);

    return blobService;
  }

  /// <summary>
  ///   Reset global counters
  /// </summary>
  public static void InitMock()
    => configureBlobCreationResponseCount_ = 0;

  #region Blob creation

  private static int configureBlobCreationResponseCount_;

  public static ISetupSequentialResult<AsyncUnaryCall<CreateResultsResponse>> ConfigureBlobCreationResponseSequence(this Mock<CallInvoker> mockInvoker,
                                                                                                                    params (string sessionId, string blobId, string
                                                                                                                      blobName)[] blobs)
  {
    var blobInfos = blobs.Select(b => new ResultRaw
                                      {
                                        SessionId = b.sessionId,
                                        ResultId  = b.blobId,
                                        Name      = b.blobName,
                                        CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                                      });
    // Configure response of payload blob creation
    var submitResultResponse = new CreateResultsResponse
                               {
                                 Results =
                                 {
                                   blobInfos,
                                 },
                               };
    configureBlobCreationResponseCount_++;
    return mockInvoker.InitAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(submitResultResponse);
  }

  public static ISetupSequentialResult<AsyncUnaryCall<CreateResultsResponse>> ConfigureBlobCreationResponseSequence(
    this   ISetupSequentialResult<AsyncUnaryCall<CreateResultsResponse>> sequentialResult,
    params (string sessionId, string blobId, string blobName)[]          blobs)
  {
    var blobInfos = blobs.Select(b => new ResultRaw
                                      {
                                        SessionId = b.sessionId,
                                        ResultId  = b.blobId,
                                        Name      = b.blobName,
                                        CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                                      });
    // Configure response of payload blob creation
    var submitResultResponse = new CreateResultsResponse
                               {
                                 Results =
                                 {
                                   blobInfos,
                                 },
                               };
    configureBlobCreationResponseCount_++;
    return sequentialResult.AddAsyncUnaryCallInvokerMock<CreateResultsRequest, CreateResultsResponse>(submitResultResponse);
  }

  public static void Stop(this ISetupSequentialResult<AsyncUnaryCall<CreateResultsResponse>> sequentialResult)
    => sequentialResult.Throws(new InvalidOperationException("There was more blob creation calls than expected!"));


  public static void CheckConfigureBlobCreationResponseCount(this Mock<CallInvoker> mockInvoker)
    => mockInvoker.Verify(s => s.AsyncUnaryCall(It.IsAny<Method<CreateResultsRequest, CreateResultsResponse>>(),
                                                It.IsAny<string>(),
                                                It.IsAny<CallOptions>(),
                                                It.IsAny<CreateResultsRequest>()),
                          Times.Exactly(configureBlobCreationResponseCount_));

  public static byte[] GetBlobDataSent(this Mock<CallInvoker> mockInvoker,
                                       string                 blobName,
                                       int                    index = 0)
  {
    var i = 0;
    foreach (var invocation in mockInvoker.Invocations)
    {
      if (invocation.Arguments[0] is Method<CreateResultsRequest, CreateResultsResponse> method && method.Name == "CreateResults")
      {
        var blobs = ((CreateResultsRequest)invocation.Arguments[3]).Results;
        foreach (var blob in blobs)
        {
          if (blob.Name == blobName)
          {
            if (i == index)
            {
              return blob.Data.ToByteArray();
            }

            i++;
          }
        }
      }
    }

    throw new InvalidOperationException($"Could not find blob '{blobName}'");
  }

  #endregion Blob creation
}
