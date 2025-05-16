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

using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Factory;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

namespace Tests.Helpers;

internal static class MockHelper
{
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
                                                 StatusFunc,
                                                 TrailersFunc,
                                                 DisposeAction));

    return mockInvoker;

    void DisposeAction()
    {
    }

    Metadata TrailersFunc()
      => new();

    Status StatusFunc()
      => Status.DefaultSuccess;
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


  public static ITasksService GetTasksServiceMock(Mock<CallInvoker>?  mockInvoker     = null,
                                                  Mock<IBlobService>? mockBlobService = null)
  {
    mockInvoker ??= new Mock<CallInvoker>();
    var mockChannelBase = new Mock<ChannelBase>("localhost")
                          {
                            CallBase = true,
                          };
    mockChannelBase.Setup(m => m.CreateCallInvoker())
                   .Returns(mockInvoker.Object);
    var objectPool = new ObjectPool<ChannelBase>(() => mockChannelBase.Object);
    mockBlobService ??= new Mock<IBlobService>();

    var taskService = TasksServiceFactory.CreateTaskService(objectPool,
                                                            mockBlobService.Object,
                                                            NullLoggerFactory.Instance);
    return taskService;
  }

  public static IEventsService GetEventsServiceMock(Mock<CallInvoker>? mockInvoker = null)
  {
    mockInvoker ??= new Mock<CallInvoker>();
    var mockChannelBase = new Mock<ChannelBase>("localhost")
                          {
                            CallBase = true,
                          };
    mockChannelBase.Setup(m => m.CreateCallInvoker())
                   .Returns(mockInvoker.Object);
    var objectPool = new ObjectPool<ChannelBase>(() => mockChannelBase.Object);
    return EventsServiceFactory.CreateEventsService(objectPool,
                                                    NullLoggerFactory.Instance);
  }

  public static ISessionService GetSessionServiceMock(Properties         properties,
                                                      TaskConfiguration  taskConfiguration,
                                                      Mock<CallInvoker>? mockInvoker = null)
  {
    mockInvoker ??= new Mock<CallInvoker>();
    var mockChannelBase = new Mock<ChannelBase>("localhost")
                          {
                            CallBase = true,
                          };
    mockChannelBase.Setup(m => m.CreateCallInvoker())
                   .Returns(mockInvoker.Object);
    var objectPool = new ObjectPool<ChannelBase>(() => mockChannelBase.Object);
    return SessionServiceFactory.CreateSessionService(objectPool,
                                                      properties,
                                                      NullLoggerFactory.Instance);
  }

  public static IBlobService GetBlobServiceMock(Mock<CallInvoker>? mockInvoker = null)
  {
    mockInvoker ??= new Mock<CallInvoker>();
    var mockChannelBase = new Mock<ChannelBase>("localhost")
                          {
                            CallBase = true,
                          };
    mockChannelBase.Setup(m => m.CreateCallInvoker())
                   .Returns(mockInvoker.Object);
    var objectPool = new ObjectPool<ChannelBase>(() => mockChannelBase.Object);
    return BlobServiceFactory.CreateBlobService(objectPool,
                                                NullLoggerFactory.Instance);
  }

  public static Mock<IBlobService> SetupCreateBlobMock(this Mock<IBlobService> blobService,
                                                       List<BlobInfo>          returnData)
  {
    blobService.Setup(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                              It.IsAny<IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                                              It.IsAny<CancellationToken>()))
               .Returns(returnData.ToAsyncEnumerable);

    return blobService;
  }
}
