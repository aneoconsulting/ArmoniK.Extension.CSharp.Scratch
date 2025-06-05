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

using System.Collections.Immutable;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Utils;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Helpers;

using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;

namespace Tests.Services;

public class TasksServiceTests
{
  [Test]
  public async Task CreateTaskReturnsNewTaskWithId()
  {
    var submitTaskResponse = new SubmitTasksResponse
                             {
                               TaskInfos =
                               {
                                 new SubmitTasksResponse.Types.TaskInfo
                                 {
                                   TaskId = "taskId1",
                                   ExpectedOutputIds =
                                   {
                                     new List<string>
                                     {
                                       "blobId1",
                                     },
                                   },
                                   PayloadId = "payloadId1",
                                 },
                               },
                             };

    var mockInvoker = new Mock<CallInvoker>();

    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(submitTaskResponse);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker);
    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        ExpectedOutputs = new List<BlobInfo>
                                          {
                                            new()
                                            {
                                              BlobName = "blob1",
                                              BlobId = submitTaskResponse.TaskInfos[0]
                                                                         .ExpectedOutputIds[0],
                                              SessionId = "sessionId1",
                                            },
                                          },
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payload1",
                                    BlobId    = submitTaskResponse.TaskInfos[0].PayloadId,
                                    SessionId = "sessionId1",
                                  },
                        Session = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                      },
                    };

    var result = await taskService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                    taskNodes)
                                  .ConfigureAwait(false);

    var taskInfosEnumerable = result as TaskInfos[] ?? result.ToArray();
    Assert.Multiple(() =>
                    {
                      Assert.That(taskInfosEnumerable,
                                  Is.Not.Null,
                                  "Result should not be null.");
                      Assert.That(taskInfosEnumerable.Length,
                                  Is.EqualTo(1),
                                  "Expected one task info in the response.");
                      Assert.That(taskInfosEnumerable.First()
                                                     .TaskId,
                                  Is.EqualTo("taskId1"),
                                  "Expected task ID to match.");
                      Assert.That(taskInfosEnumerable.First()
                                                     .PayloadId,
                                  Is.EqualTo("payloadId1"),
                                  "Expected payload ID to match.");
                      Assert.That(taskInfosEnumerable.First()
                                                     .ExpectedOutputs.First(),
                                  Is.EqualTo("blobId1"),
                                  "Expected blob ID to match.");
                    });
  }

  [Test]
  public async Task SubmitTasksAsyncMultipleTasksWithOutputsReturnsCorrectResponses()
  {
    var taskResponse = new SubmitTasksResponse
                       {
                         TaskInfos =
                         {
                           new SubmitTasksResponse.Types.TaskInfo
                           {
                             TaskId    = "taskId1",
                             PayloadId = "payloadId1",
                             ExpectedOutputIds =
                             {
                               "outputId1",
                             },
                           },
                           new SubmitTasksResponse.Types.TaskInfo
                           {
                             TaskId    = "taskId2",
                             PayloadId = "payloadId2",
                             ExpectedOutputIds =
                             {
                               "outputId2",
                             },
                           },
                         },
                       };

    var mockInvoker = new Mock<CallInvoker>();

    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker);

    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        ExpectedOutputs = new List<BlobInfo>
                                          {
                                            new()
                                            {
                                              BlobName  = "blob1",
                                              BlobId    = "blobId1",
                                              SessionId = "sessionId1",
                                            },
                                          },
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payload1",
                                    BlobId    = "payloadId1",
                                    SessionId = "sessionId1",
                                  },
                        Session = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                      },
                      new()
                      {
                        ExpectedOutputs = new List<BlobInfo>
                                          {
                                            new()
                                            {
                                              BlobName  = "blob2",
                                              BlobId    = "blobId2",
                                              SessionId = "sessionId1",
                                            },
                                          },
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payload2",
                                    BlobId    = "payloadId2",
                                    SessionId = "sessionId1",
                                  },
                        Session = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                      },
                    };

    var result = await taskService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                    taskNodes)
                                  .ToListAsync()
                                  .ConfigureAwait(false);
    Assert.That(result,
                Has.Count.EqualTo(2),
                "Result should contain two task infos");
    Assert.That(result,
                Has.Some.Matches<TaskInfos>(r => r.TaskId == "taskId1" && r.PayloadId == "payloadId1" && r.ExpectedOutputs.Contains("outputId1")),
                "Result should contain an item with taskId1, payloadId1, and outputId1");
    Assert.That(result,
                Has.Some.Matches<TaskInfos>(r => r.TaskId == "taskId2" && r.PayloadId == "payloadId2" && r.ExpectedOutputs.Contains("outputId2")),
                "Result should contain an item with taskId2, payloadId2, and outputId2");
  }


  [Test]
  public void SubmitTasksAsyncWithEmptyExpectedOutputsThrowsException()
  {
    var taskService = MockHelper.GetTasksServiceMock();

    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payload1",
                                    BlobId    = "payloadId1",
                                    SessionId = "sessionId1",
                                  },
                        ExpectedOutputs = new List<BlobInfo>(),
                        Session         = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                      },
                    };

    Assert.That(async () => await taskService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                               taskNodes),
                Throws.Exception.TypeOf<InvalidOperationException>());
  }

  [Test]
  public async Task SubmitTasksAsyncWithDataDependenciesCreatesBlobsCorrectly()
  {
    var taskResponse = new SubmitTasksResponse
                       {
                         TaskInfos =
                         {
                           new SubmitTasksResponse.Types.TaskInfo
                           {
                             TaskId    = "taskId1",
                             PayloadId = "payloadId1",
                             ExpectedOutputIds =
                             {
                               "outputId1",
                             },
                             DataDependencies =
                             {
                               "dependencyBlobId",
                             },
                           },
                         },
                       };

    var mockInvoker = new Mock<CallInvoker>();

    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);

    var expectedBlobs = new List<BlobInfo>
                        {
                          new()
                          {
                            BlobName  = "dependencyBlob",
                            BlobId    = "dependencyBlobId",
                            SessionId = "sessionId1",
                          },
                        };

    var mockBlobService = new Mock<IBlobService>();

    mockBlobService.SetupCreateBlobMock(expectedBlobs);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker,
                                                     mockBlobService);

    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payloadId",
                                    BlobId    = "blobId",
                                    SessionId = "sessionId1",
                                  },
                        ExpectedOutputs = new List<BlobInfo>
                                          {
                                            new()
                                            {
                                              BlobName  = "output1",
                                              BlobId    = "outputId1",
                                              SessionId = "sessionId1",
                                            },
                                          },
                        Session = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                        DataDependenciesContent = new Dictionary<string, ReadOnlyMemory<byte>>
                                                  {
                                                    {
                                                      "dependencyBlob", new ReadOnlyMemory<byte>([1, 2, 3])
                                                    },
                                                  },
                      },
                    };

    var result = await taskService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                    taskNodes)
                                  .ConfigureAwait(false);

    mockBlobService.Verify(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                                   It.IsAny<IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                                                   It.IsAny<CancellationToken>()),
                           Times.Once);
    Assert.Multiple(() =>
                    {
                      Assert.That("dependencyBlobId",
                                  Is.EqualTo(result.First()
                                                   .DataDependencies.First()),
                                  "Expected data dependency blob ID to match.");
                      Assert.That("dependencyBlobId",
                                  Is.EqualTo(taskNodes.First()
                                                      .DataDependencies.First()
                                                      .BlobId),
                                  "Expected data dependency blob ID in task node to match.");
                    });
  }

  [Test]
  public async Task SubmitTasksAsyncEmptyDataDependenciesDoesNotCreateBlobs()
  {
    var taskResponse = new SubmitTasksResponse
                       {
                         TaskInfos =
                         {
                           new SubmitTasksResponse.Types.TaskInfo
                           {
                             TaskId    = "taskId1",
                             PayloadId = "payloadId1",
                             ExpectedOutputIds =
                             {
                               "outputId1",
                             },
                           },
                         },
                       };

    var mockInvoker = new Mock<CallInvoker>();

    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);

    var expectedBlobs = new List<BlobInfo>
                        {
                          new()
                          {
                            BlobName  = "dependencyBlob",
                            BlobId    = "dependencyBlobId",
                            SessionId = "sessionId1",
                          },
                        };


    var mockBlobService = new Mock<IBlobService>();

    mockBlobService.SetupCreateBlobMock(expectedBlobs);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker,
                                                     mockBlobService);

    var dataDependenciesContent = ImmutableDictionary<string, ReadOnlyMemory<byte>>.Empty;

    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payloadId",
                                    BlobId    = "blobId",
                                    SessionId = "sessionId1",
                                  },
                        ExpectedOutputs         = expectedBlobs,
                        DataDependenciesContent = dataDependenciesContent,
                        Session                 = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                      },
                    };
    await taskService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                       taskNodes)
                     .ConfigureAwait(false);
    mockBlobService.Verify(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                                   It.IsAny<IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                                                   It.IsAny<CancellationToken>()),
                           Times.Never);
    Assert.That(taskNodes.First()
                         .DataDependencies,
                Is.Empty);
  }

  [Test]
  public async Task ListTasksAsyncWithPaginationReturnsCorrectPage()
  {
    var taskResponse = new ListTasksResponse
                       {
                         Tasks =
                         {
                           new TaskSummary
                           {
                             Id        = "taskId1",
                             Status    = (ArmoniK.Api.gRPC.V1.TaskStatus)TaskStatus.Completed,
                             CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                             StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                             EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-1)),
                           },
                           new TaskSummary
                           {
                             Id        = "taskId2",
                             Status    = (ArmoniK.Api.gRPC.V1.TaskStatus)TaskStatus.Cancelling,
                             CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                             StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-10)),
                             EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                           },
                         },
                         Total = 2,
                       };

    var mockInvoker = new Mock<CallInvoker>();
    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<ListTasksRequest, ListTasksResponse>(taskResponse);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker);

    var paginationOptions = new TaskPagination
                            {
                              Page          = 1,
                              PageSize      = 10,
                              SortDirection = SortDirection.Asc,
                              Total         = 2,
                              Filter        = new Filters(),
                            };

    var result = await taskService.ListTasksAsync(paginationOptions)
                                  .ToListAsync()
                                  .ConfigureAwait(false);
    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.Not.Null,
                                  "Result should not be null.");
                      Assert.That(result,
                                  Has.Count.EqualTo(1),
                                  "Expected one page of tasks.");
                      Assert.That(result.Count,
                                  Is.EqualTo(1));
                      Assert.That(result[0].TotalTasks,
                                  Is.EqualTo(2));
                    });

    var tasksData = result[0]
                    .TasksData.ToList();
    Assert.That(tasksData.Count,
                Is.EqualTo(2));

    var expectedTaskIds = new List<string>
                          {
                            "taskId1",
                            "taskId2",
                          };
    var expectedStatuses = new List<TaskStatus>
                           {
                             TaskStatus.Completed,
                             TaskStatus.Cancelling,
                           };

    for (var i = 0; i < tasksData.Count; i++)
    {
      Assert.That(expectedTaskIds[i],
                  Is.EqualTo(tasksData[i].Item1));
      Assert.That(expectedStatuses[i],
                  Is.EqualTo(tasksData[i].Item2));
    }
  }

  [Test]
  public async Task GetTaskDetailedAsyncShouldReturnCorrectTaskDetails()
  {
    var taskResponse = new GetTaskResponse
                       {
                         Task = new TaskDetailed
                                {
                                  Id = "taskId1",
                                  ExpectedOutputIds =
                                  {
                                    "outputId1",
                                  },
                                  DataDependencies =
                                  {
                                    "dependencyId1",
                                  },
                                  CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                                  StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                                  EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-1)),
                                  SessionId = "sessionId1",
                                },
                       };

    var mockInvoker = new Mock<CallInvoker>();
    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<GetTaskRequest, GetTaskResponse>(taskResponse);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker);

    var result = await taskService.GetTasksDetailedAsync("taskId1")
                                  .ConfigureAwait(false);
    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.Not.Null,
                                  "Result should not be null.");
                      Assert.That(result.ExpectedOutputs,
                                  Has.Count.EqualTo(1),
                                  "Expected one expected output.");
                      Assert.That(result.ExpectedOutputs.First(),
                                  Is.EqualTo("outputId1"),
                                  "Expected output ID to match.");
                      Assert.That(result.DataDependencies,
                                  Has.Count.EqualTo(1),
                                  "Expected one data dependency.");
                      Assert.That(result.DataDependencies.First(),
                                  Is.EqualTo("dependencyId1"),
                                  "Expected data dependency ID to match.");
                    });
  }

  [Test]
  public async Task ListTasksDetailedAsyncReturnsCorrectTaskDetailedPage()
  {
    var taskResponse = new ListTasksDetailedResponse
                       {
                         Tasks =
                         {
                           new TaskDetailed
                           {
                             Id        = "taskId1",
                             Status    = ArmoniK.Api.gRPC.V1.TaskStatus.Completed,
                             CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                             StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                             EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-1)),
                             ExpectedOutputIds =
                             {
                               "outputId1",
                             },
                             DataDependencies =
                             {
                               "dependencyId1",
                             },
                             SessionId = "sessionId1",
                           },
                           new TaskDetailed
                           {
                             Id        = "taskId2",
                             Status    = ArmoniK.Api.gRPC.V1.TaskStatus.Cancelling,
                             CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                             StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-10)),
                             EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                             ExpectedOutputIds =
                             {
                               "outputId2",
                             },
                             DataDependencies =
                             {
                               "dependencyId2",
                             },
                             SessionId = "sessionId1",
                           },
                         },
                         Total = 1,
                       };
    var mockInvoker = new Mock<CallInvoker>();
    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<ListTasksRequest, ListTasksDetailedResponse>(taskResponse);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker);

    var paginationOptions = new TaskPagination
                            {
                              Page          = 1,
                              PageSize      = 10,
                              Filter        = new Filters(),
                              SortDirection = SortDirection.Asc,
                            };


    var result = await taskService.ListTasksDetailedAsync(new SessionInfo("sessionId1"),
                                                          paginationOptions)
                                  .FirstOrDefaultAsync()
                                  .ConfigureAwait(false);

    Assert.That(result,
                Is.Not.Null,
                "Result should not be null.");
  }

  [Test]
  public async Task CancelTasksAsyncCancelsTasksCorrectly()
  {
    var mockInvoker = new Mock<CallInvoker>();
    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<CancelTasksRequest, CancelTasksResponse>(new CancelTasksResponse());

    var taskService = MockHelper.GetTasksServiceMock(callInvoker);

    var taskIds = new List<string>
                  {
                    "taskId1",
                    "taskId2",
                  };

    await taskService.CancelTasksAsync(taskIds)
                     .ConfigureAwait(false);

    mockInvoker.Verify(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CancelTasksRequest, CancelTasksResponse>>(),
                                                         It.IsAny<string>(),
                                                         It.IsAny<CallOptions>(),
                                                         It.Is<CancelTasksRequest>(req => req.TaskIds.Count == 2 && req.TaskIds.Contains("taskId1") &&
                                                                                          req.TaskIds.Contains("taskId2"))),
                       Times.Once);
  }

  [Test]
  public async Task CreateNewBlobsAsyncCreatesBlobsCorrectly()
  {
    var session = new SessionInfo("sessionId1");
    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payloadId",
                                    BlobId    = "blobId",
                                    SessionId = "sessionId1",
                                  },
                        ExpectedOutputs = new List<BlobInfo>
                                          {
                                            new()
                                            {
                                              BlobName  = "output1",
                                              BlobId    = "outputId1",
                                              SessionId = "sessionId1",
                                            },
                                          },
                        Session = session,
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                        DataDependenciesContent = new Dictionary<string, ReadOnlyMemory<byte>>
                                                  {
                                                    {
                                                      "dependencyBlob", new ReadOnlyMemory<byte>([1, 2, 3])
                                                    },
                                                  },
                      },
                    };

    var expectedBlobs = new List<BlobInfo>
                        {
                          new()
                          {
                            BlobName  = "dependencyBlob",
                            BlobId    = "dependencyBlobId",
                            SessionId = "sessionId1",
                          },
                        };

    var mockBlobService = new Mock<IBlobService>();
    mockBlobService.SetupCreateBlobMock(expectedBlobs);

    var mockInvoker = new Mock<CallInvoker>();
    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(new SubmitTasksResponse());

    var taskService = MockHelper.GetTasksServiceMock(callInvoker,
                                                     mockBlobService);

    var result = await taskService.SubmitTasksAsync(session,
                                                    taskNodes)
                                  .ConfigureAwait(false);

    Assert.That(result,
                Is.Not.Null,
                "Result should not be null.");
    mockBlobService.Verify(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                                   It.IsAny<IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                                                   It.IsAny<CancellationToken>()),
                           Times.Once);
  }

  [Test]
  public async Task SubmitTasksAsyncWithDataDependenciesReturnsCorrectResponses()
  {
    var taskResponse = new SubmitTasksResponse
                       {
                         TaskInfos =
                         {
                           new SubmitTasksResponse.Types.TaskInfo
                           {
                             TaskId    = "taskId1",
                             PayloadId = "payloadId1",
                             ExpectedOutputIds =
                             {
                               "outputId1",
                             },
                             DataDependencies =
                             {
                               "dependencyBlobId",
                             },
                           },
                         },
                       };

    var mockInvoker = new Mock<CallInvoker>();
    var callInvoker = mockInvoker.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);

    var expectedBlobs = new List<BlobInfo>
                        {
                          new()
                          {
                            BlobName  = "dependencyBlob",
                            BlobId    = "dependencyBlobId",
                            SessionId = "sessionId1",
                          },
                        };

    var mockBlobService = new Mock<IBlobService>();
    mockBlobService.SetupCreateBlobMock(expectedBlobs);

    var taskService = MockHelper.GetTasksServiceMock(callInvoker,
                                                     mockBlobService);

    var taskNodes = new List<TaskNode>
                    {
                      new()
                      {
                        Payload = new BlobInfo
                                  {
                                    BlobName  = "payloadId",
                                    BlobId    = "blobId",
                                    SessionId = "sessionId1",
                                  },
                        ExpectedOutputs = new List<BlobInfo>
                                          {
                                            new()
                                            {
                                              BlobName  = "output1",
                                              BlobId    = "outputId1",
                                              SessionId = "sessionId1",
                                            },
                                          },
                        Session = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                        DataDependenciesContent = new Dictionary<string, ReadOnlyMemory<byte>>
                                                  {
                                                    {
                                                      "dependencyBlob", new ReadOnlyMemory<byte>([1, 2, 3])
                                                    },
                                                  },
                      },
                    };

    var result = await taskService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                    taskNodes)
                                  .ConfigureAwait(false);
    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.Not.Null,
                                  "Result should not be null.");
                      Assert.That(result.Count,
                                  Is.EqualTo(1),
                                  "Expected one task info in the response.");
                      Assert.That(result.First()
                                        .TaskId,
                                  Is.EqualTo("taskId1"),
                                  "Expected task ID to match.");
                      Assert.That(result.First()
                                        .PayloadId,
                                  Is.EqualTo("payloadId1"),
                                  "Expected payload ID to match.");
                      Assert.That(result.First()
                                        .ExpectedOutputs.First(),
                                  Is.EqualTo("outputId1"),
                                  "Expected output ID to match.");
                    });
  }

  [Test]
  public void GetTasksDetailedAsyncWithNonExistentTaskIdThrowsException()
  {
    var mockInvoker = new Mock<CallInvoker>();
    mockInvoker.Setup(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<GetTaskRequest, GetTaskResponse>>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CallOptions>(),
                                                        It.IsAny<GetTaskRequest>()))
               .Throws(new RpcException(new Status(StatusCode.NotFound,
                                                   "Task not found")));

    var taskService = MockHelper.GetTasksServiceMock(mockInvoker);

    Assert.That(async () => await taskService.GetTasksDetailedAsync("nonExistentTaskId"),
                Throws.Exception.TypeOf<RpcException>());
  }

  [Test]
  public void CancelTasksAsyncWithNonExistentTaskIdsThrowsException()
  {
    var mockInvoker = new Mock<CallInvoker>();
    mockInvoker.Setup(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CancelTasksRequest, CancelTasksResponse>>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CallOptions>(),
                                                        It.IsAny<CancelTasksRequest>()))
               .Throws(new RpcException(new Status(StatusCode.NotFound,
                                                   "Task not found")));

    var taskService = MockHelper.GetTasksServiceMock(mockInvoker);

    var taskIds = new List<string>
                  {
                    "nonExistentTaskId1",
                    "nonExistentTaskId2",
                  };

    Assert.That(async () => await taskService.CancelTasksAsync(taskIds),
                Throws.Exception.TypeOf<RpcException>());
  }
}
