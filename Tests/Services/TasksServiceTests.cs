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

using System.Text;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Common.Library;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Moq;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

using TaskStatus = ArmoniK.Extension.CSharp.Common.Common.Domain.Task.TaskStatus;
using V1_TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace Tests.Services;

public class TasksServiceTests
{
  [SetUp]
  public void Setup()
    => MockHelper.InitMock();

  [Test]
  public async Task CreateTaskReturnsNewTaskWithId()
  {
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var taskOption = new TaskConfiguration
                     {
                       PartitionId = "subtasking",
                     };
    var sessionInfo = new SessionInfo("sessionId1");

    // Configure blob metadata creation response
    var outputBlob = (sessionId: sessionInfo.SessionId, blobId: "blobId1", blobName: "blob1");
    mock.ConfigureBlobMetadataCreationResponse(outputBlob);

    // Configure blob creation response
    var payload = (sessionId: sessionInfo.SessionId, blobId: "payloadId1", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(payload)
        .Stop();

    // Configure task submission response
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task =
      (taskId: "taskId1", payloadId: payload.blobId, inputs: null, outputs: [outputBlob.blobId]);
    var taskDefinition = new TaskDefinition().WithOutput(outputBlob.blobName,
                                                         BlobDefinition.CreateOutput(outputBlob.blobName))
                                             .WithTaskOptions(taskOption);
    mock.ConfigureSubmitTaskResponse(task);

    // Configure blob service configuration response
    mock.ConfigureBlobService();

    var result = await client.TasksService.SubmitTasksAsync(sessionInfo,
                                                            [taskDefinition])
                             .ConfigureAwait(false);

    var taskInfosEnumerable = result as TaskInfos[] ?? result.ToArray();
    Assert.Multiple(() =>
                    {
                      Assert.That(taskInfosEnumerable.Length,
                                  Is.EqualTo(1),
                                  "Expected one task info in the response.");
                      Assert.That(taskInfosEnumerable.First()
                                                     .TaskId,
                                  Is.EqualTo(task.taskId),
                                  "Expected task ID to match.");
                      Assert.That(taskInfosEnumerable.First()
                                                     .PayloadId,
                                  Is.EqualTo(task.payloadId),
                                  "Expected payload ID to match.");
                      Assert.That(taskInfosEnumerable.First()
                                                     .ExpectedOutputs.First(),
                                  Is.EqualTo(outputBlob.blobId),
                                  "Expected blob ID to match.");
                    });
  }

  [Test]
  public async Task SubmitTasksAsyncMultipleTasksWithOutputsReturnsCorrectResponses()
  {
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var sessionInfo = new SessionInfo("sessionId1");
    var taskOptions = new TaskConfiguration
                      {
                        PartitionId = "subtasking",
                      };

    // Configure blob metadata creation response
    var outputBlob1 = (sessionId: sessionInfo.SessionId, blobId: "blobId1", blobName: "blob1");
    var outputBlob2 = (sessionId: sessionInfo.SessionId, blobId: "blobId2", blobName: "blob2");
    mock.ConfigureBlobMetadataCreationResponse(outputBlob1,
                                               outputBlob2);

    // Configure payload blobs creation response
    var payload1 = (sessionId: sessionInfo.SessionId, blobId: "payloadId1", blobName: "payload");
    var payload2 = (sessionId: sessionInfo.SessionId, blobId: "payloadId2", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(payload1,
                                               payload2)
        .Stop();

    // Configure task submission response
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task1 =
      (taskId: "taskId1", payloadId: payload1.blobId, inputs: null, outputs: [outputBlob1.blobId]);
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task2 =
      (taskId: "taskId2", payloadId: payload2.blobId, inputs: null, outputs: [outputBlob2.blobId]);
    var taskDefinition1 = new TaskDefinition().WithOutput(outputBlob1.blobName,
                                                          BlobDefinition.CreateOutput(outputBlob1.blobName))
                                              .WithTaskOptions(taskOptions);
    var taskDefinition2 = new TaskDefinition().WithOutput(outputBlob2.blobName,
                                                          BlobDefinition.CreateOutput(outputBlob2.blobName))
                                              .WithTaskOptions(taskOptions);
    mock.ConfigureSubmitTaskResponse(task1,
                                     task2);

    // Configure blob service configuration response
    mock.ConfigureBlobService();

    var result = await client.TasksService.SubmitTasksAsync(sessionInfo,
                                                            [taskDefinition1, taskDefinition2])
                             .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.Not.Null,
                                  "Result should not be null.");
                      Assert.That(result.Count,
                                  Is.EqualTo(2),
                                  "Expected two task infos in the response.");
                      Assert.That(result.Select(t => t.TaskId),
                                  Is.EqualTo(new[]
                                             {
                                               task1.taskId,
                                               task2.taskId,
                                             }),
                                  "Expected task IDs to match.");
                      Assert.That(result.Select(t => t.PayloadId),
                                  Is.EqualTo(new[]
                                             {
                                               payload1.blobId,
                                               payload2.blobId,
                                             }),
                                  "Expected payload IDs to match.");
                    });
  }

  [Test]
  public void SubmitTasksAsyncWithEmptyExpectedOutputsThrowsException()
  {
    // Arrange
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var sessionInfo = new SessionInfo("sessionId1");
    var taskOptions = new TaskConfiguration
                      {
                        PartitionId = "subtasking",
                      };

    // Configure payload blob creation response
    var payload = (sessionId: sessionInfo.SessionId, blobId: "payloadId1", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(payload)
        .Stop();

    var taskDefinition = new TaskDefinition().WithTaskOptions(taskOptions);

    Assert.That(async () => await client.TasksService.SubmitTasksAsync(sessionInfo,
                                                                       [taskDefinition])
                                        .ConfigureAwait(false),
                Throws.Exception.TypeOf<InvalidOperationException>());
  }

  [Test]
  public async Task SubmitTasksAsyncWithDataDependenciesCreatesBlobsCorrectly()
  {
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var sessionInfo = new SessionInfo("sessionId1");
    var taskOptions = new TaskConfiguration
                      {
                        PartitionId = "subtasking",
                      };

    // Configure blob service configuration response
    mock.ConfigureBlobService();

    // Configure blobs creation response
    var dependency = (sessionId: sessionInfo.SessionId, blobId: "dependencyBlobId", blobName: "dependencyBlob");
    var payload    = (sessionId: sessionInfo.SessionId, blobId: "payloadId", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(dependency)
        .ConfigureBlobCreationResponseSequence(payload)
        .Stop();

    // Configure blob metadata creation response
    var outputBlob = (sessionId: sessionInfo.SessionId, blobId: "outputId1", blobName: "output1");
    mock.ConfigureBlobMetadataCreationResponse(outputBlob);

    // Configure task submission response
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task =
      (taskId: "taskId1", payloadId: payload.blobId, inputs: [dependency.blobId], outputs: [outputBlob.blobId]);
    var taskDefinition = new TaskDefinition().WithTaskOptions(taskOptions)
                                             .WithInput(dependency.blobName,
                                                        BlobDefinition.FromByteArray(dependency.blobName,
                                                                                     [1, 2, 3]))
                                             .WithOutput(outputBlob.blobName,
                                                         BlobDefinition.CreateOutput(outputBlob.blobName));
    mock.ConfigureSubmitTaskResponse(task);

    var result = await client.TasksService.SubmitTasksAsync(sessionInfo,
                                                            [taskDefinition])
                             .ConfigureAwait(false);

    var dependencyData = mock.GetBlobDataSent(dependency.blobName);

    mock.CheckConfigureBlobCreationResponseCount();
    Assert.Multiple(() =>
                    {
                      Assert.That(dependencyData,
                                  Is.EqualTo((byte[]) [1, 2, 3]));
                      Assert.That(dependency.blobId,
                                  Is.EqualTo(result.First()
                                                   .DataDependencies.First()),
                                  "Expected data dependency blob ID to match.");
                      Assert.That(dependency.blobId,
                                  Is.EqualTo(taskDefinition.InputDefinitions.First()
                                                           .Value.BlobHandle!.BlobInfo.BlobId),
                                  "Expected data dependency blob ID in task definition to match.");
                    });
  }

  [Test]
  public async Task SubmitTasksAsyncCheckInteroperabilityConvention()
  {
    // Arrange
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var sessionInfo = new SessionInfo("sessionId1");
    var taskOptions = new TaskConfiguration
                      {
                        PartitionId = "subtasking",
                      };

    var lib = new DynamicLibrary
              {
                Symbol        = "Test",
                LibraryPath   = "C:\\Test",
                LibraryBlobId = "libBlobId",
              };

    // Configure blob service configuration response
    mock.ConfigureBlobService();

    // Configure blobs creation response
    var dependency = (sessionId: sessionInfo.SessionId, blobId: "dependencyBlobId", blobName: "dependencyBlob");
    var payload    = (sessionId: sessionInfo.SessionId, blobId: "payloadId", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(dependency)
        .ConfigureBlobCreationResponseSequence(payload)
        .Stop();

    // Configure blob metadata creation response
    var outputBlob = (sessionId: sessionInfo.SessionId, blobId: "outputId1", blobName: "output1");
    mock.ConfigureBlobMetadataCreationResponse(outputBlob);

    // Configure task submission response
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task =
      (taskId: "taskId1", payloadId: payload.blobId, inputs: [dependency.blobId], outputs: [outputBlob.blobId]);
    var taskDefinition = new TaskDefinition().WithTaskOptions(taskOptions)
                                             .WithInput(dependency.blobName,
                                                        BlobDefinition.FromByteArray(dependency.blobName,
                                                                                     [1, 2, 3]))
                                             .WithOutput(outputBlob.blobName,
                                                         BlobDefinition.CreateOutput(outputBlob.blobName))
                                             .WithLibrary(lib);
    mock.ConfigureSubmitTaskResponse(task);

    await client.TasksService.SubmitTasksAsync(sessionInfo,
                                               [taskDefinition])
                .ConfigureAwait(false);

    var payloadData = mock.GetBlobDataSent(payload.blobName);
    var payloadStr  = Encoding.UTF8.GetString(payloadData);

    mock.CheckConfigureBlobCreationResponseCount();
    Assert.Multiple(() =>
                    {
                      Assert.That(payloadStr,
                                  Is.EqualTo("{\"inputs\":{\"dependencyBlob\":\"dependencyBlobId\"},\"outputs\":{\"output1\":\"outputId1\"}}"),
                                  "Bad payload");
                      Assert.That(taskDefinition.TaskOptions.Options["ConventionVersion"],
                                  Is.EqualTo("v1"),
                                  "Expected convention version v1.");
                      Assert.That(taskDefinition.TaskOptions.Options["LibraryBlobId"],
                                  Is.EqualTo(lib.LibraryBlobId),
                                  "Expected library blob id being lib.LibraryBlobId.");
                      Assert.That(taskDefinition.TaskOptions.Options["Symbol"],
                                  Is.EqualTo(lib.Symbol),
                                  "Expected symbol being lib.Symbol.");
                      Assert.That(taskDefinition.TaskOptions.Options["LibraryPath"],
                                  Is.EqualTo(lib.LibraryPath),
                                  "Expected library path being lib.LibraryPath.");
                    });
  }

  [Test]
  public async Task ListTasksAsyncWithPaginationReturnsCorrectPage()
  {
    var client = new MockedArmoniKClient();

    var taskResponse = new ListTasksResponse
                       {
                         Tasks =
                         {
                           new TaskSummary
                           {
                             Id        = "taskId1",
                             Status    = (V1_TaskStatus)TaskStatus.Completed,
                             CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                             StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                             EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-1)),
                           },
                           new TaskSummary
                           {
                             Id        = "taskId2",
                             Status    = (V1_TaskStatus)TaskStatus.Cancelling,
                             CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
                             StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-10)),
                             EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                           },
                         },
                         Total = 2,
                       };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListTasksRequest, ListTasksResponse>(taskResponse);

    var paginationOptions = new TaskPagination
                            {
                              Page          = 1,
                              PageSize      = 10,
                              SortDirection = SortDirection.Asc,
                              Total         = 2,
                              Filter        = new Filters(),
                            };

    var result = await client.TasksService.ListTasksAsync(paginationOptions)
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
    var client = new MockedArmoniKClient();

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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<GetTaskRequest, GetTaskResponse>(taskResponse);

    var result = await client.TasksService.GetTasksDetailedAsync("taskId1")
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
    var client = new MockedArmoniKClient();

    var taskResponse = new ListTasksDetailedResponse
                       {
                         Tasks =
                         {
                           new TaskDetailed
                           {
                             Id        = "taskId1",
                             Status    = V1_TaskStatus.Completed,
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
                             Status    = V1_TaskStatus.Cancelling,
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
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListTasksRequest, ListTasksDetailedResponse>(taskResponse);

    var paginationOptions = new TaskPagination
                            {
                              Page          = 1,
                              PageSize      = 10,
                              Filter        = new Filters(),
                              SortDirection = SortDirection.Asc,
                            };

    var result = await client.TasksService.ListTasksDetailedAsync(new SessionInfo("sessionId1"),
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
    var client = new MockedArmoniKClient();

    var cancelResponse = new CancelTasksResponse
                         {
                           Tasks =
                           {
                             new TaskSummary
                             {
                               Id        = "taskId1",
                               Status    = V1_TaskStatus.Cancelled,
                               CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-10)),
                               StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-5)),
                               EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow),
                               SessionId = "sessionId1",
                               PayloadId = "payloadId1",
                             },
                             new TaskSummary
                             {
                               Id        = "taskId2",
                               Status    = V1_TaskStatus.Cancelled,
                               CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-8)),
                               StartedAt = Timestamp.FromDateTime(DateTime.UtcNow.AddMinutes(-3)),
                               EndedAt   = Timestamp.FromDateTime(DateTime.UtcNow),
                               SessionId = "sessionId1",
                               PayloadId = "payloadId2",
                             },
                           },
                         };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<CancelTasksRequest, CancelTasksResponse>(cancelResponse);

    var taskIds = new List<string>
                  {
                    "taskId1",
                    "taskId2",
                  };

    var result = await client.TasksService.CancelTasksAsync(taskIds)
                             .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(result,
                                  Is.Not.Null);
                      Assert.That(result.Count,
                                  Is.EqualTo(2));

                      var resultList = result.ToList();
                      Assert.That(resultList[0].TaskId,
                                  Is.EqualTo("taskId1"));
                      Assert.That(resultList[0].Status,
                                  Is.EqualTo(TaskStatus.Cancelled));
                      Assert.That(resultList[1].TaskId,
                                  Is.EqualTo("taskId2"));
                      Assert.That(resultList[1].Status,
                                  Is.EqualTo(TaskStatus.Cancelled));
                    });
  }

  [Test]
  public async Task CreateNewBlobsAsyncCreatesBlobsCorrectly()
  {
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var sessionInfo = new SessionInfo("sessionId1");
    var taskOptions = new TaskConfiguration
                      {
                        PartitionId = "subtasking",
                      };

    // Configure blob service configuration response
    mock.ConfigureBlobService();

    // Configure blobs creation response
    var dependency = (sessionId: sessionInfo.SessionId, blobId: "dependencyBlobId", blobName: "dependencyBlob");
    var payload    = (sessionId: sessionInfo.SessionId, blobId: "payloadId", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(dependency)
        .ConfigureBlobCreationResponseSequence(payload)
        .Stop();

    // Configure blob metadata creation response
    var outputBlob = (sessionId: sessionInfo.SessionId, blobId: "outputId1", blobName: "output1");
    mock.ConfigureBlobMetadataCreationResponse(outputBlob);

    // Configure task submission response
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task =
      (taskId: "taskId1", payloadId: payload.blobId, inputs: [dependency.blobId], outputs: [outputBlob.blobId]);
    var taskDefinition = new TaskDefinition().WithTaskOptions(taskOptions)
                                             .WithInput("dependencyBlob",
                                                        BlobDefinition.FromByteArray("dependencyBlob",
                                                                                     [1, 2, 3]))
                                             .WithOutput("output1",
                                                         BlobDefinition.CreateOutput("output1"));
    mock.ConfigureSubmitTaskResponse(task);

    var result = await client.TasksService.SubmitTasksAsync(sessionInfo,
                                                            [taskDefinition])
                             .ConfigureAwait(false);

    Assert.That(result,
                Is.Not.Null,
                "Result should not be null.");
    mock.CheckConfigureBlobCreationResponseCount();
  }

  [Test]
  public async Task SubmitTasksAsyncWithDataDependenciesReturnsCorrectResponses()
  {
    var client = new MockedArmoniKClient();
    var mock   = client.CallInvokerMock;

    var sessionInfo = new SessionInfo("sessionId1");
    var taskOptions = new TaskConfiguration
                      {
                        PartitionId = "subtasking",
                      };

    // Configure blob service configuration response
    mock.ConfigureBlobService();

    // Configure blobs creation response
    var dependency = (sessionId: sessionInfo.SessionId, blobId: "dependencyBlobId", blobName: "dependencyBlob");
    var payload    = (sessionId: sessionInfo.SessionId, blobId: "payloadId", blobName: "payload");
    mock.ConfigureBlobCreationResponseSequence(dependency)
        .ConfigureBlobCreationResponseSequence(payload)
        .Stop();

    // Configure blob metadata creation response
    var outputBlob = (sessionId: sessionInfo.SessionId, blobId: "outputId1", blobName: "output1");
    mock.ConfigureBlobMetadataCreationResponse(outputBlob);

    // Configure task submission response
    (string taskId, string payloadId, string[]? inputs, string[]? outputs) task =
      (taskId: "taskId1", payloadId: payload.blobId, inputs: [dependency.blobId], outputs: [outputBlob.blobId]);
    var taskDefinition = new TaskDefinition().WithTaskOptions(taskOptions)
                                             .WithInput("dependencyBlob",
                                                        BlobDefinition.FromByteArray("dependencyBlob",
                                                                                     [1, 2, 3]))
                                             .WithOutput("output1",
                                                         BlobDefinition.CreateOutput("output1"));
    mock.ConfigureSubmitTaskResponse(task);

    var result = await client.TasksService.SubmitTasksAsync(sessionInfo,
                                                            [taskDefinition])
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
                                  Is.EqualTo(task.taskId),
                                  "Expected task ID to match.");
                      Assert.That(result.First()
                                        .PayloadId,
                                  Is.EqualTo(payload.blobId),
                                  "Expected payload ID to match.");
                      Assert.That(result.First()
                                        .ExpectedOutputs.First(),
                                  Is.EqualTo(outputBlob.blobId),
                                  "Expected output ID to match.");
                    });
  }

  [Test]
  public void GetTasksDetailedAsyncWithNonExistentTaskIdThrowsException()
  {
    var client = new MockedArmoniKClient();
    client.CallInvokerMock.Setup(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<GetTaskRequest, GetTaskResponse>>(),
                                                                   It.IsAny<string>(),
                                                                   It.IsAny<CallOptions>(),
                                                                   It.IsAny<GetTaskRequest>()))
          .Throws(new RpcException(new Status(StatusCode.NotFound,
                                              "Task not found")));

    Assert.That(async () => await client.TasksService.GetTasksDetailedAsync("nonExistentTaskId"),
                Throws.Exception.TypeOf<RpcException>());
  }

  [Test]
  public void CancelTasksAsyncWithNonExistentTaskIdsThrowsException()
  {
    var client = new MockedArmoniKClient();
    client.CallInvokerMock.Setup(invoker => invoker.AsyncUnaryCall(It.IsAny<Method<CancelTasksRequest, CancelTasksResponse>>(),
                                                                   It.IsAny<string>(),
                                                                   It.IsAny<CallOptions>(),
                                                                   It.IsAny<CancelTasksRequest>()))
          .Throws(new RpcException(new Status(StatusCode.NotFound,
                                              "Task not found")));

    var taskIds = new List<string>
                  {
                    "nonExistentTaskId1",
                    "nonExistentTaskId2",
                  };

    Assert.That(async () => await client.TasksService.CancelTasksAsync(taskIds),
                Throws.Exception.TypeOf<RpcException>());
  }
}
