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
using ArmoniK.Utils;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Services;

public class TasksServiceTests
{
  [Test]
  public async Task CreateTask_ReturnsNewTaskWithId()
  {
    var client = new MockedArmoniKClient();
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
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(submitTaskResponse);
    // Act
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

    var result = await client.TasksService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                            taskNodes);

    // Assert
    var taskInfosEnumerable = result as TaskInfos[] ?? result.ToArray();
    ClassicAssert.AreEqual("taskId1",
                           taskInfosEnumerable.FirstOrDefault()
                                              ?.TaskId);
    ClassicAssert.AreEqual("payloadId1",
                           taskInfosEnumerable.FirstOrDefault()
                                              ?.PayloadId);
    ClassicAssert.AreEqual("blobId1",
                           taskInfosEnumerable.FirstOrDefault()
                                              ?.ExpectedOutputs.First());
  }

  [Test]
  public async Task SubmitTasksAsync_MultipleTasksWithOutputs_ReturnsCorrectResponses()
  {
    // Arrange
    var client = new MockedArmoniKClient();
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
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);
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

    // Act
    var result = await client.TasksService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                            taskNodes)
                             .ToListAsync();
    // Assert
    ClassicAssert.AreEqual(2,
                           result.Count());
    Assert.That(result,
                Has.Some.Matches<TaskInfos>(r => r.TaskId == "taskId1" && r.PayloadId == "payloadId1" && r.ExpectedOutputs.Contains("outputId1")),
                "Result should contain an item with taskId1, payloadId1, and outputId1");
    Assert.That(result,
                Has.Some.Matches<TaskInfos>(r => r.TaskId == "taskId2" && r.PayloadId == "payloadId2" && r.ExpectedOutputs.Contains("outputId2")),
                "Result should contain an item with taskId2, payloadId2, and outputId2");
  }


  [Test]
  public Task SubmitTasksAsync_WithEmptyExpectedOutputs_ThrowsException()
  {
    // Arrange
    var client = new MockedArmoniKClient();
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
                        ExpectedOutputs = new List<BlobInfo>(), // Empty expected outputs
                        Session         = new SessionInfo("sessionId1"),
                        TaskOptions = new TaskConfiguration
                                      {
                                        PartitionId = "subtasking",
                                      },
                      },
                    };

    // Act & Assert
    Assert.ThrowsAsync<InvalidOperationException>(() => client.TasksService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                                                             taskNodes));
    return Task.CompletedTask;
  }

  [Test]
  public async Task SubmitTasksAsync_WithDataDependencies_CreatesBlobsCorrectly()
  {
    var client = new MockedArmoniKClient();

    var expectedBlobs = new List<BlobInfo>
                        {
                          new()
                          {
                            BlobName  = "dependencyBlob",
                            BlobId    = "dependencyBlobId",
                            SessionId = "sessionId1",
                          },
                        };
    client.BlobServiceMock.SetupCreateBlobMock(expectedBlobs);

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
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);

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

    var result = await client.TasksService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                                            taskNodes);

    client.BlobServiceMock.Verify(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                                          It.IsAny<IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                                                          It.IsAny<CancellationToken>()),
                                  Times.Once);
    ClassicAssert.AreEqual("dependencyBlobId",
                           taskNodes.First()
                                    .DataDependencies.First()
                                    .BlobId);
    ClassicAssert.AreEqual("dependencyBlobId",
                           result.First()
                                 .DataDependencies.First());
  }

  [Test]
  public async Task SubmitTasksAsync_EmptyDataDependencies_DoesNotCreateBlobs()
  {
    // Arrange
    var client = new MockedArmoniKClient();

    var expectedBlobs = new List<BlobInfo>
                        {
                          new()
                          {
                            BlobName  = "dependencyBlob",
                            BlobId    = "dependencyBlobId",
                            SessionId = "sessionId1",
                          },
                        };
    client.BlobServiceMock.SetupCreateBlobMock(expectedBlobs);

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
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<SubmitTasksRequest, SubmitTasksResponse>(taskResponse);

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
    // Act
    await client.TasksService.SubmitTasksAsync(new SessionInfo("sessionId1"),
                                               taskNodes);
    // Assert
    client.BlobServiceMock.Verify(m => m.CreateBlobsAsync(It.IsAny<SessionInfo>(),
                                                          It.IsAny<IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                                                          It.IsAny<CancellationToken>()),
                                  Times.Never);
    Assert.That(taskNodes.First()
                         .DataDependencies,
                Is.Empty);
  }
}
