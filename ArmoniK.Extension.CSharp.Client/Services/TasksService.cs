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

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Handles;
using ArmoniK.Extension.CSharp.Client.Library;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using ITasksService = ArmoniK.Extension.CSharp.Client.Common.Services.ITasksService;
using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;

namespace ArmoniK.Extension.CSharp.Client.Services;

/// <inheritdoc />
public class TasksService : ITasksService
{
  private readonly ArmoniKClient           armoniKClient_;
  private readonly IBlobService            blobService_;
  private readonly ObjectPool<ChannelBase> channelPool_;
  private readonly ILogger<TasksService>   logger_;

  /// <summary>
  ///   Creates an instance of <see cref="TasksService" /> using the specified GRPC channel, blob service, and an optional
  ///   logger factory.
  /// </summary>
  /// <param name="channel">An object pool that manages GRPC channels. This provides efficient handling of channel resources.</param>
  /// <param name="blobService">The blob service.</param>
  /// <param name="armoniKClient">The ArmoniK client.</param>
  /// <param name="loggerFactory">
  ///   An optional logger factory to enable logging within the task service. If null, logging will
  ///   be disabled.
  /// </param>
  public TasksService(ObjectPool<ChannelBase> channel,
                      IBlobService            blobService,
                      ArmoniKClient           armoniKClient,
                      ILoggerFactory          loggerFactory)
  {
    channelPool_   = channel;
    logger_        = loggerFactory.CreateLogger<TasksService>();
    armoniKClient_ = armoniKClient;
    blobService_   = blobService;
  }

  /// <inheritdoc />
  public async Task<ICollection<TaskInfos>> SubmitTasksAsync(SessionInfo           session,
                                                             IEnumerable<TaskNode> taskNodes,
                                                             bool                  manualDeletion    = false,
                                                             CancellationToken     cancellationToken = default)
  {
    var enumerableTaskNodes = taskNodes.ToList();
    // Validate each task node
    if (enumerableTaskNodes.Any(node => node.ExpectedOutputs == null || !node.ExpectedOutputs.Any()))
    {
      throw new InvalidOperationException("Expected outputs cannot be empty.");
    }

    await CreateNewBlobsAsync(session,
                              enumerableTaskNodes,
                              manualDeletion,
                              cancellationToken)
      .ConfigureAwait(false);
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var tasksClient = new Tasks.TasksClient(channel);
    var taskCreations = enumerableTaskNodes.Select(taskNode => new SubmitTasksRequest.Types.TaskCreation
                                                               {
                                                                 PayloadId = taskNode.Payload.BlobId,
                                                                 ExpectedOutputKeys =
                                                                 {
                                                                   taskNode.ExpectedOutputs.Select(i => i.BlobId),
                                                                 },
                                                                 DataDependencies =
                                                                 {
                                                                   taskNode.DataDependencies?.Select(i => i.BlobId) ?? Enumerable.Empty<string>(),
                                                                 },
                                                                 TaskOptions = taskNode.TaskOptions?.ToTaskOptions(),
                                                               })
                                           .ToList();
    var submitTasksRequest = new SubmitTasksRequest
                             {
                               SessionId = session.SessionId,
                               TaskCreations =
                               {
                                 taskCreations.ToList(),
                               },
                             };

    var taskSubmissionResponse = await tasksClient.SubmitTasksAsync(submitTasksRequest,
                                                                    cancellationToken: cancellationToken)
                                                  .ConfigureAwait(false);

    return taskSubmissionResponse.TaskInfos.Select(x => new TaskInfos(x,
                                                                      session.SessionId))
                                 .AsICollection();
  }

  /// <inheritdoc />
  public async Task<ICollection<TaskInfos>> SubmitTasksAsync(SessionInfo                 session,
                                                             IEnumerable<TaskDefinition> taskDefinitions,
                                                             TaskConfiguration           defaultTaskConfiguration,
                                                             CancellationToken           cancellationToken = default)
  {
    // Validate each task node
    if (taskDefinitions.Any(node => !node.Outputs.Any()))
    {
      throw new InvalidOperationException("Expected outputs cannot be empty.");
    }

    var taskCreations = new List<SubmitTasksRequest.Types.TaskCreation>();
    foreach (var task in taskDefinitions)
    {
      var inputs = await FetchInputsBlobInfo(session,
                                             task,
                                             cancellationToken)
                         .ToListAsync(cancellationToken)
                         .ConfigureAwait(false);
      var outputs = await FetchOutputsBlobInfo(session,
                                               task,
                                               cancellationToken)
                          .ToListAsync(cancellationToken)
                          .ConfigureAwait(false);

      var payload = new Payload(inputs.ToDictionary(b => b.BlobName,
                                                    b => b.BlobId),
                                outputs.ToDictionary(b => b.BlobName,
                                                     b => b.BlobId));

      var payloadJson = JsonConvert.SerializeObject(payload);
      task.Payload = await blobService_.CreateBlobAsync(session,
                                                        "payload",
                                                        Encoding.UTF8.GetBytes(payloadJson)
                                                                .AsMemory(),
                                                        cancellationToken: cancellationToken)
                                       .ConfigureAwait(false);
      if (task.TaskOptions == null)
      {
        task.TaskOptions = defaultTaskConfiguration;
      }

      if (task.WorkerLibrary != null)
      {
        task.TaskOptions!.AddDynamicLibrary(task.WorkerLibrary);
        task.TaskOptions.Options[nameof(DynamicLibrary.ConventionVersion)] = DynamicLibrary.ConventionVersion;
        if (task.WorkerLibrary.DllBlob != null)
        {
          inputs.Add(task.WorkerLibrary.DllBlob);
        }
      }

      taskCreations.Add(new SubmitTasksRequest.Types.TaskCreation
                        {
                          PayloadId = task.Payload!.BlobId,
                          ExpectedOutputKeys =
                          {
                            outputs.Select(o => o.BlobId),
                          },
                          DataDependencies =
                          {
                            inputs.Select(i => i.BlobId),
                          },
                          TaskOptions = task.TaskOptions?.ToTaskOptions(),
                        });
    }

    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);
    var tasksClient = new Tasks.TasksClient(channel);
    var submitTasksRequest = new SubmitTasksRequest
                             {
                               SessionId = session.SessionId,
                               TaskCreations =
                               {
                                 taskCreations,
                               },
                             };

    var taskSubmissionResponse = await tasksClient.SubmitTasksAsync(submitTasksRequest,
                                                                    cancellationToken: cancellationToken)
                                                  .ConfigureAwait(false);

    return taskSubmissionResponse.TaskInfos.Select(x => new TaskInfos(x,
                                                                      session.SessionId))
                                 .AsICollection();
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<TaskPage> ListTasksAsync(TaskPagination                             paginationOptions,
                                                         [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);

    var tasksClient = new Tasks.TasksClient(channel);

    var tasks = await tasksClient.ListTasksAsync(new ListTasksRequest
                                                 {
                                                   Filters  = paginationOptions.Filter,
                                                   Page     = paginationOptions.Page,
                                                   PageSize = paginationOptions.PageSize,
                                                   Sort = new ListTasksRequest.Types.Sort
                                                          {
                                                            Direction = paginationOptions.SortDirection.ToGrpc(),
                                                          },
                                                 },
                                                 cancellationToken: cancellationToken)
                                 .ConfigureAwait(false);
    yield return new TaskPage
                 {
                   TotalTasks = tasks.Total,
                   TasksData = tasks.Tasks.Select(x => new Tuple<string, TaskStatus>(x.Id,
                                                                                     x.Status.ToInternalStatus())),
                 };
  }


  /// <inheritdoc />
  public async Task<TaskState> GetTasksDetailedAsync(string            taskId,
                                                     CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);

    var tasksClient = new Tasks.TasksClient(channel);

    var tasks = await tasksClient.GetTaskAsync(new GetTaskRequest
                                               {
                                                 TaskId = taskId,
                                               },
                                               cancellationToken: cancellationToken)
                                 .ConfigureAwait(false);

    return tasks.Task.ToTaskState();
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<TaskDetailedPage> ListTasksDetailedAsync(SessionInfo                                session,
                                                                         TaskPagination                             paginationOptions,
                                                                         [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);

    var tasksClient = new Tasks.TasksClient(channel);

    var tasks = await tasksClient.ListTasksDetailedAsync(new ListTasksRequest
                                                         {
                                                           Filters  = paginationOptions.Filter,
                                                           Page     = paginationOptions.Page,
                                                           PageSize = paginationOptions.PageSize,
                                                           Sort = new ListTasksRequest.Types.Sort
                                                                  {
                                                                    Direction = paginationOptions.SortDirection.ToGrpc(),
                                                                  },
                                                         },
                                                         cancellationToken: cancellationToken)
                                 .ConfigureAwait(false);

    yield return new TaskDetailedPage
                 {
                   TaskDetails = tasks.Tasks.Select(task => task.ToTaskState()),
                   TotalTasks  = tasks.Total,
                 };
  }

  /// <inheritdoc />
  public async Task<ICollection<TaskState>> CancelTasksAsync(IEnumerable<string> taskIds,
                                                             CancellationToken   cancellationToken = default)
  {
    await using var channel = await channelPool_.GetAsync(cancellationToken)
                                                .ConfigureAwait(false);

    var tasksClient = new Tasks.TasksClient(channel);

    var response = await tasksClient.CancelTasksAsync(new CancelTasksRequest
                                                      {
                                                        TaskIds =
                                                        {
                                                          taskIds,
                                                        },
                                                      })
                                    .ConfigureAwait(false);
    return response.Tasks.Select(taskSummary => taskSummary.ToTaskState())
                   .AsICollection();
  }

  private async Task CreateNewBlobsAsync(SessionInfo           session,
                                         IEnumerable<TaskNode> taskNodes,
                                         bool                  manualDeletion,
                                         CancellationToken     cancellationToken)
  {
    var enumerableNodes = taskNodes.ToList();
    var nodesWithNewBlobs = enumerableNodes.Where(x => !Equals(x.DataDependenciesContent,
                                                               ImmutableDictionary<string, ReadOnlyMemory<byte>>.Empty))
                                           .ToList();
    if (nodesWithNewBlobs.Any())
    {
      var blobKeyValues = nodesWithNewBlobs.SelectMany(x => x.DataDependenciesContent);

      var createdBlobDictionary = new Dictionary<string, BlobInfo>();

      await foreach (var blob in blobService_.CreateBlobsAsync(session,
                                                               blobKeyValues,
                                                               manualDeletion,
                                                               cancellationToken)
                                             .ConfigureAwait(false))
      {
        createdBlobDictionary[blob.BlobName] = blob;
      }

      foreach (var taskNode in enumerableNodes)
      foreach (var dependency in taskNode.DataDependenciesContent)
      {
        if (createdBlobDictionary.TryGetValue(dependency.Key,
                                              out var createdBlob))
        {
          taskNode.DataDependencies.Add(createdBlob);
        }
      }
    }

    var nodeWithNewPayloads = enumerableNodes.Where(x => Equals(x.Payload,
                                                                null))
                                             .ToList();

    if (nodeWithNewPayloads.Any())
    {
      var payloadBlobKeyValues = nodeWithNewPayloads.Select(x => x.PayloadContent);

      var payloadBlobDictionary = new Dictionary<string, BlobInfo>();

      await foreach (var blob in blobService_.CreateBlobsAsync(session,
                                                               payloadBlobKeyValues,
                                                               manualDeletion,
                                                               cancellationToken)
                                             .ConfigureAwait(false))
      {
        payloadBlobDictionary[blob.BlobName] = blob;
      }

      foreach (var taskNode in enumerableNodes)
      {
        if (payloadBlobDictionary.TryGetValue(taskNode.PayloadContent.Key,
                                              out var createdBlob))
        {
          taskNode.Payload = createdBlob;
        }
      }
    }
  }

  private async IAsyncEnumerable<BlobInfo> FetchInputsBlobInfo(SessionInfo                                session,
                                                               TaskDefinition                             task,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    // Return the already available blob infos
    foreach (var blobHandle in task.InputHandles.Values)
    {
      yield return blobHandle.BlobInfo;
    }

    // Return blobs already created for the same session
    var blobsOnSameSession = task.InputDefinitions.Values.Where(b => b.BlobHandle != null && b.SessionInfo! == session);
    foreach (var blobDefinition in blobsOnSameSession)
    {
      yield return blobDefinition.BlobHandle!.BlobInfo;
    }

    // new blobs creation, whether they were never created or created on another session
    var newBlobs         = task.InputDefinitions.Where(b => (b.Value.BlobHandle != null && b.Value.SessionInfo! != session) || b.Value.BlobHandle == null);
    var blobsWithoutData = new List<(string name, bool manualDeletion)>();
    foreach (var pair in newBlobs)
    {
      var name           = pair.Key;
      var blobDefinition = pair.Value;
      if (blobDefinition.Data == null)
      {
        blobsWithoutData.Add((name, blobDefinition.ManualDeletion));

        continue;
      }

      var blobInfo = await blobService_.CreateBlobAsync(session,
                                                        name,
                                                        blobDefinition.Data.Value,
                                                        blobDefinition.ManualDeletion,
                                                        cancellationToken)
                                       .ConfigureAwait(false);

      task.InputDefinitions[name].SessionInfo = session;
      task.InputDefinitions[name].BlobHandle = new BlobHandle(blobInfo,
                                                              armoniKClient_);
      yield return blobInfo;
    }

    await foreach (var blobInfo in CreateBlobsMetadataAsync(session,
                                                            task,
                                                            blobsWithoutData,
                                                            task.InputDefinitions,
                                                            cancellationToken)
                     .ConfigureAwait(false))
    {
      yield return blobInfo;
    }
  }

  private async IAsyncEnumerable<BlobInfo> CreateBlobsMetadataAsync(SessionInfo                                     session,
                                                                    TaskDefinition                                  task,
                                                                    IEnumerable<(string name, bool manualDeletion)> blobNames,
                                                                    Dictionary<string, BlobDefinition>              name2BlobDefinition,
                                                                    [EnumeratorCancellation] CancellationToken      cancellationToken)
  {
    if (!blobNames.Any())
    {
      yield break;
    }

    var blobsInfo = blobService_.CreateBlobsMetadataAsync(session,
                                                          blobNames,
                                                          cancellationToken);
    await foreach (var blobInfo in blobsInfo.ConfigureAwait(false))
    {
      name2BlobDefinition[blobInfo.BlobName].SessionInfo = session;
      name2BlobDefinition[blobInfo.BlobName].BlobHandle = new BlobHandle(blobInfo,
                                                                         armoniKClient_);
      yield return blobInfo;
    }
  }

  private async IAsyncEnumerable<BlobInfo> FetchOutputsBlobInfo(SessionInfo                                session,
                                                                TaskDefinition                             task,
                                                                [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var blobInfo in CreateBlobsMetadataAsync(session,
                                                            task,
                                                            task.Outputs.Select(pair => (pair.Key, pair.Value.ManualDeletion)),
                                                            task.Outputs,
                                                            cancellationToken)
                     .ConfigureAwait(false))
    {
      yield return blobInfo;
    }
  }

  private class Payload
  {
    public Payload(IReadOnlyDictionary<string, string> inputs,
                   IReadOnlyDictionary<string, string> outputs)
    {
      Inputs  = inputs;
      Outputs = outputs;
    }

    public IReadOnlyDictionary<string, string> Inputs  { get; }
    public IReadOnlyDictionary<string, string> Outputs { get; }
  }
}
