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
                                                             CancellationToken           cancellationToken = default)
  {
    // Validate each task node
    if (taskDefinitions.Any(node => !node.Outputs.Any()))
    {
      throw new InvalidOperationException("Expected outputs cannot be empty.");
    }

    // Create all blobs from blob definitions
    await blobService_.CreateBlobsAsync(session,
                                        taskDefinitions.SelectMany(t => t.InputDefinitions.Values.Union(t.Outputs.Values)),
                                        cancellationToken)
                      .ConfigureAwait(false);

    // Payload instances creation
    var payloads     = new List<Payload>();
    var tasksInputs  = new List<IEnumerable<KeyValuePair<string, string>>>();
    var tasksOutputs = new List<IEnumerable<KeyValuePair<string, string>>>();
    foreach (var task in taskDefinitions)
    {
      var inputs = task.InputDefinitions.Select(i => new KeyValuePair<string, string>(i.Value.Name,
                                                                                      i.Value.BlobHandle!.BlobInfo.BlobId))
                       .ToList();
      var outputs = task.Outputs.Select(o => new KeyValuePair<string, string>(o.Value.Name,
                                                                              o.Value.BlobHandle!.BlobInfo.BlobId));

      var payload = new Payload(inputs.ToDictionary(b => b.Key,
                                                    b => b.Value),
                                outputs.ToDictionary(b => b.Key,
                                                     b => b.Value));
      payloads.Add(payload);
      if (task.WorkerLibrary != null)
      {
        task.TaskOptions!.AddDynamicLibrary(task.WorkerLibrary);
        task.TaskOptions.Options[nameof(DynamicLibrary.ConventionVersion)] = DynamicLibrary.ConventionVersion;
        if (task.WorkerLibrary.DllBlob != null)
        {
          inputs.Add(new KeyValuePair<string, string>(task.WorkerLibrary.DllBlob.BlobName,
                                                      task.WorkerLibrary.DllBlob.BlobId));
        }
      }

      tasksInputs.Add(inputs);
      tasksOutputs.Add(outputs);
    }

    // Payload blobs creation, creation of TaskCreation instances for submission
    using var taskEnumerator = taskDefinitions.GetEnumerator();
    var       index          = 0;
    var       taskCreations  = new List<SubmitTasksRequest.Types.TaskCreation>();
    var       payloadsData   = payloads.Select(p => ("payload", (ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(p)), false));
    await foreach (var payloadInfo in blobService_.CreateBlobsAsync(session,
                                                                    payloadsData,
                                                                    cancellationToken)
                                                  .ConfigureAwait(false))
    {
      taskEnumerator.MoveNext();
      var task = taskEnumerator.Current;
      task!.Payload = payloadInfo;
      taskCreations.Add(new SubmitTasksRequest.Types.TaskCreation
                        {
                          PayloadId = task.Payload!.BlobId,
                          ExpectedOutputKeys =
                          {
                            tasksOutputs[index]
                              .Select(o => o.Value),
                          },
                          DataDependencies =
                          {
                            tasksInputs[index]
                              .Select(i => i.Value),
                          },
                          TaskOptions = task.TaskOptions?.ToTaskOptions(),
                        });
      index++;
    }

    // Task submission
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
      var blobKeyValues = nodesWithNewBlobs.SelectMany(x => x.DataDependenciesContent.Select(dd => (dd.Key, dd.Value, manualDeletion)));

      var createdBlobDictionary = new Dictionary<string, BlobInfo>();

      await foreach (var blob in blobService_.CreateBlobsAsync(session,
                                                               blobKeyValues,
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
      var payloadBlobKeyValues = nodeWithNewPayloads.Select(x => (x.PayloadContent.Key, x.PayloadContent.Value, false));

      var payloadBlobDictionary = new Dictionary<string, BlobInfo>();

      await foreach (var blob in blobService_.CreateBlobsAsync(session,
                                                               payloadBlobKeyValues,
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
