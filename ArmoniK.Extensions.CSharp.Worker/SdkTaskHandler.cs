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

using System.Runtime.CompilerServices;
using System.Text;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Common.Library;
using ArmoniK.Extensions.CSharp.Worker.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Worker.Interfaces;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Worker.Interfaces.Handles;
using ArmoniK.Utils;

using Google.Protobuf;

using JsonSerializer = System.Text.Json.JsonSerializer;

namespace ArmoniK.Extensions.CSharp.Worker;

/// <summary>
///   Allow a worker to create tasks and populate results
/// </summary>
internal class SdkTaskHandler : ISdkTaskHandler
{
  private readonly ITaskHandler taskHandler_;

  /// <summary>
  ///   Creates a SdkTaskHandler
  /// </summary>
  /// <param name="taskHandler">The task handler</param>
  /// <param name="inputs">The inputs with the task input name as Key, and BlobHandle as value</param>
  /// <param name="outputs">The outputs with the task output name as Key, and BlobHandle as value</param>
  public SdkTaskHandler(ITaskHandler                            taskHandler,
                        IReadOnlyDictionary<string, BlobHandle> inputs,
                        IReadOnlyDictionary<string, BlobHandle> outputs)
  {
    taskHandler_ = taskHandler;
    Inputs       = inputs;
    Outputs      = outputs;
    TaskOptions  = taskHandler_.TaskOptions.ToTaskConfiguration();
  }

  /// <summary>List of options provided when submitting the task.</summary>
  public TaskConfiguration TaskOptions { get; init; }

  /// <summary>Id of the session this task belongs to.</summary>
  public string SessionId
    => taskHandler_.SessionId;

  /// <summary>Task's id being processed.</summary>
  public string TaskId
    => taskHandler_.TaskId;

  /// <summary>
  ///   The data required to compute the task. The key is the name defined by the client, the value is the raw data.
  /// </summary>
  public IReadOnlyDictionary<string, BlobHandle> Inputs { get; }

  /// <summary>
  ///   Result blob ids by name defined by the client.
  /// </summary>
  public IReadOnlyDictionary<string, BlobHandle> Outputs { get; }

  /// <summary>
  ///   Decode a dependency from its raw data
  /// </summary>
  /// <param name="name">The input name defined by the client</param>
  /// <param name="encoding">The encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The decoded string</returns>
  public string GetStringDependency(string    name,
                                    Encoding? encoding = null)
    => (encoding ?? Encoding.UTF8).GetString(Inputs[name].Data!);

  /// <summary>
  ///   Create blobs metadata
  /// </summary>
  /// <param name="names">The collection of blob names to be created</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A BlobHandle collection</returns>
  public async Task<ICollection<BlobHandle>> CreateBlobsMetaDataAsync(IEnumerable<string> names,
                                                                      CancellationToken   cancellationToken = default)
  {
    var blobs = names.Select(n => new CreateResultsMetaDataRequest.Types.ResultCreate
                                  {
                                    Name = n,
                                  });
    var blobsCreationResponse = await taskHandler_.CreateResultsMetaDataAsync(blobs,
                                                                              cancellationToken)
                                                  .ConfigureAwait(false);
    return blobsCreationResponse.Results.Select(b => new BlobHandle(b.ResultId,
                                                                    this))
                                .ToList();
  }

  /// <summary>
  ///   Create blobs with their data in a single request
  /// </summary>
  /// <param name="blobs">The blob names and data</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A BlobHandle collection</returns>
  public async Task<ICollection<BlobHandle>> CreateBlobsAsync(IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>> blobs,
                                                              CancellationToken                                       cancellationToken = default)
  {
    var blobsCreate = blobs.Select(kv => new CreateResultsRequest.Types.ResultCreate
                                         {
                                           Name = kv.Key,
                                           Data = ByteString.CopyFrom(kv.Value.Span),
                                         });
    var response = await taskHandler_.CreateResultsAsync(blobsCreate,
                                                         cancellationToken)
                                     .ConfigureAwait(false);
    return response.Results.Select(b => new BlobHandle(b.ResultId,
                                                       this))
                   .ToList();
  }

  /// <summary>Send the results computed by the task</summary>
  /// <param name="blob">The blob handle.</param>
  /// <param name="data">The data corresponding to the result</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendResultAsync(BlobHandle         blob,
                                    byte[]             data,
                                    CancellationToken? cancellationToken = null)
    => await taskHandler_.SendResult(blob.BlobId,
                                     data,
                                     cancellationToken)
                         .ConfigureAwait(false);

  /// <summary>Send the results computed by the task</summary>
  /// <param name="blob">The blob handle.</param>
  /// <param name="data">The string result</param>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendStringResultAsync(BlobHandle         blob,
                                          string             data,
                                          Encoding?          encoding          = null,
                                          CancellationToken? cancellationToken = null)
    => await taskHandler_.SendResult(blob.BlobId,
                                     (encoding ?? Encoding.UTF8).GetBytes(data),
                                     cancellationToken)
                         .ConfigureAwait(false);

  /// <summary>Submit tasks with existing payloads (results)</summary>
  /// <param name="taskDefinitions">The requests to create tasks</param>
  /// <param name="submissionTaskOptions">optional tasks for the whole submission</param>
  /// <param name="cancellationToken">
  ///   Token used to cancel the execution of the method.
  ///   If null, the cancellation token of the task handler is used
  /// </param>
  /// <returns>A task representing the asynchronous operation. The task result contains the collection of TaskInfos</returns>
  public async Task<ICollection<TaskInfos>> SubmitTasksAsync(ICollection<TaskDefinition> taskDefinitions,
                                                             TaskConfiguration           submissionTaskOptions,
                                                             CancellationToken           cancellationToken = default)
  {
    // Create all input and output blobs
    var allBlobDefinitions = taskDefinitions.SelectMany(t => t.InputDefinitions.Values.Union(t.OutputDefinitions.Values));
    await CreateBlobsAsync(allBlobDefinitions,
                           cancellationToken)
      .ConfigureAwait(false);

    // Compute all payloads
    var payloads = new List<Payload>();
    foreach (var task in taskDefinitions)
    {
      var inputs = task.InputDefinitions.Select(kv => new KeyValuePair<string, string>(kv.Key,
                                                                                       kv.Value.BlobHandle!.BlobId))
                       .ToDictionary();
      var outputs = task.OutputDefinitions.ToDictionary(kv => kv.Key,
                                                        kv => kv.Value.BlobHandle!.BlobId);
      payloads.Add(new Payload(inputs,
                               outputs));
    }

    // Payload blobs creation, creation of TaskCreation instances for submission
    using var taskEnumerator = taskDefinitions.GetEnumerator();
    var       taskCreations  = new List<SubmitTasksRequest.Types.TaskCreation>();
    var payloadBlobs = await CreateBlobsAsync(payloads.Select(p => new KeyValuePair<string, ReadOnlyMemory<byte>>("payload",
                                                                                                                  Encoding.UTF8.GetBytes(JsonSerializer.Serialize(p)))),
                                              cancellationToken)
                         .ConfigureAwait(false);
    foreach (var payloadBlobHandle in payloadBlobs)
    {
      taskEnumerator.MoveNext();
      var task = taskEnumerator.Current;
      task.TaskOptions.Options[nameof(DynamicLibrary.ConventionVersion)] = DynamicLibrary.ConventionVersion;
      var dataDependencies = task.InputDefinitions.Values.Select(b => b.BlobHandle!.BlobId);
      if (task.WorkerLibrary != null)
      {
        task.TaskOptions.SetDynamicLibrary(task.WorkerLibrary);
        dataDependencies = dataDependencies.Concat([task.WorkerLibrary.LibraryBlobId]);
      }

      taskCreations.Add(new SubmitTasksRequest.Types.TaskCreation
                        {
                          PayloadId = payloadBlobHandle.BlobId,
                          ExpectedOutputKeys =
                          {
                            task.OutputDefinitions.Values.Select(b => b.BlobHandle!.BlobId),
                          },
                          DataDependencies =
                          {
                            dataDependencies,
                          },
                          TaskOptions = task.TaskOptions?.ToTaskOptions(),
                        });
    }

    // Send the request
    var response = await taskHandler_.SubmitTasksAsync(taskCreations,
                                                       submissionTaskOptions.ToTaskOptions(),
                                                       cancellationToken)
                                     .ConfigureAwait(false);

    return response.TaskInfos.Select(t => t.ToTaskInfos(taskHandler_.SessionId))
                   .AsICollection();
  }

  private async Task CreateBlobsAsync(IEnumerable<BlobDefinition> blobs,
                                      CancellationToken           cancellationToken = default)
  {
    var blobsWithData    = new List<BlobDefinition>();
    var blobsWithoutData = new List<BlobDefinition>();

    foreach (var blob in blobs)
    {
      if (blob.BlobHandle != null)
      {
        continue;
      }

      if (blob.HasData)
      {
        blob.RefreshFile();
        await blob.FetchDataAsync(cancellationToken)
                  .ConfigureAwait(false);
        blobsWithData.Add(blob);
      }
      else
      {
        blobsWithoutData.Add(blob);
      }
    }

    if (blobsWithData.Any())
    {
      // Creation of blobs with data
      foreach (var blobsWithoutDuplicateName in DeDuplicateWithName(blobsWithData))
      {
        var name2Blob = blobsWithoutDuplicateName.ToDictionary(b => b.Name,
                                                               b => b);
        var response = CreateBlobsWithContentAsync(blobsWithoutDuplicateName,
                                                   cancellationToken);
        await foreach (var blob in response.ConfigureAwait(false))
        {
          name2Blob[blob.Name].BlobHandle = new BlobHandle(blob.ResultId,
                                                           this);
        }
      }
    }

    if (blobsWithoutData.Any())
    {
      // Creation of blobs without data
      foreach (var blobsWithoutDuplicateName in DeDuplicateWithName(blobsWithoutData))
      {
        var name2Blob = blobsWithoutDuplicateName.ToDictionary(b => b.Name,
                                                               b => b);
        var blobsCreate = blobsWithoutDuplicateName.Select(b => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                {
                                                                  Name = b.Name,
                                                                });
        var response = await taskHandler_.CreateResultsMetaDataAsync(blobsCreate,
                                                                     cancellationToken)
                                         .ConfigureAwait(false);
        foreach (var blob in response.Results)
        {
          name2Blob[blob.Name].BlobHandle = new BlobHandle(blob.ResultId,
                                                           this);
        }
      }
    }
  }

  private async IAsyncEnumerable<ResultMetaData> CreateBlobsWithContentAsync(IEnumerable<BlobDefinition>                blobDefinitions,
                                                                             [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    // This is a bin packing kind of problem for which we apply the first-fit-decreasing strategy
    var batches = GetOptimizedBatches(blobDefinitions,
                                      taskHandler_.Configuration.DataChunkMaxSize);

    foreach (var batch in batches)
    {
      var items = batch.Items.Select(b => new CreateResultsRequest.Types.ResultCreate
                                          {
                                            Name = b.Name,
                                            Data = ByteString.CopyFrom(b.Data!.Value.Span),
                                          });

      var blobCreationResponse = await taskHandler_.CreateResultsAsync(items,
                                                                       cancellationToken)
                                                   .ConfigureAwait(false);
      foreach (var blob in blobCreationResponse.Results)
      {
        yield return blob;
      }
    }
  }

  /// <summary>
  ///   Dispatches a list of blob definitions in a minimal number of batches, each batch size being less than the 'maxSize'
  /// </summary>
  /// <param name="blobDefinitions">The list of blob definitions to dispatch</param>
  /// <param name="maxSize">The maximum size for a batch</param>
  /// <returns>The list of batches created</returns>
  private static List<Batch> GetOptimizedBatches(IEnumerable<BlobDefinition> blobDefinitions,
                                                 int                         maxSize)
  {
    var blobsByDescendingSize = blobDefinitions.OrderByDescending(b => b.TotalSize)
                                               .ToList();
    var batches = new List<Batch>();
    foreach (var blob in blobsByDescendingSize)
    {
      var batch = batches.FirstOrDefault(b => maxSize > b.Size + blob.TotalSize);
      if (batch == null)
      {
        batch = new Batch();
        batches.Add(batch);
      }

      batch.AddItem(blob);
    }

    return batches;
  }

  private static List<List<BlobDefinition>> DeDuplicateWithName(List<BlobDefinition> blobsWithData)
  {
    var grouped = blobsWithData.GroupBy(x => x.Name)
                               .Select(g => g.ToList())
                               .ToList();
    var result = new List<List<BlobDefinition>>();

    do
    {
      var currentList = new List<BlobDefinition>();
      foreach (var list in grouped)
      {
        var blob = list.LastOrDefault();
        if (blob != null)
        {
          list.RemoveAt(list.Count - 1);
          currentList.Add(blob);
        }
      }

      if (!currentList.Any())
      {
        break;
      }

      result.Add(currentList);
    } while (true);

    return result;
  }

  private class Batch
  {
    private readonly List<BlobDefinition> items_ = new();

    public IEnumerable<BlobDefinition> Items
      => items_;

    public long Size { get; private set; }

    public void AddItem(BlobDefinition item)
    {
      items_.Add(item);
      Size += item.TotalSize;
    }
  }
}
