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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Newtonsoft.Json;

namespace ArmoniK.Extension.CSharp.Client.Handles;

/// <summary>
///   Handles session management operations for an ArmoniK client, providing methods to control the lifecycle of a
///   session.
/// </summary>
public class SessionHandle
{
  private readonly ArmoniKClient armoniKClient_;
  private readonly SessionInfo   sessionInfo_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionHandle" /> class.
  /// </summary>
  /// <param name="session">The session information for managing session-related operations.</param>
  /// <param name="armoniKClient">The ArmoniK client used to perform operations on the session.</param>
  public SessionHandle(SessionInfo   session,
                       ArmoniKClient armoniKClient)
  {
    armoniKClient_ = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
    sessionInfo_   = session       ?? throw new ArgumentNullException(nameof(session));
  }

  /// <summary>
  ///   Implicit conversion operator from SessionHandle to SessionInfo.
  ///   Allows SessionHandle to be used wherever SessionInfo is expected.
  /// </summary>
  /// <param name="sessionHandle">The SessionHandle to convert.</param>
  /// <returns>The SessionInfo contained within the SessionHandle.</returns>
  /// <exception cref="ArgumentNullException">Thrown when sessionHandle is null.</exception>
  public static implicit operator SessionInfo(SessionHandle sessionHandle)
    => sessionHandle?.sessionInfo_ ?? throw new ArgumentNullException(nameof(sessionHandle));

  /// <summary>
  ///   Creates a SessionHandle from SessionInfo and ArmoniKClient.
  /// </summary>
  /// <param name="sessionInfo">The SessionInfo to wrap.</param>
  /// <param name="armoniKClient">The ArmoniK client for operations.</param>
  /// <returns>A new SessionHandle instance.</returns>
  /// <exception cref="ArgumentNullException">Thrown when sessionInfo or armoniKClient is null.</exception>
  public static SessionHandle FromSessionInfo(SessionInfo   sessionInfo,
                                              ArmoniKClient armoniKClient)
    => new(sessionInfo   ?? throw new ArgumentNullException(nameof(sessionInfo)),
           armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient)));

  /// <summary>
  ///   Cancels the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task CancelSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.CancelSessionAsync(sessionInfo_,
                                                              cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Closes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task CloseSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.CloseSessionAsync(sessionInfo_,
                                                             cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Pauses the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task PauseSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.PauseSessionAsync(sessionInfo_,
                                                             cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Stops submissions to the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task StopSubmissionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.StopSubmissionAsync(sessionInfo_,
                                                               cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Resumes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task ResumeSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.ResumeSessionAsync(sessionInfo_,
                                                              cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Purges the session asynchronously, removing all data associated with it.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task PurgeSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.PurgeSessionAsync(sessionInfo_,
                                                             cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Deletes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task DeleteSessionAsync(CancellationToken cancellationToken)
    => await armoniKClient_.SessionService.DeleteSessionAsync(sessionInfo_,
                                                              cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Submit a task.
  /// </summary>
  /// <param name="task">The task to submit</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns></returns>
  /// <exception cref="ArgumentException">When the task provided is null</exception>
  public async Task<TaskHandle> SubmitAsync(TaskDefinition    task,
                                            CancellationToken cancellationToken = default)
  {
    _ = task ?? throw new ArgumentException("Task parameter should not be null");

    var inputs = await FetchInputsBlobInfo(task,
                                           cancellationToken)
                       .ToListAsync(cancellationToken)
                       .ConfigureAwait(false);
    var outputs = await FetchOutputsBlobInfo(task,
                                             cancellationToken)
                        .ToListAsync(cancellationToken)
                        .ConfigureAwait(false);

    var payload = new Payload(inputs.ToDictionary(b => b.BlobName,
                                                  b => b.BlobId),
                              outputs.ToDictionary(b => b.BlobName,
                                                   b => b.BlobId));

    var payloadJson = JsonConvert.SerializeObject(payload);
    var payloadBlob = await armoniKClient_.BlobService.CreateBlobAsync(sessionInfo_,
                                                                       "payload",
                                                                       Encoding.UTF8.GetBytes(payloadJson)
                                                                               .AsMemory(),
                                                                       cancellationToken: cancellationToken)
                                          .ConfigureAwait(false);

    var taskNode = new TaskNode
                   {
                     DataDependencies = inputs,
                     ExpectedOutputs  = outputs,
                     Session          = sessionInfo_,
                     Payload          = payloadBlob,
                   };
    if (task.TaskOptions != null)
    {
      taskNode.TaskOptions = task.TaskOptions;
    }

    var taskInfos = await armoniKClient_.TasksService.SubmitTasksAsync(sessionInfo_,
                                                                       [taskNode],
                                                                       cancellationToken: cancellationToken)
                                        .ConfigureAwait(false);
    return new TaskHandle(armoniKClient_,
                          taskInfos.First());
  }

  private async IAsyncEnumerable<BlobInfo> FetchInputsBlobInfo(TaskDefinition                             task,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    // Return the already available blob infos
    foreach (var blobHandle in task.InputHandles.Values)
    {
      yield return blobHandle.BlobInfo;
    }

    // Return blobs already created for the same session
    var blobsOnSameSession = task.InputDefinitions.Values.Where(b => b.BlobHandle != null && b.SessionHandle == this);
    foreach (var blobDefinition in blobsOnSameSession)
    {
      yield return blobDefinition.BlobHandle!.BlobInfo;
    }

    // new blobs creation, whether they were never created or created on another session
    var newBlobs = task.InputDefinitions.Where(b => (b.Value.BlobHandle != null && b.Value.SessionHandle != this) || b.Value.BlobHandle == null);
    foreach (var pair in newBlobs)
    {
      var      name           = pair.Key;
      var      blobDefinition = pair.Value;
      BlobInfo blobInfo;
      if (blobDefinition.Data == null)
      {
        // Creation of the metadata only as there is no data
        blobInfo = await armoniKClient_.BlobService.CreateBlobsMetadataAsync(sessionInfo_,
                                                                             [name],
                                                                             blobDefinition.ManualDeletion,
                                                                             cancellationToken)
                                       .FirstAsync(cancellationToken)
                                       .ConfigureAwait(false);
      }
      else
      {
        blobInfo = await armoniKClient_.BlobService.CreateBlobAsync(sessionInfo_,
                                                                    name,
                                                                    blobDefinition.Data.Value,
                                                                    blobDefinition.ManualDeletion,
                                                                    cancellationToken)
                                       .ConfigureAwait(false);
      }

      task.InputDefinitions[name].SessionHandle = this;
      task.InputDefinitions[name].BlobHandle = new BlobHandle(blobInfo,
                                                              armoniKClient_);
      yield return blobInfo;
    }
  }

  private async IAsyncEnumerable<BlobInfo> FetchOutputsBlobInfo(TaskDefinition                             task,
                                                                [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    foreach (var pair in task.Outputs)
    {
      var name           = pair.Key;
      var blobDefinition = pair.Value;
      var blobInfo = await armoniKClient_.BlobService.CreateBlobsMetadataAsync(sessionInfo_,
                                                                               [name],
                                                                               blobDefinition.ManualDeletion,
                                                                               cancellationToken)
                                         .FirstAsync(cancellationToken)
                                         .ConfigureAwait(false);
      task.Outputs[name].SessionHandle = this;
      task.Outputs[name].BlobHandle = new BlobHandle(blobInfo,
                                                     armoniKClient_);
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
