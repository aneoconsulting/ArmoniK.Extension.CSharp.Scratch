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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Library;
using ArmoniK.Extension.CSharp.Client.Services;

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
  ///   Asynchronously sends a dynamic library blob to a blob service
  /// </summary>
  /// <param name="dynamicLibrary">The dynamic library related to the blob being sent.</param>
  /// <param name="zipPath">File path to the zipped library.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>
  ///   The created <see cref="DllBlob" /> instance with relevant identifiers.
  /// </returns>
  public async Task<DllBlob> SendDllBlobAsync(DynamicLibrary    dynamicLibrary,
                                              string            zipPath,
                                              bool              manualDeletion,
                                              CancellationToken cancellationToken)
    => await armoniKClient_.BlobService.SendDllBlobAsync(sessionInfo_,
                                                         dynamicLibrary,
                                                         zipPath,
                                                         manualDeletion,
                                                         cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Submit a task.
  /// </summary>
  /// <param name="tasks">The tasks to submit</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains a collection of task handles.</returns>
  /// <exception cref="ArgumentException">When the tasks parameter provided is null</exception>
  public async Task<ICollection<TaskHandle>> SubmitAsync(IEnumerable<TaskDefinition> tasks,
                                                         CancellationToken           cancellationToken = default)
  {
    _ = tasks ?? throw new ArgumentException("Tasks parameter should not be null");

    var taskInfos = await armoniKClient_.TasksService.SubmitTasksAsync(sessionInfo_,
                                                                       tasks,
                                                                       cancellationToken)
                                        .ConfigureAwait(false);
    return taskInfos.Select(t => new TaskHandle(armoniKClient_,
                                                t))
                    .ToList();
  }
}
