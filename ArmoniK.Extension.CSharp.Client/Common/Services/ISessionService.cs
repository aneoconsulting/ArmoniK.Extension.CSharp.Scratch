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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Task;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for managing sessions, including creating, canceling, closing, and modifying sessions.
/// </summary>
public interface ISessionService
{
  /// <summary>
  ///   Asynchronously creates a new session.
  /// </summary>
  /// <param name="partitionIds">Partitions related to opened session</param>
  /// <param name="taskOptions">Default task options for the session</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the created session info.</returns>
  Task<SessionInfo> CreateSessionAsync(IEnumerable<string> partitionIds,
                                       TaskConfiguration   taskOptions,
                                       CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Asynchronously cancels a specified session.
  /// </summary>
  /// <param name="session">The session information to cancel.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task CancelSessionAsync(SessionInfo       session,
                          CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously closes a specified session.
  /// </summary>
  /// <param name="session">The session information to close.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task CloseSessionAsync(SessionInfo       session,
                         CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously pauses a specified session.
  /// </summary>
  /// <param name="session">The session information to pause.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task PauseSessionAsync(SessionInfo       session,
                         CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously stops submissions for a specified session.
  /// </summary>
  /// <param name="session">The session information to stop submissions for.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task StopSubmissionAsync(SessionInfo       session,
                           CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously resumes a specified session.
  /// </summary>
  /// <param name="session">The session information to resume.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task ResumeSessionAsync(SessionInfo       session,
                          CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously purges a specified session, removing all data associated with it.
  /// </summary>
  /// <param name="session">The session information to purge.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task PurgeSessionAsync(SessionInfo       session,
                         CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously deletes a specified session.
  /// </summary>
  /// <param name="session">The session information to delete.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task DeleteSessionAsync(SessionInfo       session,
                          CancellationToken cancellationToken = default);
}
