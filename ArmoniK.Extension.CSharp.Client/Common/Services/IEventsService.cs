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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.Extension.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for handling events related to a session.
/// </summary>
public interface IEventsService
{
  /// <summary>
  ///   Asynchronously waits for the specified blobs to become available in the given session.
  /// </summary>
  /// <param name="session">The session information in which the blobs are expected.</param>
  /// <param name="blobInfos">The collection of blob information objects to wait for.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  Task WaitForBlobsAsync(SessionInfo           session,
                         ICollection<BlobInfo> blobInfos,
                         CancellationToken     cancellationToken = default);
}

/// <summary>
///   Provides extension methods for the <see cref="IEventsService" /> interface.
/// </summary>
public static class EventsServiceExt
{
  /// <summary>
  ///   Asynchronously waits for the specified blobs, identified by their IDs, to become available in the given session.
  /// </summary>
  /// <param name="eventsService">The events service instance.</param>
  /// <param name="session">The session information in which the blobs are expected.</param>
  /// <param name="blobInfos">The collection of blob IDs to wait for.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public static Task WaitForBlobsAsync(this IEventsService eventsService,
                                       SessionInfo         session,
                                       ICollection<string> blobInfos,
                                       CancellationToken   cancellationToken = default)
    => eventsService.WaitForBlobsAsync(session,
                                       blobInfos.Select(info => new BlobInfo
                                                                {
                                                                  BlobId    = info,
                                                                  BlobName  = "",
                                                                  SessionId = session.SessionId,
                                                                })
                                                .ToList(),
                                       cancellationToken);
}
