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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Task;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Services;

/// <inheritdoc />
public class SessionService : ISessionService
{
  private readonly ArmoniKClient           armoniKClient_;
  private readonly ObjectPool<ChannelBase> channel_;
  private readonly ILogger<SessionService> logger_;

  private readonly Properties properties_;

  /// <summary>
  ///   Creates an instance of <see cref="SessionService" /> using the specified GRPC channel, application properties, and
  ///   an optional logger factory.
  /// </summary>
  /// <param name="channel">
  ///   An object pool that manages GRPC channels, providing efficient handling and reuse of channel
  ///   resources.
  /// </param>
  /// <param name="properties">A collection of configuration properties used to configure the session service.</param>
  /// <param name="armoniKClient">The ArmoniK client.</param>
  /// <param name="loggerFactory">
  ///   An optional factory for creating loggers, which can be used to enable logging within the
  ///   session service. If null, logging will be disabled.
  /// </param>
  public SessionService(ObjectPool<ChannelBase> channel,
                        Properties              properties,
                        ArmoniKClient           armoniKClient,
                        ILoggerFactory          loggerFactory)
  {
    properties_    = properties;
    logger_        = loggerFactory.CreateLogger<SessionService>();
    channel_       = channel;
    armoniKClient_ = armoniKClient;
  }

  /// <inheritdoc />
  public async Task CancelSessionAsync(SessionInfo       session,
                                       CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.CancelSessionAsync(new CancelSessionRequest
                                           {
                                             SessionId = session.SessionId,
                                           })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task CloseSessionAsync(SessionInfo       session,
                                      CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.CloseSessionAsync(new CloseSessionRequest
                                          {
                                            SessionId = session.SessionId,
                                          })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task PauseSessionAsync(SessionInfo       session,
                                      CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.PauseSessionAsync(new PauseSessionRequest
                                          {
                                            SessionId = session.SessionId,
                                          })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task StopSubmissionAsync(SessionInfo       session,
                                        CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.StopSubmissionAsync(new StopSubmissionRequest
                                            {
                                              SessionId = session.SessionId,
                                            })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task ResumeSessionAsync(SessionInfo       session,
                                       CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.ResumeSessionAsync(new ResumeSessionRequest
                                           {
                                             SessionId = session.SessionId,
                                           })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task PurgeSessionAsync(SessionInfo       session,
                                      CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.PurgeSessionAsync(new PurgeSessionRequest
                                          {
                                            SessionId = session.SessionId,
                                          })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(SessionInfo       session,
                                       CancellationToken cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.DeleteSessionAsync(new DeleteSessionRequest
                                           {
                                             SessionId = session.SessionId,
                                           })
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<SessionInfo> CreateSessionAsync(IEnumerable<string> partitionIds,
                                                    TaskConfiguration   taskOptions,
                                                    CancellationToken   cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var sessionClient = new Sessions.SessionsClient(channel);
    var createSessionReply = await sessionClient.CreateSessionAsync(new CreateSessionRequest
                                                                    {
                                                                      DefaultTaskOption = taskOptions.ToTaskOptions(),
                                                                      PartitionIds =
                                                                      {
                                                                        partitionIds,
                                                                      },
                                                                    })
                                                .ConfigureAwait(false);

    return new SessionInfo(createSessionReply.SessionId);
  }
}
