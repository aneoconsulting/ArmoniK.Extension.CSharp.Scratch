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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Services;

internal class SessionService : ISessionService
{
  private readonly ObjectPool<ChannelBase> channel_;
  private readonly ILogger<SessionService> logger_;

  private readonly Properties properties_;

  /// <summary>
  ///   Creates an instance of <see cref="ISessionService" /> using the specified GRPC channel, application properties, and
  ///   an optional logger factory.
  /// </summary>
  /// <param name="channel">
  ///   An object pool that manages GRPC channels, providing efficient handling and reuse of channel
  ///   resources.
  /// </param>
  /// <param name="properties">A collection of configuration properties used to configure the session service.</param>
  /// <param name="loggerFactory">
  ///   An optional factory for creating loggers, which can be used to enable logging within the
  ///   session service. If null, logging will be disabled.
  /// </param>
  /// <returns>An instance of <see cref="ISessionService" /> configured with the provided parameters.</returns>
  public SessionService(ObjectPool<ChannelBase> channel,
                        Properties              properties,
                        ILoggerFactory          loggerFactory)
  {
    properties_ = properties;
    logger_     = loggerFactory.CreateLogger<SessionService>();
    channel_    = channel;
  }

  public async Task<SessionInfo> CreateSessionAsync(TaskConfiguration   taskOptions,
                                                    IEnumerable<string> partitionIds,
                                                    CancellationToken   cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    var createSessionReply = await sessionClient.CreateSessionAsync(new CreateSessionRequest
                                                                    {
                                                                      DefaultTaskOption = taskOptions.ToTaskOptions(),
                                                                      PartitionIds =
                                                                      {
                                                                        partitionIds,
                                                                      },
                                                                    });

    return new SessionInfo(createSessionReply.SessionId);
  }

  public async Task CancelSessionAsync(SessionInfo       session,
                                       CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.CancelSessionAsync(new CancelSessionRequest
                                           {
                                             SessionId = session.SessionId,
                                           });
  }

  public async Task CloseSessionAsync(SessionInfo       session,
                                      CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.CloseSessionAsync(new CloseSessionRequest
                                          {
                                            SessionId = session.SessionId,
                                          });
  }

  public async Task PauseSessionAsync(SessionInfo       session,
                                      CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.PauseSessionAsync(new PauseSessionRequest
                                          {
                                            SessionId = session.SessionId,
                                          });
  }

  public async Task StopSubmissionAsync(SessionInfo       session,
                                        CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.StopSubmissionAsync(new StopSubmissionRequest
                                            {
                                              SessionId = session.SessionId,
                                            });
  }

  public async Task ResumeSessionAsync(SessionInfo       session,
                                       CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.ResumeSessionAsync(new ResumeSessionRequest
                                           {
                                             SessionId = session.SessionId,
                                           });
  }

  public async Task PurgeSessionAsync(SessionInfo       session,
                                      CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.PurgeSessionAsync(new PurgeSessionRequest
                                          {
                                            SessionId = session.SessionId,
                                          });
  }

  public async Task DeleteSessionAsync(SessionInfo       session,
                                       CancellationToken cancellationToken = default)
  {
    await using var channel       = await channel_.GetAsync(cancellationToken);
    var             sessionClient = new Sessions.SessionsClient(channel);
    await sessionClient.DeleteSessionAsync(new DeleteSessionRequest
                                           {
                                             SessionId = session.SessionId,
                                           });
  }
}
