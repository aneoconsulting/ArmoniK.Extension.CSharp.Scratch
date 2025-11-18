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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Handles;
using ArmoniK.Extension.CSharp.Client.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client;

/// <summary>
///   Provides a client for interacting with the ArmoniK services, including blob, session, task, event, health check,
///   partition, and version services.
/// </summary>
public class ArmoniKClient
{
  private readonly ILogger                 logger_;
  private readonly ServiceProvider         serviceProvider_;
  private          ObjectPool<ChannelBase> channelPool_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="ArmoniKClient" /> class with the specified properties and logger
  ///   factory.
  /// </summary>
  /// <param name="properties">The properties for configuring the client.</param>
  /// <param name="loggerFactory">The factory for creating loggers.</param>
  /// <exception cref="ArgumentNullException">Thrown when properties or loggerFactory is null.</exception>
  public ArmoniKClient(Properties     properties,
                       ILoggerFactory loggerFactory)
  {
    Properties    = properties    ?? throw new ArgumentNullException(nameof(properties));
    LoggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    logger_       = loggerFactory.CreateLogger<ArmoniKClient>();

    var services = new ServiceCollection();
    services.AddSingleton(BuildBlobService)
            .AddSingleton(BuildEventsService)
            .AddSingleton(BuildHealthCheckService)
            .AddSingleton(BuildPartitionsService)
            .AddSingleton(BuildSessionsService)
            .AddSingleton(BuildTasksService)
            .AddSingleton(BuildVersionsService);
    serviceProvider_ = services.BuildServiceProvider();
  }

  /// <summary>
  ///   The properties
  /// </summary>
  public Properties Properties { get; }

  /// <summary>
  ///   The logger factory
  /// </summary>
  public ILoggerFactory LoggerFactory { get; }

  /// <summary>
  ///   Gets the channel pool used for managing GRPC channels.
  /// </summary>
  public ObjectPool<ChannelBase> ChannelPool
    => channelPool_ ??= ClientServiceConnector.ControlPlaneConnectionPool(Properties,
                                                                          LoggerFactory);

  /// <summary>
  ///   Gets the blob service
  /// </summary>
  public IBlobService BlobService
    => serviceProvider_.GetRequiredService<IBlobService>();

  /// <summary>
  ///   Gets the tasks service.
  /// </summary>
  public ITasksService TasksService
    => serviceProvider_.GetRequiredService<ITasksService>();

  /// <summary>
  ///   Gets the session service.
  /// </summary>
  public ISessionService SessionService
    => serviceProvider_.GetRequiredService<ISessionService>();

  /// <summary>
  ///   Gets the events service.
  /// </summary>
  public IEventsService EventsService
    => serviceProvider_.GetRequiredService<IEventsService>();

  /// <summary>
  ///   Gets the version service.
  /// </summary>
  public IVersionsService VersionService
    => serviceProvider_.GetRequiredService<IVersionsService>();

  /// <summary>
  ///   Gets the partitions service.
  /// </summary>
  public IPartitionsService PartitionsService
    => serviceProvider_.GetRequiredService<IPartitionsService>();

  /// <summary>
  ///   Gets the health check service.
  /// </summary>
  public IHealthCheckService HealthCheckService
    => serviceProvider_.GetRequiredService<IHealthCheckService>();

  private IBlobService BuildBlobService(IServiceProvider provider)
    => new BlobService(ChannelPool,
                       this,
                       LoggerFactory);

  private IEventsService BuildEventsService(IServiceProvider provider)
    => new EventsService(ChannelPool,
                         LoggerFactory);

  private IHealthCheckService BuildHealthCheckService(IServiceProvider provider)
    => new HealthCheckService(ChannelPool,
                              LoggerFactory);

  private IPartitionsService BuildPartitionsService(IServiceProvider provider)
    => new PartitionsService(ChannelPool,
                             LoggerFactory);

  private ISessionService BuildSessionsService(IServiceProvider provider)
    => new SessionService(ChannelPool,
                          Properties,
                          this,
                          LoggerFactory);

  private ITasksService BuildTasksService(IServiceProvider provider)
    => new TasksService(ChannelPool,
                        BlobService,
                        this,
                        LoggerFactory);

  private IVersionsService BuildVersionsService(IServiceProvider provider)
    => new VersionsService(ChannelPool,
                           LoggerFactory);

  /// <summary>
  ///   Gets a blob handle for the specified blob information.
  /// </summary>
  /// <param name="blobInfo">The blob information.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob handle instance.</returns>
  public BlobHandle GetBlobHandle(BlobInfo blobInfo)
    => new(blobInfo,
           this);

  /// <summary>
  ///   Gets a task handle for the specified task information.
  /// </summary>
  /// <param name="taskInfos">The task information.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the task handle instance.</returns>
  public TaskHandle GetTaskHandle(TaskInfos taskInfos)
    => new(this,
           taskInfos);

  /// <summary>
  ///   Asynchronously creates a new session.
  /// </summary>
  /// <param name="partitionIds">Partitions related to opened session</param>
  /// <param name="taskOptions">Default task options for the session</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <param name="closeOnDispose">Whether the session should be closed once the SessionHandle instance is disposed.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the created session info.</returns>
  public async Task<SessionHandle> CreateSessionAsync(IEnumerable<string> partitionIds,
                                                      TaskConfiguration   taskOptions,
                                                      bool                closeOnDispose,
                                                      CancellationToken   cancellationToken = default)
  {
    var sessionInfo = await SessionService.CreateSessionAsync(partitionIds,
                                                              taskOptions,
                                                              cancellationToken)
                                          .ConfigureAwait(false);
    return new SessionHandle(sessionInfo,
                             this,
                             closeOnDispose);
  }
}
