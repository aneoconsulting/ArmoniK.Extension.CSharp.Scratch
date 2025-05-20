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
using System.Threading.Tasks;

using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Factory;
using ArmoniK.Extension.CSharp.Client.Handlers;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client;

/// <summary>
///   Provides a client for interacting with the ArmoniK services, including blob, session, task, event, health check,
///   partition, and version services.
/// </summary>
public class ArmoniKClient
{
  private readonly ILoggerFactory loggerFactory_;
  private readonly ILogger        logger_;
  private readonly Properties     properties_;


  private IBlobService            blobService_;
  private ObjectPool<ChannelBase> channelPool_;
  private IEventsService          eventsService_;
  private IHealthCheckService     healthCheckService_;
  private IPartitionsService      partitionsService_;
  private ISessionService         sessionService_;
  private ITasksService           tasksService_;
  private IVersionsService        versionsService_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="ArmoniKClient" /> class with the specified properties and logger
  ///   factory.
  /// </summary>
  /// <param name="properties">The properties for configuring the client.</param>
  /// <param name="loggerFactory">The factory for creating loggers.</param>
  /// <param name="taskConfiguration">The default task configuration</param>
  /// <exception cref="ArgumentNullException">Thrown when properties or loggerFactory is null.</exception>
  public ArmoniKClient(Properties        properties,
                       ILoggerFactory    loggerFactory,
                       TaskConfiguration taskConfiguration)
  {
    properties_    = properties    ?? throw new ArgumentNullException(nameof(properties));
    loggerFactory_ = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
    logger_        = loggerFactory.CreateLogger<ArmoniKClient>();
  }

  /// <summary>
  ///   Gets the channel pool used for managing GRPC channels.
  /// </summary>
  public ObjectPool<ChannelBase> ChannelPool
    => channelPool_ ??= ClientServiceConnector.ControlPlaneConnectionPool(properties_,
                                                                          loggerFactory_);

  /// <summary>
  ///   Gets the blob service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob service instance.</returns>
  public Task<IBlobService> GetBlobService()
  {
    if (blobService_ is not null)
    {
      return Task.FromResult(blobService_);
    }

    blobService_ = BlobServiceFactory.CreateBlobService(ChannelPool,
                                                        loggerFactory_);
    return Task.FromResult(blobService_);
  }

  /// <summary>
  ///   Gets the session service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the session service instance.</returns>
  public Task<ISessionService> GetSessionService()
  {
    if (sessionService_ is not null)
    {
      return Task.FromResult(sessionService_);
    }

    sessionService_ = SessionServiceFactory.CreateSessionService(ChannelPool,
                                                                 properties_,
                                                                 loggerFactory_);
    return Task.FromResult(sessionService_);
  }

  /// <summary>
  ///   Gets the tasks service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the tasks service instance.</returns>
  public async Task<ITasksService> GetTasksServiceAsync()
  {
    if (tasksService_ is not null)
    {
      return tasksService_;
    }

    tasksService_ = TasksServiceFactory.CreateTaskService(ChannelPool,
                                                          await GetBlobService(),
                                                          loggerFactory_);
    return tasksService_;
  }

  /// <summary>
  ///   Gets the events service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the events service instance.</returns>
  public Task<IEventsService> GetEventsService()
  {
    if (eventsService_ is not null)
    {
      return Task.FromResult(eventsService_);
    }

    eventsService_ = EventsServiceFactory.CreateEventsService(ChannelPool,
                                                              loggerFactory_);
    return Task.FromResult(eventsService_);
  }

  /// <summary>
  ///   Gets the version service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the version service instance.</returns>
  public Task<IVersionsService> GetVersionService()
  {
    if (versionsService_ is not null)
    {
      return Task.FromResult(versionsService_);
    }

    versionsService_ = VersionsServiceFactory.CreateVersionsService(ChannelPool,
                                                                    loggerFactory_);
    return Task.FromResult(versionsService_);
  }

  /// <summary>
  ///   Gets the partitions service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the partitions service instance.</returns>
  public Task<IPartitionsService> GetPartitionsService()
  {
    if (partitionsService_ is not null)
    {
      return Task.FromResult(partitionsService_);
    }

    partitionsService_ = PartitionsServiceFactory.CreatePartitionsService(ChannelPool,
                                                                          loggerFactory_);
    return Task.FromResult(partitionsService_);
  }

  /// <summary>
  ///   Gets the health check service.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the health check service instance.</returns>
  public Task<IHealthCheckService> GetHealthCheckService()
  {
    if (healthCheckService_ is not null)
    {
      return Task.FromResult(healthCheckService_);
    }

    healthCheckService_ = HealthCheckServiceFactory.CreateHealthCheckService(ChannelPool,
                                                                             loggerFactory_);
    return Task.FromResult(healthCheckService_);
  }

  /// <summary>
  ///   Gets a blob handler for the specified blob information.
  /// </summary>
  /// <param name="blobInfo">The blob information.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob handler instance.</returns>
  public Task<BlobHandler> GetBlobHandler(BlobInfo blobInfo)
    => Task.FromResult(new BlobHandler(blobInfo,
                                       this));

  /// <summary>
  ///   Gets a task handler for the specified task information.
  /// </summary>
  /// <param name="taskInfos">The task information.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the task handler instance.</returns>
  public Task<TaskHandler> GetTaskHandler(TaskInfos taskInfos)
    => Task.FromResult(new TaskHandler(this,
                                       taskInfos));

  /// <summary>
  ///   Gets a session handler for the specified session information.
  /// </summary>
  /// <param name="session">The session information.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the session handler instance.</returns>
  public Task<SessionHandler> GetSessionHandler(SessionInfo session)
    => Task.FromResult(new SessionHandler(session,
                                          this));
}
