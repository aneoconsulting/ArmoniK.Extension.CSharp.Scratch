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

using ArmoniK.Extension.CSharp.Client.Common.Services;

namespace ArmoniK.Extension.CSharp.Client.Services;

/// <summary>
///   Provides the default services configuration for the ArmoniK client
/// </summary>
public sealed class ServicesConfiguration : IServicesConfiguration
{
  private readonly IArmoniKClient client_;

  /// <summary>
  ///   Build the instance
  /// </summary>
  /// <param name="client">The ArmoniK client</param>
  public ServicesConfiguration(IArmoniKClient client)
    => client_ = client;

  /// <summary>
  ///   Returns the function that creates the BlobService
  /// </summary>
  public Func<IServiceProvider, IBlobService> BlobServiceBuilder
    => BuildBlobService;

  /// <summary>
  ///   Returns the function that creates the EventsService
  /// </summary>
  public Func<IServiceProvider, IEventsService> EventsServiceBuilder
    => BuildEventsService;

  /// <summary>
  ///   Returns the function that creates the HealthCheckService
  /// </summary>
  public Func<IServiceProvider, IHealthCheckService> HealthCheckServiceBuilder
    => BuildHealthCheckService;

  /// <summary>
  ///   Returns the function that creates the PartitionsService
  /// </summary>
  public Func<IServiceProvider, IPartitionsService> PartitionsServiceBuilder
    => BuildPartitionsService;

  /// <summary>
  ///   Returns the function that creates the SessionService
  /// </summary>
  public Func<IServiceProvider, ISessionService> SessionServiceBuilder
    => BuildSessionsService;

  /// <summary>
  ///   Returns the function that creates the TasksService
  /// </summary>
  public Func<IServiceProvider, ITasksService> TaskServiceBuilder
    => BuildTasksService;

  /// <summary>
  ///   Returns the function that creates the VersionsService
  /// </summary>
  public Func<IServiceProvider, IVersionsService> VersionsServiceBuilder
    => BuildVersionsService;

  private IBlobService BuildBlobService(IServiceProvider provider)
    => new BlobService(client_.ChannelPool,
                       client_.LoggerFactory);

  private IEventsService BuildEventsService(IServiceProvider provider)
    => new EventsService(client_.ChannelPool,
                         client_.LoggerFactory);

  private IHealthCheckService BuildHealthCheckService(IServiceProvider provider)
    => new HealthCheckService(client_.ChannelPool,
                              client_.LoggerFactory);

  private IPartitionsService BuildPartitionsService(IServiceProvider provider)
    => new PartitionsService(client_.ChannelPool,
                             client_.LoggerFactory);

  private ISessionService BuildSessionsService(IServiceProvider provider)
    => new SessionService(client_.ChannelPool,
                          client_.Properties,
                          client_.LoggerFactory);

  private ITasksService BuildTasksService(IServiceProvider provider)
    => new TasksService(client_.ChannelPool,
                        client_.BlobService,
                        client_.LoggerFactory);

  private IVersionsService BuildVersionsService(IServiceProvider provider)
    => new VersionsService(client_.ChannelPool,
                           client_.LoggerFactory);
}
