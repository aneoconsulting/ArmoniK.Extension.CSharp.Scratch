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

using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Extension.CSharp.Client.Services;

/// <summary>
///   Provides the default services configuration for the ArmoniK client
/// </summary>
public class DefaultServicesConfiguration : IServicesConfiguration
{
  /// <summary>
  ///   Adds the services necessary for the client
  /// </summary>
  /// <param name="client">The client that needs to be configured</param>
  /// <param name="services">The service collection</param>
  public void AddServices(IArmoniKClient    client,
                          ServiceCollection services)
  {
    services.AddSingleton<IBlobService>(_ => new BlobService(client.ChannelPool,
                                                             client.LoggerFactory));
    services.AddSingleton<IEventsService>(_ => new EventsService(client.ChannelPool,
                                                                 client.LoggerFactory));
    services.AddSingleton<IHealthCheckService>(_ => new HealthCheckService(client.ChannelPool,
                                                                           client.LoggerFactory));
    services.AddSingleton<IPartitionsService>(_ => new PartitionsService(client.ChannelPool,
                                                                         client.LoggerFactory));
    services.AddSingleton<ISessionService>(_ => new SessionService(client.ChannelPool,
                                                                   client.Properties,
                                                                   client.LoggerFactory));
    services.AddSingleton<ITasksService>(_ => new TasksService(client.ChannelPool,
                                                               client.BlobService,
                                                               client.LoggerFactory));
    services.AddSingleton<IVersionsService>(_ => new VersionsService(client.ChannelPool,
                                                                     client.LoggerFactory));
  }
}
