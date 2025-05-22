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

using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Factory;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;

using Moq;

namespace Tests.Configuration;

internal sealed class MockedServicesConfiguration : IServicesConfiguration
{
  private readonly Mock<CallInvoker> mockInvoker_;

  public MockedServicesConfiguration(Mock<CallInvoker> mockInvoker)
    => mockInvoker_ = mockInvoker;

  public void AddServices(ArmoniKClient     client,
                          ServiceCollection services)
  {
    var mockChannelBase = new Mock<ChannelBase>("localhost")
                          {
                            CallBase = true,
                          };
    mockChannelBase.Setup(m => m.CreateCallInvoker())
                   .Returns(mockInvoker_.Object);
    var objectPool = new ObjectPool<ChannelBase>(() => mockChannelBase.Object);

    services.AddSingleton<IBlobService>(_ => BlobServiceFactory.CreateBlobService(objectPool,
                                                                                  client.LoggerFactory));
    services.AddSingleton<IEventsService>(_ => EventsServiceFactory.CreateEventsService(objectPool,
                                                                                        client.LoggerFactory));
    services.AddSingleton<IHealthCheckService>(_ => HealthCheckServiceFactory.CreateHealthCheckService(objectPool,
                                                                                                       client.LoggerFactory));
    services.AddSingleton<IPartitionsService>(_ => PartitionsServiceFactory.CreatePartitionsService(objectPool,
                                                                                                    client.LoggerFactory));
    services.AddSingleton<ISessionService>(_ => SessionServiceFactory.CreateSessionService(objectPool,
                                                                                           client.Properties,
                                                                                           client.LoggerFactory));
    services.AddSingleton<ITasksService>(_ => TasksServiceFactory.CreateTaskService(objectPool,
                                                                                    client.BlobService,
                                                                                    client.LoggerFactory));
    services.AddSingleton<IVersionsService>(_ => VersionsServiceFactory.CreateVersionsService(objectPool,
                                                                                              client.LoggerFactory));
  }
}
