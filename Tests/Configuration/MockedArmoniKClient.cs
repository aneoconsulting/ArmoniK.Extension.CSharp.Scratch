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
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

namespace Tests.Configuration;

internal sealed class MockedArmoniKClient : IArmoniKClient
{
  private readonly ServiceProvider            serviceProvider_;
  private          Mock<IBlobService>?        blobServiceMock_;
  private          Mock<IEventsService>?      eventServiceMock_;
  private          Mock<IHealthCheckService>? healthCheckServiceMock_;
  private          Mock<IPartitionsService>?  partitionServiceMock_;
  private          Mock<ISessionService>?     sessionServiceMock_;
  private          Mock<ITasksService>?       tasksServiceMock_;
  private          Mock<IVersionsService>?    versionsServiceMock_;

  public MockedArmoniKClient()
  {
    CallInvokerMock = new Mock<CallInvoker>();
    IConfiguration configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                             .AddJsonFile("appsettings.tests.json",
                                                                          false)
                                                             .AddEnvironmentVariables()
                                                             .Build();
    Properties = new Properties(configuration);
    TaskOptions = new TaskConfiguration(2,
                                        1,
                                        "subtasking",
                                        TimeSpan.FromHours(1));

    var mockChannelBase = new Mock<ChannelBase>("localhost")
                          {
                            CallBase = true,
                          };
    mockChannelBase.Setup(m => m.CreateCallInvoker())
                   .Returns(CallInvokerMock.Object);
    ChannelPool = new ObjectPool<ChannelBase>(() => mockChannelBase.Object);

    var services              = new ServiceCollection();
    var servicesConfiguration = new DefaultServicesConfiguration();
    servicesConfiguration.AddServices(this,
                                      services);
    serviceProvider_ = services.BuildServiceProvider();
  }

  public Mock<CallInvoker> CallInvokerMock { get; }

  public TaskConfiguration TaskOptions { get; }

  public Mock<IBlobService> BlobServiceMock
    => blobServiceMock_ ??= new Mock<IBlobService>();

  public Mock<ITasksService> TaskServiceMock
    => tasksServiceMock_ ??= new Mock<ITasksService>();

  public Mock<ISessionService> SessionServiceMock
    => sessionServiceMock_ ??= new Mock<ISessionService>();

  public Mock<IEventsService> EventServiceMock
    => eventServiceMock_ ??= new Mock<IEventsService>();

  public Mock<IVersionsService> VersionsServiceMock
    => versionsServiceMock_ ??= new Mock<IVersionsService>();

  public Mock<IPartitionsService> PartitionsServiceMock
    => partitionServiceMock_ ??= new Mock<IPartitionsService>();

  public Mock<IHealthCheckService> HealthServiceMock
    => healthCheckServiceMock_ ??= new Mock<IHealthCheckService>();

  public Properties Properties { get; }

  public ILoggerFactory LoggerFactory
    => NullLoggerFactory.Instance;

  public ObjectPool<ChannelBase> ChannelPool { get; }

  public IBlobService BlobService
    => blobServiceMock_?.Object ?? serviceProvider_.GetRequiredService<IBlobService>();

  public ITasksService TasksService
    => tasksServiceMock_?.Object ?? serviceProvider_.GetRequiredService<ITasksService>();

  public ISessionService SessionService
    => sessionServiceMock_?.Object ?? serviceProvider_.GetRequiredService<ISessionService>();

  public IEventsService EventsService
    => eventServiceMock_?.Object ?? serviceProvider_.GetRequiredService<IEventsService>();

  public IVersionsService VersionService
    => versionsServiceMock_?.Object ?? serviceProvider_.GetRequiredService<IVersionsService>();

  public IPartitionsService PartitionsService
    => partitionServiceMock_?.Object ?? serviceProvider_.GetRequiredService<IPartitionsService>();

  public IHealthCheckService HealthCheckService
    => healthCheckServiceMock_?.Object ?? serviceProvider_.GetRequiredService<IHealthCheckService>();
}
