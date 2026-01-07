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

using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Client.Services;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Task;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

namespace Tests.Configuration;

internal sealed class MockedArmoniKClient
{
  private readonly ServiceProvider     serviceProvider_;
  private          Mock<IBlobService>? blobServiceMock_;

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

  public Mock<CallInvoker> CallInvokerMock { get; }

  public TaskConfiguration TaskOptions { get; }

  public Mock<IBlobService> BlobServiceMock
    => blobServiceMock_ ??= new Mock<IBlobService>();

  public Properties Properties { get; }

  public ILoggerFactory LoggerFactory
    => NullLoggerFactory.Instance;

  public ObjectPool<ChannelBase> ChannelPool { get; }

  public IBlobService BlobService
    => blobServiceMock_?.Object ?? serviceProvider_.GetRequiredService<IBlobService>();

  public ITasksService TasksService
    => serviceProvider_.GetRequiredService<ITasksService>();

  public ISessionService SessionService
    => serviceProvider_.GetRequiredService<ISessionService>();

  public IEventsService EventsService
    => serviceProvider_.GetRequiredService<IEventsService>();

  public IVersionsService VersionService
    => serviceProvider_.GetRequiredService<IVersionsService>();

  public IPartitionsService PartitionsService
    => serviceProvider_.GetRequiredService<IPartitionsService>();

  public IHealthCheckService HealthCheckService
    => serviceProvider_.GetRequiredService<IHealthCheckService>();

  /// <summary>
  ///   Implicit conversion to ArmoniKClient for compatibility with handlers.
  /// </summary>
  public static implicit operator ArmoniKClient(MockedArmoniKClient mockedClient)
    => new(mockedClient.Properties,
           mockedClient.LoggerFactory);

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
}
