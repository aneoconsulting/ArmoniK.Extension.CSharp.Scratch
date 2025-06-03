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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace Tests;

public class ArmoniKClientTests
{
  private readonly ArmoniKClient        client_;
  private readonly Properties           defaultProperties_;
  private readonly TaskConfiguration    defaultTaskOptions_;
  private readonly Mock<ILoggerFactory> loggerFactoryMock_;

  public ArmoniKClientTests()
  {
    IConfiguration configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                             .AddJsonFile("appsettings.tests.json",
                                                                          false)
                                                             .AddEnvironmentVariables()
                                                             .Build();
    List<string> defaultPartitionsIds = new()
                                        {
                                          "subtasking",
                                        };
    defaultTaskOptions_ = new TaskConfiguration(2,
                                                1,
                                                "subtasking",
                                                TimeSpan.FromHours(1));

    defaultProperties_ = new Properties(configuration);

    loggerFactoryMock_ = new Mock<ILoggerFactory>();

    client_ = new ArmoniKClient(defaultProperties_,
                                loggerFactoryMock_.Object,
                                defaultTaskOptions_);
  }

  [Test]
  public void Constructor_ThrowsArgumentNullException_IfPropertiesIsNull()
  {
    // Act 
    var exception = Assert.Throws<ArgumentNullException>(() => new ArmoniKClient(null,
                                                                                 loggerFactoryMock_.Object,
                                                                                 defaultTaskOptions_));
    Assert.That(exception?.ParamName,
                Is.EqualTo("properties"));
  }

  [Test]
  public void Constructor_ThrowsArgumentNullException_IfLoggerFactoryIsNull()
  {
    // Act  
    var exception = Assert.Throws<ArgumentNullException>(() => new ArmoniKClient(defaultProperties_,
                                                                                 null,
                                                                                 defaultTaskOptions_));
    // Assert
    Assert.That(exception?.ParamName,
                Is.EqualTo("loggerFactory"));
  }

  [Test]
  public async Task GetBlobService_ShouldReturnInstance()
  {
    // Arrange
    var session = new Session
                  {
                    Id = Guid.NewGuid()
                             .ToString(),
                  };
    // Act
    var blobService = await client_.GetBlobService();
    // Assert
    Assert.That(blobService,
                Is.InstanceOf<IBlobService>(),
                "The returned object should be an instance of IBlobService or derive from it.");
  }

  [Test]
  public async Task GetSessionService_ShouldReturnInstance()
  {
    // Act
    var sessionService = await client_.GetSessionService();
    // Assert
    Assert.That(sessionService,
                Is.InstanceOf<ISessionService>(),
                "The returned object should be an instance of ISessionService or derive from it.");
  }

  [Test]
  public async Task GetTasksService_ShouldReturnInstance()
  {
    // Arrange
    var session = new Session
                  {
                    Id = Guid.NewGuid()
                             .ToString(),
                  };
    // Act
    var taskService = await client_.GetTasksServiceAsync();
    // Assert
    Assert.That(taskService,
                Is.InstanceOf<ITasksService>(),
                "The returned object should be an instance of ITasksService or derive from it.");
  }

  [Test]
  public async Task GetEventsService_ShouldReturnInstance()
  {
    // Arrange
    var session = new Session
                  {
                    Id = Guid.NewGuid()
                             .ToString(),
                  };
    // Act
    var eventsService = await client_.GetEventsService();
    // Assert
    Assert.That(eventsService,
                Is.InstanceOf<IEventsService>(),
                "The returned object should be an instance of IEventsService or derive from it.");
  }
}
