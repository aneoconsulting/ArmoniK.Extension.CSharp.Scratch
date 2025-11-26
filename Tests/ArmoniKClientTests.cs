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
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Task;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace Tests;

public class ArmoniKClientTests
{
  private ArmoniKClient?        client_;
  private Properties?           defaultProperties_;
  private TaskConfiguration?    defaultTaskOptions_;
  private Mock<ILoggerFactory>? loggerFactoryMock_;

  [SetUp]
  public void SetUp()
  {
    IConfiguration configuration = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                             .AddJsonFile("appsettings.tests.json",
                                                                          false)
                                                             .AddEnvironmentVariables()
                                                             .Build();
    defaultTaskOptions_ = new TaskConfiguration(2,
                                                1,
                                                "subtasking",
                                                TimeSpan.FromHours(1));

    defaultProperties_ = new Properties(configuration);

    loggerFactoryMock_ = new Mock<ILoggerFactory>();

    client_ = new ArmoniKClient(defaultProperties_,
                                loggerFactoryMock_.Object);
  }

  [Test]
  public void Constructor_ThrowsArgumentNullException_IfPropertiesIsNull()
    // Act & Assert
    => Assert.That(() => new ArmoniKClient(null!,
                                           loggerFactoryMock_!.Object),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("properties"));

  [Test]
  public void Constructor_ThrowsArgumentNullException_IfLoggerFactoryIsNull()
    // Act & Assert
    => Assert.That(() => new ArmoniKClient(defaultProperties_!,
                                           null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("loggerFactory"));

  [Test]
  public void GetBlobService_ShouldReturnInstance()
  {
    // Arrange
    var session = new Session
                  {
                    Id = Guid.NewGuid()
                             .ToString(),
                  };
    // Act
    var blobService = client_!.BlobService;
    // Assert
    Assert.That(client_.BlobService,
                Is.InstanceOf<IBlobService>(),
                "The returned object should be an instance of IBlobService or derive from it.");
  }

  [Test]
  public void GetSessionService_ShouldReturnInstance()
  {
    // Act
    var sessionService = client_!.SessionService;
    // Assert
    Assert.That(sessionService,
                Is.InstanceOf<ISessionService>(),
                "The returned object should be an instance of ISessionService or derive from it.");
  }

  [Test]
  public void GetTasksService_ShouldReturnInstance()
  {
    // Arrange
    var session = new Session
                  {
                    Id = Guid.NewGuid()
                             .ToString(),
                  };
    // Act
    var taskService = client_!.TasksService;
    // Assert
    Assert.That(taskService,
                Is.InstanceOf<ITasksService>(),
                "The returned object should be an instance of ITasksService or derive from it.");
  }

  [Test]
  public void GetEventsService_ShouldReturnInstance()
  {
    // Arrange
    var session = new Session
                  {
                    Id = Guid.NewGuid()
                             .ToString(),
                  };
    // Act
    var eventsService = client_!.EventsService;
    // Assert
    Assert.That(eventsService,
                Is.InstanceOf<IEventsService>(),
                "The returned object should be an instance of IEventsService or derive from it.");
  }
}
