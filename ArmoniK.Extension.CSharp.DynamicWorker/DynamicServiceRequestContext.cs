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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.Worker;
using ArmoniK.Extension.CSharp.Worker.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Worker.Interfaces;

namespace ArmoniK.Extension.CSharp.DynamicWorker;

/// <summary>
///   Represents the context for handling service requests with dynamic loading capability.
/// </summary>
public class DynamicServiceRequestContext : IServiceRequestContext, IAsyncDisposable
{
  private readonly LibraryLoader libraryLoader_;
  private readonly LibraryWorker libraryWorker_;
  private readonly ILogger       logger_;

  private string currentSession_ = string.Empty;

  /// <summary>
  ///   Initializes a new instance of the <see cref="DynamicServiceRequestContext" /> class.
  /// </summary>
  /// <param name="configuration">The configuration settings.</param>
  /// <param name="loggerFactory">The logger factory to create logger instances.</param>
  public DynamicServiceRequestContext(IConfiguration configuration,
                                      ILoggerFactory loggerFactory)
  {
    LoggerFactory = loggerFactory;
    logger_       = loggerFactory.CreateLogger<DynamicServiceRequestContext>();

    libraryLoader_ = new LibraryLoader(loggerFactory);
    libraryWorker_ = new LibraryWorker(configuration,
                                       loggerFactory);
  }

  /// <summary>
  ///   Gets or sets the logger factory.
  /// </summary>
  public ILoggerFactory LoggerFactory { get; set; }

  /// <summary>
  ///   Release instance's resources
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    await libraryLoader_.DisposeAsync()
                        .ConfigureAwait(false);
    logger_.LogInformation("The DynamicServiceRequestContext instance was disposed");
  }

  /// <summary>
  ///   Check the health of the library worker.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the heath status of the worker.</returns>
  public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken)
    => await libraryLoader_.CheckHealth(cancellationToken)
                           .ConfigureAwait(false);

  /// <summary>
  ///   Executes a task asynchronously.
  /// </summary>
  /// <param name="taskHandler">The task handler containing the task details.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  public async Task<Output> ExecuteTaskAsync(ITaskHandler      taskHandler,
                                             CancellationToken cancellationToken)
  {
    if (taskHandler.SessionId != currentSession_)
    {
      logger_.LogInformation("New session is {Session}",
                             taskHandler.SessionId);
      currentSession_ = taskHandler.SessionId;
      await libraryLoader_.ResetServiceAsync(cancellationToken)
                          .ConfigureAwait(false);
    }

    logger_.LogInformation("New request received");

    // Get the data about the dynamic library
    var dynamicLibrary = taskHandler.TaskOptions.GetDynamicLibrary();

    var workerInstance = await libraryLoader_.GetWorkerInstanceAsync(taskHandler,
                                                                     dynamicLibrary,
                                                                     cancellationToken)
                                             .ConfigureAwait(false);

    var result = await libraryWorker_.ExecuteAsync(taskHandler,
                                                   dynamicLibrary,
                                                   workerInstance,
                                                   cancellationToken)
                                     .ConfigureAwait(false);
    return result;
  }
}
