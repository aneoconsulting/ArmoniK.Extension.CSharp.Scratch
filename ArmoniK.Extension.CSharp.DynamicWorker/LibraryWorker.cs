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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.Common.Exceptions;
using ArmoniK.Extension.CSharp.Common.Library;
using ArmoniK.Extension.CSharp.Worker;
using ArmoniK.Extension.CSharp.Worker.Interfaces;

namespace ArmoniK.Extension.CSharp.DynamicWorker;

/// <summary>
///   Represents a worker that handles the execution of tasks within a specific library context.
/// </summary>
internal class LibraryWorker
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="LibraryWorker" /> class.
  /// </summary>
  /// <param name="configuration">The configuration settings.</param>
  /// <param name="factory">The logger factory to create logger instances.</param>
  public LibraryWorker(IConfiguration configuration,
                       ILoggerFactory factory)
  {
    Configuration = configuration;
    LoggerFactory = factory;
    Logger        = factory.CreateLogger<LibraryWorker>();
  }

  /// <summary>
  ///   Gets the logger for the <see cref="LibraryWorker" /> class.
  /// </summary>
  private ILogger<LibraryWorker> Logger { get; }

  /// <summary>
  ///   Gets or sets the logger factory.
  /// </summary>
  public ILoggerFactory LoggerFactory { get; set; }

  /// <summary>
  ///   Gets or sets the configuration settings.
  /// </summary>
  public IConfiguration Configuration { get; set; }

  /// <summary>
  ///   Executes a task asynchronously within a specified library context.
  /// </summary>
  /// <param name="taskHandler">The task handler.</param>
  /// <param name="dynamicLibrary">The worker library.</param>
  /// <param name="worker">The worker instance to invoke execution on.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  /// <exception cref="ArmoniKSdkException">When there is a compliance error with the ArmoniK SDK conventions.</exception>
  public async Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                                         DynamicLibrary    dynamicLibrary,
                                         IWorker           worker,
                                         CancellationToken cancellationToken)
  {
    try
    {
      using var _ = Logger.BeginPropertyScope(("sessionId", taskHandler.SessionId),
                                              ("taskId", $"{taskHandler.TaskId}"));

      Logger.LogDebug("Executing service class: {ServiceKey}",
                      dynamicLibrary.Symbol);

      return await Task.Run(async () => await SdkTaskRunner.Run(taskHandler,
                                                                worker,
                                                                Logger,
                                                                cancellationToken)
                                                           .ConfigureAwait(false),
                            cancellationToken);
    }
    catch (Exception ex)
    {
      Logger.LogError(ex,
                      "Error during library method invocation");
      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message,
                       },
             };
    }
  }
}
