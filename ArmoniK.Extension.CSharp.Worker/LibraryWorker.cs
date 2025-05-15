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

using System.Runtime.Loader;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.DllCommon;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Worker;

/// <summary>
///   Represents a worker that handles the execution of tasks within a specific library context.
/// </summary>
public class LibraryWorker : ILibraryWorker
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
  /// <param name="libraryLoader">The library loader to load the necessary assemblies.</param>
  /// <param name="libraryContext">The context of the library to execute the task in.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  /// <exception cref="WorkerApiException">Thrown when there is an error in the service library or execution process.</exception>
  public async Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                                         ILibraryLoader    libraryLoader,
                                         string            libraryContext,
                                         CancellationToken cancellationToken)
  {
    using var _ = Logger.BeginPropertyScope(("sessionId", taskHandler.SessionId),
                                            ("taskId", $"{taskHandler.TaskId}"));

    var serviceLibrary = taskHandler.TaskOptions.GetServiceLibrary();
    if (string.IsNullOrEmpty(serviceLibrary))
    {
      throw new WorkerApiException("No ServiceLibrary found");
    }

    var dynamicLibrary = taskHandler.TaskOptions.GetTaskLibraryDefinition(serviceLibrary);
    if (dynamicLibrary == null)
    {
      throw new WorkerApiException($"Library '{serviceLibrary}' not found");
    }

    Logger.LogInformation("ServiceLibrary: {serviceLibrary}",
                          serviceLibrary);
    Logger.LogInformation("DynamicLibrary.Service: {Service}",
                          dynamicLibrary.Service);

    if (string.IsNullOrEmpty(dynamicLibrary.Service))
    {
      throw new WorkerApiException("No ServiceLibrary found");
    }

    Logger.LogInformation("Entering Context");

    var context = libraryLoader.GetAssemblyLoadContext(libraryContext);
    try
    {
      if (AssemblyLoadContext.CurrentContextualReflectionContext == null || AssemblyLoadContext.CurrentContextualReflectionContext?.Name != context.Name)
      {
        context.EnterContextualReflection();
      }

      var serviceClass = libraryLoader.GetClassInstance<IWorker>(dynamicLibrary);
      var result = await serviceClass.ExecuteAsync(taskHandler,
                                                   Logger,
                                                   cancellationToken)
                                     .ConfigureAwait(false);

      Logger.LogInformation("Got the following result from the execution: {result}",
                            result);

      return result;
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
