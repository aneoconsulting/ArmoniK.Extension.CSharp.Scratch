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
using ArmoniK.Extension.CSharp.Common.Exceptions;
using ArmoniK.Extension.CSharp.Worker;
using ArmoniK.Extension.CSharp.Worker.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Worker.Interfaces;

namespace ArmoniK.Extension.CSharp.DynamicWorker;

/// <summary>
///   Represents a worker that handles the execution of tasks within a specific library context.
/// </summary>
internal class LibraryWorker
{
  /// <summary>
  ///   Reference to the last loaded service for health checking.
  /// </summary>
  private IWorker? currentService_;

  /// <summary>
  ///   Key of the last loaded service for logging purposes.
  /// </summary>
  private string lastServiceKey_ = string.Empty;

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
  /// <exception cref="ArmoniKSdkException">When there is a compliance error with the ArmoniK SDK conventions.</exception>
  public async Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                                         LibraryLoader     libraryLoader,
                                         string            libraryContext,
                                         CancellationToken cancellationToken)
  {
    try
    {
      using var _ = Logger.BeginPropertyScope(("sessionId", taskHandler.SessionId),
                                              ("taskId", $"{taskHandler.TaskId}"));

      var dynamicLibrary = taskHandler.TaskOptions.GetDynamicLibrary();

      Logger.LogInformation("DynamicLibrary.Symbol: {Service}",
                            dynamicLibrary.Symbol);
      Logger.LogInformation("Entering Context");

      var context = libraryLoader.GetAssemblyLoadContext(libraryContext);
      if (AssemblyLoadContext.CurrentContextualReflectionContext == null || AssemblyLoadContext.CurrentContextualReflectionContext?.Name != context.Name)
      {
        context.EnterContextualReflection();
      }

      lastServiceKey_ = $"{libraryContext}:{dynamicLibrary.Symbol}";
      Logger.LogDebug("Loading service class: {ServiceKey}",
                      lastServiceKey_);
      currentService_ = libraryLoader.GetClassInstance<IWorker>(dynamicLibrary);

      var output = await SdkTaskRunner.Run(taskHandler,
                                           currentService_,
                                           Logger,
                                           cancellationToken)
                                      .ConfigureAwait(false);
      currentService_ = null;
      return output;
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

  /// <summary>
  ///   Checks the health of the library worker and the last loaded service.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A health check result indicating the status of the worker and the last loaded service.</returns>
  public async Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
  {
    try
    {
      Logger.LogDebug("Starting library worker health check");

      var testLogger = LoggerFactory.CreateLogger<LibraryWorker>();
      if (testLogger == null)
      {
        return HealthCheckResult.Unhealthy("Cannot create logger instance");
      }

      testLogger.LogInformation("Health check validation at {Time}",
                                DateTime.UtcNow);

      if (currentService_ == null)
      {
        Logger.LogDebug("No service has been loaded yet");
        return HealthCheckResult.Healthy("Library worker infrastructure is operational (no service loaded yet)");
      }

      try
      {
        Logger.LogDebug("Checking health of last loaded service: {ServiceKey}",
                        lastServiceKey_);

        // Async call to check health of the last loaded service
        var serviceHealthResult = await currentService_.CheckHealth(cancellationToken)
                                                       .ConfigureAwait(false);


        if (!serviceHealthResult.IsHealthy)
        {
          var errorMessage = $"Service {lastServiceKey_}: {serviceHealthResult.Description}";
          Logger.LogWarning("Last loaded service is unhealthy: {ErrorMessage}",
                            errorMessage);
          return HealthCheckResult.Unhealthy(errorMessage);
        }

        Logger.LogDebug("Last loaded service {ServiceKey} is healthy",
                        lastServiceKey_);
        return HealthCheckResult.Healthy($"Library worker and service {lastServiceKey_} are operational");
      }
      catch (Exception ex)
      {
        var errorMessage = $"Failed to check health of service {lastServiceKey_}: {ex.Message}";
        Logger.LogError(ex,
                        "Health check failed for service {ServiceKey}",
                        lastServiceKey_);
        return HealthCheckResult.Unhealthy(errorMessage);
      }
    }
    catch (Exception ex)
    {
      Logger?.LogError(ex,
                       "Library worker health check failed");
      return HealthCheckResult.Unhealthy("Library worker health check failed",
                                         ex);
    }
  }
}
