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
  private readonly object locker_ = new();

  /// <summary>
  ///   Reference to the last loaded service for health checking.
  /// </summary>
  private IWorker? currentService_;

  private HealthCheckResult lastStatus_;

  /// <summary>
  ///   Key of the last loaded service for logging purposes.
  /// </summary>
  private string serviceKey_ = string.Empty;

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
    lastStatus_   = HealthCheckResult.Healthy("Library dynamic worker infrastructure is operational (no service loaded yet)");
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

  private void SetCurrentWorker(IWorker? worker,
                                string   serviceKey)
  {
    lock (locker_)
    {
      currentService_ = worker;
      serviceKey_     = serviceKey;
    }
  }

  private (IWorker?, string) GetCurrentWorker()
  {
    lock (locker_)
    {
      return (currentService_, serviceKey_);
    }
  }

  private void SetLastHealthStatus(HealthCheckResult result)
  {
    lock (locker_)
    {
      if (result.IsHealthy && !lastStatus_.IsHealthy)
      {
        // Healthy status cannot override unhealthy
        return;
      }

      lastStatus_ = result;
    }
  }

  private HealthCheckResult GetLastHealthStatus()
  {
    lock (locker_)
    {
      return lastStatus_;
    }
  }

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
    var serviceKey = "Unknown service";
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

      serviceKey = $"{libraryContext}:{dynamicLibrary.Symbol}";
      Logger.LogDebug("Loading service class: {ServiceKey}",
                      serviceKey);
      SetCurrentWorker(libraryLoader.GetClassInstance<IWorker>(dynamicLibrary),
                       serviceKey);

      var output = await SdkTaskRunner.Run(taskHandler,
                                           currentService_!,
                                           Logger,
                                           cancellationToken)
                                      .ConfigureAwait(false);
      var healthCheckResult = await currentService_!.CheckHealth(cancellationToken)
                                                    .ConfigureAwait(false);
      SetLastHealthStatus(healthCheckResult);

      // Dispose the worker if necessary
      if (currentService_ is IAsyncDisposable asyncDisposable)
      {
        await asyncDisposable.DisposeAsync()
                             .ConfigureAwait(false);
      }
      else if (currentService_ is IDisposable disposable)
      {
        disposable.Dispose();
      }

      SetCurrentWorker(null,
                       string.Empty);
      return output;
    }
    catch (Exception ex)
    {
      Logger.LogError(ex,
                      "Error during library method invocation");
      SetLastHealthStatus(HealthCheckResult.Unhealthy($"An error occured while executing service '{serviceKey}'",
                                                      ex));
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
    var serviceKey = "Unknown service";
    try
    {
      IWorker? service          = null;
      var      lastHealthStatus = GetLastHealthStatus();
      if (!lastHealthStatus.IsHealthy)
      {
        return lastHealthStatus;
      }

      (service, serviceKey) = GetCurrentWorker();
      Logger.LogDebug("Starting library worker health check");
      if (service == null)
      {
        Logger.LogDebug("No service is currently loaded");
        return lastHealthStatus;
      }

      Logger.LogDebug("Checking health of current service: '{ServiceKey}'",
                      serviceKey);

      // Async call to check health of the last loaded service
      var serviceHealthResult = await service.CheckHealth(cancellationToken)
                                             .ConfigureAwait(false);

      var message = $"Service '{serviceKey}': {serviceHealthResult.Description}";
      if (!serviceHealthResult.IsHealthy)
      {
        Logger.LogWarning("Current service is unhealthy: {Message}",
                          message);
        serviceHealthResult = HealthCheckResult.Unhealthy(message);
      }
      else
      {
        Logger.LogWarning("Current service is healthy: {Message}",
                          message);
        serviceHealthResult = HealthCheckResult.Healthy(message);
      }

      SetLastHealthStatus(serviceHealthResult);
    }
    catch (Exception ex)
    {
      var errorMessage = $"Library worker health check failed '{serviceKey}': {ex.Message}";
      Logger?.LogError(ex,
                       "Library worker health check failed for service '{Service}'",
                       serviceKey);
      var serviceHealthResult = HealthCheckResult.Unhealthy(errorMessage,
                                                            ex);
      SetLastHealthStatus(serviceHealthResult);
    }

    return lastStatus_;
  }
}
