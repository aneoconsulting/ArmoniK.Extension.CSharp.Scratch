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
using System.Text;
using System.Text.Json;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.Common.Exceptions;
using ArmoniK.Extension.CSharp.Worker.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Worker.Interfaces;
using ArmoniK.Extension.CSharp.Worker.Interfaces.Handles;

namespace ArmoniK.Extension.CSharp.Worker;

/// <summary>
///   Represents a worker that handles the execution of tasks within a specific library context.
/// </summary>
public class LibraryWorker : ILibraryWorker
{
  /// <summary>
  ///   Reference to the last loaded service for health checking.
  /// </summary>
  private IWorker? lastLoadedService_;

  /// <summary>
  ///   Key of the last loaded service for logging purposes.
  /// </summary>
  private string? lastServiceKey_;

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
                                         ILibraryLoader    libraryLoader,
                                         string            libraryContext,
                                         CancellationToken cancellationToken)
  {
    try
    {
      using var _ = Logger.BeginPropertyScope(("sessionId", taskHandler.SessionId),
                                              ("taskId", $"{taskHandler.TaskId}"));

      var conventionVersion = taskHandler.TaskOptions.GetConventionVersion();
      if (conventionVersion != "v1")
      {
        throw new ArmoniKSdkException($"ArmoniK SDK version '{conventionVersion}' not supported.");
      }

      var dynamicLibrary = taskHandler.TaskOptions.GetDynamicLibrary();

      Logger.LogInformation("DynamicLibrary.Symbol: {Service}",
                            dynamicLibrary.Symbol);

      Logger.LogInformation("Entering Context");

      var context = libraryLoader.GetAssemblyLoadContext(libraryContext);
      if (AssemblyLoadContext.CurrentContextualReflectionContext == null || AssemblyLoadContext.CurrentContextualReflectionContext?.Name != context.Name)
      {
        context.EnterContextualReflection();
      }

      var serviceKey = $"{libraryContext}:{dynamicLibrary.Symbol}";

      Logger.LogDebug("Loading service class: {ServiceKey}",
                      serviceKey);
      var serviceClass = libraryLoader.GetClassInstance<IWorker>(dynamicLibrary);

      lastLoadedService_ = serviceClass;
      lastServiceKey_    = serviceKey;

      var dataDependencies = new Dictionary<string, BlobHandle>();
      var expectedResults  = new Dictionary<string, BlobHandle>();
      var sdkTaskHandler = new SdkTaskHandler(taskHandler,
                                              dataDependencies,
                                              expectedResults);
      var payload = string.Empty;
      try
      {
        // Decoding of the payload
        payload = Encoding.UTF8.GetString(taskHandler.Payload);
        var name2BlobId = JsonSerializer.Deserialize<Payload>(payload);
        foreach (var pair in name2BlobId!.Inputs)
        {
          var name   = pair.Key;
          var blobId = pair.Value;
          var data   = taskHandler.DataDependencies[blobId];
          dataDependencies[name] = new BlobHandle(blobId,
                                                  sdkTaskHandler,
                                                  data);
        }

        foreach (var pair in name2BlobId.Outputs)
        {
          var name   = pair.Key;
          var blobId = pair.Value;
          expectedResults[name] = new BlobHandle(blobId,
                                                 sdkTaskHandler);
        }
      }
      catch (Exception ex)
      {
        Logger.LogError(ex,
                        "Could not decode payload: {Message}",
                        ex.Message);
        Logger.LogError("Payload is:{Payload}",
                        payload);
        throw;
      }

      var result = await serviceClass.ExecuteAsync(sdkTaskHandler,
                                                   Logger,
                                                   cancellationToken)
                                     .ConfigureAwait(false);

      Logger.LogInformation("Got the following result from the execution: {result}",
                            result);

      if (result.IsSuccess)
      {
        return new Output
               {
                 Ok = new Empty(),
               };
      }

      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = result.ErrorMessage,
                       },
             };
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

      if (lastLoadedService_ == null)
      {
        Logger.LogDebug("No service has been loaded yet");
        return HealthCheckResult.Healthy("Library worker infrastructure is operational (no service loaded yet)");
      }

      try
      {
        Logger.LogDebug("Checking health of last loaded service: {ServiceKey}",
                        lastServiceKey_);

        // Async call to check health of the last loaded service
        var serviceHealthResult = await lastLoadedService_.CheckHealth(cancellationToken)
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
