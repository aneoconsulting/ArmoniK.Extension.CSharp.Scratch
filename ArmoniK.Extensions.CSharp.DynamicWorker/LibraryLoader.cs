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

using System.Collections.Concurrent;

using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extensions.CSharp.Common.Exceptions;
using ArmoniK.Extensions.CSharp.Common.Library;
using ArmoniK.Extensions.CSharp.Worker.Interfaces;
using ArmoniK.Utils;

namespace ArmoniK.Extensions.CSharp.DynamicWorker;

/// <summary>
///   Provides functionality to load and manage dynamic libraries for the ArmoniK project.
/// </summary>
internal sealed class LibraryLoader : IAsyncDisposable, IDisposable
{
  private readonly SemaphoreSlim                               binarySemaphore_;
  private readonly ExecutionSingleizer<HealthCheckResult>      checkHealthSingleizer_ = new();
  private readonly ILoggerFactory                              loggerFactory_;
  private readonly ILogger                                     logger_;
  private readonly ConcurrentDictionary<string, WorkerService> workerServices_ = new();

  /// <summary>
  ///   Initializes a new instance of the <see cref="LibraryLoader" /> class.
  /// </summary>
  /// <param name="loggerFactory">The logger factory to create logger instances.</param>
  public LibraryLoader(ILoggerFactory loggerFactory)
  {
    logger_        = loggerFactory.CreateLogger<LibraryLoader>();
    loggerFactory_ = loggerFactory;
    binarySemaphore_ = new SemaphoreSlim(1,
                                         1);
  }

  public async ValueTask DisposeAsync()
  {
    await ResetServiceAsync(CancellationToken.None)
      .ConfigureAwait(false);
    binarySemaphore_.Dispose();
    checkHealthSingleizer_.Dispose();
    logger_.LogInformation("The LibraryLoader instance was disposed");
  }

  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  /// <summary>
  ///   Resets the service by unloading workers.
  /// </summary>
  public async ValueTask ResetServiceAsync(CancellationToken cancellationToken)
  {
    // Take the semaphore token to avoid data races between a Dispose() on a worker and CheckHealth()
    await binarySemaphore_.WaitAsync(cancellationToken)
                          .ConfigureAwait(false);
    try
    {
      logger_.LogInformation("Unloading existing workers.");
      foreach (var service in workerServices_.Values)
      {
        // Dispose the worker if necessary
        switch (service.Worker)
        {
          case IAsyncDisposable asyncDisposable:
            await asyncDisposable.DisposeAsync()
                                 .ConfigureAwait(false);
            break;
          case IDisposable disposable:
            disposable.Dispose();
            break;
        }

        service.Dispose();
      }

      workerServices_.Clear();
    }
    finally
    {
      binarySemaphore_.Release();
    }
  }

  /// <summary>
  ///   Checks the health of the library worker and the last loaded service.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A health check result indicating the status of the worker and the last loaded service.</returns>
  public async Task<HealthCheckResult> CheckHealth(CancellationToken cancellationToken = default)
    => await checkHealthSingleizer_.Call(async token =>
                                         {
                                           var serviceName = "Unknown service";
                                           // Take the semaphore token to avoid data races between a CheckHealth() on a worker and a Dispose() called in ResetServiceAsync()
                                           await binarySemaphore_.WaitAsync(token)
                                                                 .ConfigureAwait(false);
                                           try
                                           {
                                             logger_.LogInformation("Starting health check of loaded workers.");
                                             foreach (var service in workerServices_.Values)
                                             {
                                               serviceName = service.ServiceName;

                                               var health = await service.Worker.CheckHealth(token)
                                                                         .ConfigureAwait(false);

                                               if (health.IsHealthy)
                                               {
                                                 logger_.LogInformation("Service {Service} is healthy: {Message}",
                                                                        serviceName,
                                                                        health.Description ?? "");
                                               }
                                               else
                                               {
                                                 logger_.LogError("Service {Service} is unhealthy: {Message}",
                                                                  serviceName,
                                                                  health.Description);
                                                 return health;
                                               }
                                             }

                                             logger_.LogInformation("Ended health check successfully.");

                                             return HealthCheckResult.Healthy();
                                           }
                                           catch (Exception ex)
                                           {
                                             logger_.LogError(ex,
                                                              "Library worker health check failed for service '{Service}'",
                                                              serviceName);
                                             return HealthCheckResult.Unhealthy($"Library worker health check failed for service '{serviceName}': {ex.Message}",
                                                                                ex);
                                           }
                                           finally
                                           {
                                             binarySemaphore_.Release();
                                           }
                                         },
                                         cancellationToken)
                                   .ConfigureAwait(false);

  /// <summary>
  ///   Loads a library asynchronously based on the task handler and cancellation token provided.
  /// </summary>
  /// <param name="taskHandler">The task handler containing the task options.</param>
  /// <param name="dynamicLibrary">The worker library.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the name of the dynamic library loaded.</returns>
  /// <exception cref="ArmoniKSdkException">Thrown when there is an error loading the library.</exception>
  public async Task<IWorker> GetWorkerInstanceAsync(ITaskHandler      taskHandler,
                                                    DynamicLibrary    dynamicLibrary,
                                                    CancellationToken cancellationToken)
  {
    try
    {
      if (!taskHandler.DataDependencies.ContainsKey(dynamicLibrary.LibraryBlobId))
      {
        throw new ArmoniKSdkException($"No library found on data dependencies. (Library BlobId is {dynamicLibrary.LibraryBlobId})");
      }

      var key = $"{dynamicLibrary.Symbol}|{dynamicLibrary.LibraryBlobId}";
      if (workerServices_.TryGetValue(key,
                                      out var srv))
      {
        return srv.Worker;
      }

      logger_.LogInformation("Starting to LoadLibrary");
      logger_.LogInformation("Nb of current loaded assemblies: {nbAssemblyLoadContexts}",
                             workerServices_.Count);

      var service = await WorkerService.CreateWorkerService(taskHandler,
                                                            dynamicLibrary,
                                                            loggerFactory_,
                                                            cancellationToken)
                                       .ConfigureAwait(false);

      if (!workerServices_.TryAdd(key,
                                  service))
      {
        service.Dispose();
        throw new ArmoniKSdkException($"Unable to add load context {dynamicLibrary}");
      }

      logger_.LogInformation("Nb of current loaded assemblies: {nbAssemblyLoadContexts}",
                             workerServices_.Count);

      return service.Worker;
    }
    catch (Exception ex)
    {
      logger_.LogError(ex.Message);
      throw new ArmoniKSdkException(ex);
    }
  }
}
