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

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.Common.Options;
using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.Worker;

using Grpc.Core;

namespace ArmoniK.Extension.CSharp.DllWorker;

/// <summary>
///   Represents a computer service that processes tasks.
/// </summary>
public class ComputerService : WorkerStreamWrapper
{
  /// <summary>
  ///   Initializes a new instance of the ComputerService class.
  /// </summary>
  /// <param name="serviceRequestContext">The context for the service request.</param>
  /// <param name="loggerFactory">The factory to create logger instances.</param>
  /// <param name="computePlaneOptions">The compute plane options for the service.</param>
  /// <param name="provider">The provider for gRPC channels.</param>
  public ComputerService(ServiceRequestContext serviceRequestContext,
                         ILoggerFactory        loggerFactory,
                         ComputePlane          computePlaneOptions,
                         GrpcChannelProvider   provider)
    : base(loggerFactory,
           computePlaneOptions,
           provider)
  {
    ServiceRequestContext = serviceRequestContext;
    Logger                = loggerFactory.CreateLogger<ComputerService>();
  }

  /// <summary>
  ///   Gets or sets the service request context.
  /// </summary>
  public ServiceRequestContext ServiceRequestContext { get; set; }

  /// <summary>
  ///   Gets or sets the logger for the ComputerService.
  /// </summary>
  public ILogger<ComputerService> Logger { get; set; }

  /// <summary>
  ///   Processes the task asynchronously.
  /// </summary>
  /// <param name="taskHandler">The task handler for the task to be processed.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task that represents the asynchronous operation, containing the output of the task.</returns>
  /// <exception cref="RpcException">Thrown when an unmanaged exception occurs while executing the task.</exception>
  public override async Task<Output> ProcessAsync(ITaskHandler      taskHandler,
                                                  CancellationToken cancellationToken)
  {
    using var scopedLog = Logger.BeginNamedScope("Execute task",
                                                 ("Session", taskHandler.SessionId),
                                                 ("TaskId", taskHandler.TaskId));
    Logger.LogTrace("DataDependencies {DataDependencies}",
                    taskHandler.DataDependencies.Keys);
    Logger.LogTrace("ExpectedResults {ExpectedResults}",
                    taskHandler.ExpectedResults);
    Output output;
    try
    {
      await ServiceRequestContext.ExecuteTaskAsync(taskHandler,
                                                   cancellationToken)
                                 .ConfigureAwait(false);

      output = new Output
               {
                 Ok = new Empty(),
               };
    }
    catch (WorkerApiException ex)
    {
      Logger.LogError(ex,
                      "WorkerAPIException failure while executing task");

      return new Output
             {
               Error = new Output.Types.Error
                       {
                         Details = ex.Message + Environment.NewLine + ex.StackTrace,
                       },
             };
    }
    catch (Exception ex)
    {
      Logger.LogError(ex,
                      "Unmanaged exception while executing task");

      throw new RpcException(new Status(StatusCode.Internal,
                                        ex.Message + '\n' + ex.StackTrace)); // Is this right ?
    }

    return output;
  }

  /// <summary>
  ///   Checks the health of the service.
  /// </summary>
  /// <param name="request"></param>
  /// <param name="context"></param>
  /// <returns></returns>
  public override async Task<HealthCheckReply> HealthCheck(Empty             request,
                                                           ServerCallContext context)
  {
    var isHealthy = await ServiceRequestContext.LibraryWorker.CheckHealth(context.CancellationToken)
                                               .ConfigureAwait(false);

    return new HealthCheckReply
           {
             Status = isHealthy.IsHealthy
                        ? HealthCheckReply.Types.ServingStatus.Serving
                        : HealthCheckReply.Types.ServingStatus.NotServing,
           };
  }
}
