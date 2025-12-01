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

using System.Text;
using System.Text.Json;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.Common.Exceptions;
using ArmoniK.Extension.CSharp.Worker.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Worker.Interfaces;
using ArmoniK.Extension.CSharp.Worker.Interfaces.Handles;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Worker;

/// <summary>
///   Handle the run of a task with the support of the SDK.
/// </summary>
public static class SdkTaskRunner
{
  /// <summary>
  ///   Run a task.
  /// </summary>
  /// <param name="taskHandler">The task handler containing task details and data.</param>
  /// <param name="worker">The worker that executes the task.</param>
  /// <param name="logger">The logger instance for recording execution information.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  public static async Task<Output> Run(ITaskHandler      taskHandler,
                                       IWorker           worker,
                                       ILogger           logger,
                                       CancellationToken cancellationToken)
  {
    var conventionVersion = taskHandler.TaskOptions.GetConventionVersion();
    if (conventionVersion != "v1")
    {
      throw new ArmoniKSdkException($"ArmoniK SDK version '{conventionVersion}' not supported.");
    }

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
      logger.LogError(ex,
                      "Could not decode payload: {Message}",
                      ex.Message);
      logger.LogError("Payload is:{Payload}",
                      payload);
      throw;
    }

    var result = await worker.ExecuteAsync(sdkTaskHandler,
                                           logger,
                                           cancellationToken)
                             .ConfigureAwait(false);

    logger.LogInformation("Got the following result from the execution: {result}",
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
}
