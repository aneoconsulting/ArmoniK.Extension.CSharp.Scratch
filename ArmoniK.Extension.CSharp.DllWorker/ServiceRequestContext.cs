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
using ArmoniK.Api.Worker.Worker;
using ArmoniK.Extension.CSharp.DllCommon;
using ArmoniK.Extension.CSharp.Worker;

namespace ArmoniK.Extension.CSharp.DllWorker;

/// <summary>
///   Represents the context for handling service requests.
/// </summary>
public class ServiceRequestContext
{
  private readonly ILibraryLoader                 libraryLoader_;
  private readonly ILibraryWorker                 libraryWorker_;
  private readonly ILogger<ServiceRequestContext> logger_;

  private string? currentSession_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="ServiceRequestContext" /> class.
  /// </summary>
  /// <param name="configuration">The configuration settings.</param>
  /// <param name="loggerFactory">The logger factory to create logger instances.</param>
  public ServiceRequestContext(IConfiguration configuration,
                               ILoggerFactory loggerFactory)
  {
    LoggerFactory = loggerFactory;

    logger_ = loggerFactory.CreateLogger<ServiceRequestContext>();

    libraryLoader_ = new LibraryLoader(loggerFactory);
    libraryWorker_ = new LibraryWorker(configuration,
                                       loggerFactory);
  }

  /// <summary>
  ///   Gets or sets the logger factory.
  /// </summary>
  public ILoggerFactory LoggerFactory { get; set; }

  /// <summary>
  ///   Executes a task asynchronously.
  /// </summary>
  /// <param name="taskHandler">The task handler containing the task details.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  public async Task<Output> ExecuteTaskAsync(ITaskHandler      taskHandler,
                                             CancellationToken cancellationToken)
  {
    if (string.IsNullOrEmpty(currentSession_) || taskHandler.SessionId != currentSession_)
    {
      currentSession_ = taskHandler.SessionId;
      libraryLoader_.ResetService();
    }

    var contextName = await libraryLoader_.LoadLibraryAsync(taskHandler,
                                                            cancellationToken)
                                          .ConfigureAwait(false);

    var result = await libraryWorker_.ExecuteAsync(taskHandler,
                                                   libraryLoader_,
                                                   contextName,
                                                   cancellationToken)
                                     .ConfigureAwait(false);
    return result;
  }
}
