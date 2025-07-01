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

namespace ArmoniK.Extension.CSharp.Worker;

/// <summary>
///   Represents a worker that handles the execution of tasks within a specific library context.
/// </summary>
public interface ILibraryWorker
{
  /// <summary>
  ///   Executes a task asynchronously within a specified library context.
  /// </summary>
  /// <param name="taskHandler">The task handler.</param>
  /// <param name="libraryLoader">The library loader to load the necessary assemblies.</param>
  /// <param name="libraryContext">The context of the library to execute the task in.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the output of the executed task.</returns>
  /// <exception cref="WorkerApiException">Thrown when there is an error in the service library or execution process.</exception>
  Task<Output> ExecuteAsync(ITaskHandler      taskHandler,
                            ILibraryLoader    libraryLoader,
                            string            libraryContext,
                            CancellationToken cancellationToken);

  /// <summary>
  ///   Checks the health of the library worker.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>True if the library worker is healthy; otherwise, false.</returns>
  public bool CheckHealth(CancellationToken cancellationToken = default);
}
