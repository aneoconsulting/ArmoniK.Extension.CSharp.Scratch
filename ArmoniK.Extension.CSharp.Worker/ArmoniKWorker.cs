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

using ArmoniK.Api.Worker.Utils;

using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Extension.CSharp.Worker;

/// <summary>
///   The server handling tasks execution.
/// </summary>
public static class ArmoniKWorker
{
  /// <summary>
  ///   Start the worker service.
  /// </summary>
  /// <typeparam name="T">The type handling the execution of a task</typeparam>
  public static void Run<T>()
    where T : class, IServiceRequestContext
    => WorkerServer.Create<ComputerService>(serviceConfigurator: collection =>
                                                                 {
                                                                   collection.AddSingleton<IServiceRequestContext, T>();
                                                                   collection.AddSingleton<ComputerService>();
                                                                 })
                   .Run();
}
