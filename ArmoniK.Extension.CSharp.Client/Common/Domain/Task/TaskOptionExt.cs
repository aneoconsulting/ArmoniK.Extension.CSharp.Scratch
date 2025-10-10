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

using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Extension.CSharp.Client.Library;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

/// <summary>
///   Provides extension methods for TaskOption.
/// </summary>
public static class TaskOptionExt
{
  /// <summary>
  ///   Retrieves a TaskLibraryDefinition from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to retrieve from.</param>
  /// <param name="libraryName">The name of the library.</param>
  /// <returns>The TaskLibraryDefinition associated with the specified library name.</returns>
  public static TaskLibraryDefinition GetTaskLibraryDefinition(this TaskOptions taskOptions,
                                                               string           libraryName)
  {
    var dll = taskOptions.GetDynamicLibrary(libraryName);
    taskOptions.Options.TryGetValue($"{libraryName}.Namespace",
                                    out var serviceNamespace);

    taskOptions.Options.TryGetValue($"{libraryName}.Service",
                                    out var service);
    return new TaskLibraryDefinition(dll,
                                     serviceNamespace ?? string.Empty,
                                     service          ?? string.Empty);
  }

  /// <summary>
  ///   Retrieves a DynamicLibrary from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to retrieve from.</param>
  /// <param name="libraryName">The name of the library.</param>
  /// <returns>The DynamicLibrary associated with the specified library name.</returns>
  public static DynamicLibrary GetDynamicLibrary(this TaskOptions taskOptions,
                                                 string           libraryName)
  {
    if (taskOptions.Options.TryGetValue($"{libraryName}.Name",
                                        out var name))
    {
      taskOptions.Options.TryGetValue($"{libraryName}.PathToFile",
                                      out var pathToFile);
      taskOptions.Options.TryGetValue($"{libraryName}.DllFileName",
                                      out var dllFileName);
      taskOptions.Options.TryGetValue($"{libraryName}.Version",
                                      out var version);
      taskOptions.Options.TryGetValue($"{libraryName}.LibraryBlobId",
                                      out var libraryId);

      return new DynamicLibrary
             {
               Name          = name,
               DllFileName   = dllFileName ?? string.Empty,
               PathToFile    = pathToFile  ?? string.Empty,
               Version       = version     ?? string.Empty,
               LibraryBlobId = libraryId   ?? string.Empty,
             };
    }

    throw new KeyNotFoundException($"Could not find library {libraryName}");
  }

  /// <summary>
  ///   Retrieves the service library from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to retrieve from.</param>
  /// <returns>The value of the service library option.</returns>
  public static string? GetServiceLibrary(this TaskOptions taskOptions)
  {
    taskOptions.Options.TryGetValue("ServiceLibrary",
                                    out var value);
    return value;
  }
}
