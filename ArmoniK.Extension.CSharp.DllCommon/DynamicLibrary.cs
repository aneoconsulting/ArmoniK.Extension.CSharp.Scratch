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
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

namespace ArmoniK.Extension.CSharp.DllCommon;

/// <summary>
///   Represents the configuration of an application, including its name, version, namespace, service, and engine type.
/// </summary>
public record DynamicLibrary
{
  /// <summary>
  ///   FileName of the Dll.
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   FileName of the Dll.
  /// </summary>
  public string DllFileName { get; init; }

  /// <summary>
  ///   Version of the application.
  /// </summary>
  public string PathToFile { get; init; }

  /// <summary>
  ///   Version of the application.
  /// </summary>
  public string Version { get; init; }

  /// <summary>
  ///   Library Blob Identifier.
  /// </summary>
  public string LibraryBlobId { get; set; }

  /// <summary>
  ///   Returns a string representation of the DynamicLibrary instance.
  /// </summary>
  /// <returns>A string that represents the current DynamicLibrary.</returns>
  public override string ToString()
    => $"{Name}-{Version}";
}

/// <summary>
///   Provides extension methods for handling DynamicLibrary instances within TaskConfiguration and TaskOptions.
/// </summary>
public static class DynamicLibraryExt
{
  /// <summary>
  ///   Adds a DynamicLibrary to the TaskConfiguration.
  /// </summary>
  /// <param name="taskConfiguration">The task configuration to add to.</param>
  /// <param name="dynamicLibrary">The DynamicLibrary to add.</param>
  /// <returns>The updated TaskConfiguration.</returns>
  public static TaskConfiguration AddDynamicLibrary(this TaskConfiguration taskConfiguration,
                                                    DynamicLibrary         dynamicLibrary)
  {
    taskConfiguration.Options.Add($"{dynamicLibrary}.Name",
                                  dynamicLibrary.Name);
    taskConfiguration.Options.Add($"{dynamicLibrary}.PathToFile",
                                  dynamicLibrary.PathToFile);
    taskConfiguration.Options.Add($"{dynamicLibrary}.DllFileName",
                                  dynamicLibrary.DllFileName);
    taskConfiguration.Options.Add($"{dynamicLibrary}.Version",
                                  dynamicLibrary.Version);
    taskConfiguration.Options.Add($"{dynamicLibrary}.LibraryBlobId",
                                  dynamicLibrary.LibraryBlobId);
    return taskConfiguration;
  }

  /// <summary>
  ///   Retrieves the service library from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to retrieve from.</param>
  /// <returns>The value of the service library option.</returns>
  public static string GetServiceLibrary(this TaskOptions taskOptions)
    => taskOptions.Options.FirstOrDefault(x => x.Key.Equals("ServiceLibrary"))
                  .Value;

  /// <summary>
  ///   Retrieves a DynamicLibrary from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to retrieve from.</param>
  /// <param name="libraryName">The name of the library.</param>
  /// <returns>The DynamicLibrary associated with the specified library name.</returns>
  public static DynamicLibrary GetDynamicLibrary(this TaskOptions taskOptions,
                                                 string           libraryName)
  {
    taskOptions.Options.TryGetValue($"{libraryName}.Name",
                                    out var name);
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
             DllFileName   = dllFileName,
             PathToFile    = pathToFile,
             Version       = version,
             LibraryBlobId = libraryId,
           };
  }
}
