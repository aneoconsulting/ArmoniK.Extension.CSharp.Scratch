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
///   Represents a task library definition which extends the DynamicLibrary class.
///   It is used for defining which namespace and service the tasks will consume from the dll
/// </summary>
public record TaskLibraryDefinition : DynamicLibrary
{
  /// <summary>
  ///   Initializes a new instance of the TaskLibraryDefinition class.
  /// </summary>
  /// <param name="dll">The dynamic library instance.</param>
  /// <param name="space">Namespace of the application.</param>
  /// <param name="service">Service name of the application (class name).</param>
  public TaskLibraryDefinition(DynamicLibrary dll,
                               string         space,
                               string         service)
    : base(dll)
  {
    Namespace = space;
    Service   = service;
  }

  /// <summary>
  ///   Namespace of the application.
  /// </summary>
  public string Namespace { get; init; }

  /// <summary>
  ///   Service name of the application.
  /// </summary>
  public string Service { get; init; }

  /// <summary>
  ///   Returns a string representation of the TaskLibraryDefinition instance.
  /// </summary>
  /// <returns>A string that represents the current TaskLibraryDefinition.</returns>
  public override string ToString()
    => $"{Name}-{Version}";
}

/// <summary>
///   Provides extension methods for TaskLibraryDefinition.
/// </summary>
public static class TaskLibraryDefinitionExt
{
  /// <summary>
  ///   Adds a TaskLibraryDefinition to the TaskConfiguration.
  /// </summary>
  /// <param name="taskConfiguration">The task configuration to add to.</param>
  /// <param name="dynamicLibrary">The TaskLibraryDefinition to add.</param>
  /// <returns>The updated TaskConfiguration.</returns>
  public static TaskConfiguration AddTaskLibraryDefinition(this TaskConfiguration taskConfiguration,
                                                           TaskLibraryDefinition  dynamicLibrary)
  {
    taskConfiguration.AddDynamicLibrary(dynamicLibrary);
    taskConfiguration.Options.Add($"{dynamicLibrary}.Namespace",
                                  dynamicLibrary.Namespace);
    taskConfiguration.Options.Add($"{dynamicLibrary}.Service",
                                  dynamicLibrary.Service);
    taskConfiguration.Options.Add("ServiceLibrary",
                                  dynamicLibrary.ToString());
    return taskConfiguration;
  }

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
}
