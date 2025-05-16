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
using ArmoniK.Extension.CSharp.DllCommon;

using Google.Protobuf.Collections;

namespace ArmoniK.Extension.CSharp.Client.DllHelper.Common;

/// <summary>
///   Provides a specialized configuration for tasks that use dynamic libraries. This configuration extends the basic
///   <see cref="TaskConfiguration" />
///   by including a collection of dynamic libraries along with additional task options.
/// </summary>
public record DllTasksConfiguration : TaskConfiguration
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="DllTasksConfiguration" /> record with specified dynamic libraries and
  ///   task settings.
  /// </summary>
  /// <param name="dynamicLibraries">A collection of dynamic libraries.</param>
  /// <param name="maxRetries">The maximum number of retry attempts for the task.</param>
  /// <param name="priority">The execution priority of the task.</param>
  /// <param name="partitionId">A unique identifier for the task partition.</param>
  /// <param name="maxDuration">The maximum duration for the task execution.</param>
  /// <param name="options">Additional configuration options as key-value pairs.</param>
  public DllTasksConfiguration(IEnumerable<DynamicLibrary> dynamicLibraries,
                               int                         maxRetries,
                               int                         priority,
                               string                      partitionId,
                               TimeSpan                    maxDuration,
                               Dictionary<string, string>? options = null)
    : base(maxRetries,
           priority,
           partitionId,
           maxDuration,
           options)
    => DynamicLibraries = dynamicLibraries;

  /// <summary>
  ///   Initializes a new instance of the <see cref="DllTasksConfiguration" /> record, copying settings from an existing
  ///   <see cref="TaskConfiguration" />
  ///   and adding dynamic libraries.
  /// </summary>
  /// <param name="dynamicLibraries">A collection of dynamic libraries.</param>
  /// <param name="taskConfiguration">An existing task configuration to copy settings from.</param>
  public DllTasksConfiguration(IEnumerable<DynamicLibrary> dynamicLibraries,
                               TaskConfiguration           taskConfiguration)
    : base(taskConfiguration.MaxRetries,
           taskConfiguration.Priority,
           taskConfiguration.PartitionId,
           taskConfiguration.MaxDuration,
           taskConfiguration.Options)
    => DynamicLibraries = dynamicLibraries;

  /// <summary>
  ///   Collection of dynamic libraries associated with the task configuration.
  /// </summary>
  public IEnumerable<DynamicLibrary> DynamicLibraries { get; }

  /// <summary>
  ///   Converts this <see cref="TaskConfiguration" /> instance to a <see cref="TaskOptions" /> suitable for use with task
  ///   submission.
  /// </summary>
  /// <returns>A new <see cref="TaskOptions" /> instance populated with the settings from this configuration.</returns>
  public new TaskOptions ToTaskOptions()
  {
    var taskOptions = base.ToTaskOptions();

    if (DynamicLibraries.Any())
    {
      foreach (var lib in DynamicLibraries)
      {
        taskOptions.Options.Add(new MapField<string, string>
                                {
                                  Keys =
                                  {
                                    $"{lib}.PathToFile",
                                    $"{lib}.DllFileName",
                                    $"{lib}.Version",
                                  },
                                  Values =
                                  {
                                    lib.PathToFile,
                                    lib.DllFileName,
                                    lib.Version,
                                  },
                                });
      }
    }

    return taskOptions;
  }
}
