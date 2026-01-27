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

using System;
using System.Collections.Generic;
using System.Linq;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Extensions.CSharp.Common.Library;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

/// <summary>
///   Configuration settings for a task, including retry policies, priorities, partitioning details, and
///   application-specific configurations.
/// </summary>
public record TaskConfiguration
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskConfiguration" /> class with specified task configuration
  ///   settings.
  /// </summary>
  public TaskConfiguration()
    => Options = new Dictionary<string, string>();

  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskConfiguration" /> class with specified task configuration
  ///   settings.
  /// </summary>
  /// <param name="maxRetries">The maximum number of retries for the task.</param>
  /// <param name="priority">The priority level of the task.</param>
  /// <param name="partitionId">The partition identifier for task segregation.</param>
  /// <param name="maxDuration">The maximum duration allowed for the task to complete.</param>
  /// <param name="options">Optional additional key-value pairs for further customization.</param>
  public TaskConfiguration(int                         maxRetries,
                           int                         priority,
                           string                      partitionId,
                           TimeSpan                    maxDuration,
                           Dictionary<string, string>? options = null)
  {
    MaxRetries  = maxRetries;
    Priority    = priority;
    PartitionId = partitionId;
    Options     = options ?? new Dictionary<string, string>();
    MaxDuration = maxDuration;
  }

  /// <summary>
  ///   Maximum number of retries for the task.
  /// </summary>
  public int MaxRetries { get; init; }

  /// <summary>
  ///   Priority level of the task.
  /// </summary>
  public int Priority { get; init; }

  /// <summary>
  ///   Partition identifier used for task segregation.
  /// </summary>
  public string PartitionId { get; init; } = string.Empty;

  /// <summary>
  ///   Key-value pair options for task configuration.
  /// </summary>
  public Dictionary<string, string> Options { get; init; }

  /// <summary>
  ///   Maximum duration allowed for the task to run.
  /// </summary>
  public TimeSpan MaxDuration { get; init; }

  /// <summary>
  ///   Adds a DynamicLibrary to the TaskConfiguration.
  /// </summary>
  /// <param name="dynamicLibrary">The DynamicLibrary to add.</param>
  /// <returns>The updated TaskConfiguration.</returns>
  public TaskConfiguration SetDynamicLibrary(DynamicLibrary dynamicLibrary)
  {
    Options[nameof(DynamicLibrary.LibraryPath)]   = dynamicLibrary.LibraryPath;
    Options[nameof(DynamicLibrary.Symbol)]        = dynamicLibrary.Symbol;
    Options[nameof(DynamicLibrary.LibraryBlobId)] = dynamicLibrary.LibraryBlobId;
    return this;
  }

  /// <summary>
  ///   Converts this <see cref="TaskConfiguration" /> instance to a <see cref="TaskOptions" /> suitable for use with task
  ///   submission.
  /// </summary>
  /// <returns>A new <see cref="TaskOptions" /> instance populated with the settings from this configuration.</returns>
  public TaskOptions ToTaskOptions()
  {
    var taskOptions = new TaskOptions
                      {
                        MaxRetries  = MaxRetries,
                        Priority    = Priority,
                        MaxDuration = Duration.FromTimeSpan(MaxDuration),
                        PartitionId = PartitionId,
                      };

    if (!Options.Any())
    {
      return taskOptions;
    }

    taskOptions.Options.Add(Options);

    return taskOptions;
  }
}
