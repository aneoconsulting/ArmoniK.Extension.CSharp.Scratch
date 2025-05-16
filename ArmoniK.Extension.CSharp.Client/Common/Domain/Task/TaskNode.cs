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

using System;
using System.Collections.Generic;
using System.Collections.Immutable;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

/// <summary>
///   Contains details about data dependencies, payloads, and session-specific configurations for a task.
/// </summary>
public record TaskNode
{
  /// <summary>
  ///   Expected outputs as a list of <see cref="BlobInfo" />.
  /// </summary>
  public IEnumerable<BlobInfo> ExpectedOutputs { get; init; }

  /// <summary>
  ///   Collection of <see cref="BlobInfo" /> representing the data dependencies required by the task.
  /// </summary>
  public ICollection<BlobInfo> DataDependencies { get; init; } = new List<BlobInfo>();

  /// <summary>
  ///   Dictionary of data dependencies with their content, where each key is a string identifier and the value is
  ///   the binary content. This represents DataDependencies that were not yet sent to ArmoniK.
  /// </summary>
  public IEnumerable<KeyValuePair<string, ReadOnlyMemory<byte>>> DataDependenciesContent { get; init; } = ImmutableDictionary<string, ReadOnlyMemory<byte>>.Empty;

  /// <summary>
  ///   <see cref="BlobInfo" /> for the task's payload.
  /// </summary>
  public BlobInfo Payload { get; set; } = null;

  /// <summary>
  ///   Content of the payload, where the key is a string identifier and the value is the binary content/>.
  /// </summary>
  public KeyValuePair<string, ReadOnlyMemory<byte>> PayloadContent { get; init; } = new(string.Empty,
                                                                                        ReadOnlyMemory<byte>.Empty);

  /// <summary>
  ///   Gets the configuration options for the task, which may be null if no additional configurations are specified.
  /// </summary>
  public TaskConfiguration TaskOptions { get; set; } = new();

  /// <summary>
  ///   Gets the session information associated with the task, encapsulated in a <see cref="SessionInfo" />.
  /// </summary>
  public SessionInfo Session { get; init; }
}
