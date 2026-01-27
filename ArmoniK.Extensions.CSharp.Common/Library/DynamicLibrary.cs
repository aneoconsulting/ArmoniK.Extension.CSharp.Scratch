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

namespace ArmoniK.Extensions.CSharp.Common.Library;

/// <summary>
///   Represents the configuration of an application, including its name, version, namespace, service, and engine type.
/// </summary>
public record DynamicLibrary
{
  /// <summary>
  ///   The current version of the SDK
  /// </summary>
  public const string ConventionVersion = "v1";

  /// <summary>
  ///   Path to the library to load from the zip file.
  /// </summary>
  public string LibraryPath { get; init; } = string.Empty;

  /// <summary>
  ///   Library Blob Identifier.
  /// </summary>
  public string LibraryBlobId { get; set; } = string.Empty;

  /// <summary>
  ///   Symbol used to execute the task (full name of the class implementing the IWorker interface).
  /// </summary>
  public string Symbol { get; init; } = string.Empty;

  /// <summary>
  ///   Returns a string representation of the DynamicLibrary instance.
  /// </summary>
  /// <returns>A string that represents the current DynamicLibrary.</returns>
  public override string ToString()
    => Symbol;
}
