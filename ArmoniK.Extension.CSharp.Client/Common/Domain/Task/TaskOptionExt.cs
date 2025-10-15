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
using ArmoniK.Extension.CSharp.Client.Exceptions;
using ArmoniK.Extension.CSharp.Client.Library;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

/// <summary>
///   Provides extension methods for TaskOption.
/// </summary>
public static class TaskOptionExt
{
  /// <summary>
  ///   Get a DynamicLibrary from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to get the parameters from.</param>
  /// <returns>The DynamicLibrary associated with the specified library name.</returns>
  public static DynamicLibrary GetDynamicLibrary(this TaskOptions taskOptions)
  {
    if (!taskOptions.Options.TryGetValue(nameof(DynamicLibrary.LibraryPath),
                                         out var libraryFile))
    {
      throw new ArmoniKSdkException($"TaskOptions do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.LibraryPath)}' missing");
    }

    if (!taskOptions.Options.TryGetValue(nameof(DynamicLibrary.Symbol),
                                         out var symbol))
    {
      throw new ArmoniKSdkException($"TaskOptions do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.Symbol)}' missing");
    }

    if (!taskOptions.Options.TryGetValue(nameof(DynamicLibrary.LibraryBlobId),
                                         out var libraryId))
    {
      throw new ArmoniKSdkException($"TaskOptions do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.LibraryBlobId)}' missing");
    }

    return new DynamicLibrary
           {
             LibraryPath   = libraryFile ?? string.Empty,
             Symbol        = symbol      ?? string.Empty,
             LibraryBlobId = libraryId   ?? string.Empty,
           };
  }

  /// <summary>
  ///   Get the convention version from the TaskOptions.
  /// </summary>
  /// <param name="taskOptions">The task options to get the parameter from.</param>
  /// <returns>The convention version option.</returns>
  /// <exception cref="ArmoniKSdkException">When the key "ConventionVersion" is not found</exception>
  public static string GetConventionVersion(this TaskOptions taskOptions)
  {
    if (!taskOptions.Options.TryGetValue(nameof(DynamicLibrary.ConventionVersion),
                                         out var value))
    {
      throw new ArmoniKSdkException($"TaskOptions do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.ConventionVersion)}' missing");
    }

    return value;
  }
}
