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

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Common.Exceptions;
using ArmoniK.Extensions.CSharp.Common.Library;

namespace ArmoniK.Extensions.CSharp.Worker.Interfaces.Common.Domain.Task;

public static class TaskConfigurationExt
{
  /// <summary>
  ///   Get a DynamicLibrary from the TaskConfiguration.
  /// </summary>
  /// <param name="taskConfiguration">The task options to get the parameters from.</param>
  /// <returns>The DynamicLibrary associated with the specified library name.</returns>
  public static DynamicLibrary GetDynamicLibrary(this TaskConfiguration taskConfiguration)
  {
    if (!taskConfiguration.Options.TryGetValue(nameof(DynamicLibrary.LibraryPath),
                                               out var libraryFile))
    {
      throw new ArmoniKSdkException($"TaskConfiguration do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.LibraryPath)}' missing");
    }

    if (!taskConfiguration.Options.TryGetValue(nameof(DynamicLibrary.Symbol),
                                               out var symbol))
    {
      throw new ArmoniKSdkException($"TaskConfiguration do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.Symbol)}' missing");
    }

    if (!taskConfiguration.Options.TryGetValue(nameof(DynamicLibrary.LibraryBlobId),
                                               out var libraryId))
    {
      throw new ArmoniKSdkException($"TaskConfiguration do not comply with ArmoniK SDK convention, key '{nameof(DynamicLibrary.LibraryBlobId)}' missing");
    }

    return new DynamicLibrary
           {
             LibraryPath   = libraryFile ?? string.Empty,
             Symbol        = symbol      ?? string.Empty,
             LibraryBlobId = libraryId   ?? string.Empty,
           };
  }
}
