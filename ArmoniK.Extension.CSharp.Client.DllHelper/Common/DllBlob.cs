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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.DllCommon;

namespace ArmoniK.Extension.CSharp.Client.DllHelper.Common;

/// <summary>
///   Represents a specialized blob that includes metadata for a dynamic library. This record extends the
///   <see cref="BlobInfo" /> class,
///   encapsulating additional details specific to dynamic libraries used in task executions.
/// </summary>
public record DllBlob : BlobInfo
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="DllBlob" /> record using a specified dynamic library.
  /// </summary>
  /// <param name="dynamicLibrary">
  ///   The dynamic library associated with this blob, used to set the blob's name based on the
  ///   library's identifier or name.
  /// </param>
  public DllBlob(DynamicLibrary dynamicLibrary)
    => BlobName = dynamicLibrary.ToString();
}
