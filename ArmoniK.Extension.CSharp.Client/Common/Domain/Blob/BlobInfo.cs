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

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Represents information that minimally defines a blob.
/// </summary>
public record class BlobInfo
{
  /// <summary>
  ///   Session ID associated with the blob.
  /// </summary>
  public string SessionId { get; init; }

  /// <summary>
  ///   Name of the blob.
  /// </summary>
  public string BlobName { get; init; }

  /// <summary>
  ///   Blob unique identifier.
  /// </summary>
  public string BlobId { get; init; }

  /// <summary>
  ///   The ID of the Task that as submitted this result.
  /// </summary>
  public string CreatedBy { get; init; } = string.Empty;
}
