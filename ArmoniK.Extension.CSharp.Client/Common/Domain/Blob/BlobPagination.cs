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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Generic;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Provides pagination capabilities for listing blobs, including sorting and filtering functionalities.
/// </summary>
public class BlobPagination : Pagination<Filters>
{
}

/// <summary>
///   Represents a page of blob information in a paginated list.
/// </summary>
public record BlobPage
{
  /// <summary>
  ///   Total number of pages available.
  /// </summary>
  public int TotalPages { get; init; }

  /// <summary>
  ///   Details of the blob on this page.
  /// </summary>
  public BlobState BlobDetails { get; init; }
}
