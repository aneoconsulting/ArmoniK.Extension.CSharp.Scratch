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

using Google.Protobuf.Reflection;

namespace ArmoniK.Extension.CSharp.Client.Common.Enum;

/// <summary>
///   Defines the directions in which sorting can be applied.
/// </summary>
public enum SortDirection
{
  /// <summary>
  ///   Unspecified sort direction. This value should not be used as it does not represent a valid sorting order.
  /// </summary>
  [OriginalName("SORT_DIRECTION_UNSPECIFIED")]
  Unspecified,

  /// <summary>
  ///   Ascending sort order where elements progress from the lowest value to the highest.
  /// </summary>
  [OriginalName("SORT_DIRECTION_ASC")]
  Asc,

  /// <summary>
  ///   Descending sort order where elements progress from the highest value to the lowest.
  /// </summary>
  [OriginalName("SORT_DIRECTION_DESC")]
  Desc,
}

/// <summary>
///   Provides extension methods for conversion purpose between protobuf types and SDK types.
/// </summary>
public static class SortDirectionExt
{
  /// <summary>
  ///   Converts a SortDirection to its corresponding protobuf enum value.
  /// </summary>
  /// <param name="direction"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public static Api.gRPC.V1.SortDirection.SortDirection ToGrpc(this SortDirection direction)
    => direction switch
       {
         SortDirection.Unspecified => Api.gRPC.V1.SortDirection.SortDirection.Unspecified,
         SortDirection.Asc         => Api.gRPC.V1.SortDirection.SortDirection.Asc,
         SortDirection.Desc        => Api.gRPC.V1.SortDirection.SortDirection.Desc,
         _ => throw new ArgumentOutOfRangeException(nameof(direction),
                                                    direction,
                                                    null),
       };

  /// <summary>
  ///   Converts a protobuf sort direction to its corresponding SortDirection value.
  /// </summary>
  /// <param name="direction"></param>
  /// <returns></returns>
  /// <exception cref="ArgumentOutOfRangeException"></exception>
  public static SortDirection ToIternal(this Api.gRPC.V1.SortDirection.SortDirection direction)
    => direction switch
       {
         Api.gRPC.V1.SortDirection.SortDirection.Unspecified => SortDirection.Unspecified,
         Api.gRPC.V1.SortDirection.SortDirection.Asc         => SortDirection.Asc,
         Api.gRPC.V1.SortDirection.SortDirection.Desc        => SortDirection.Desc,
         _ => throw new ArgumentOutOfRangeException(nameof(direction),
                                                    direction,
                                                    null),
       };
}
