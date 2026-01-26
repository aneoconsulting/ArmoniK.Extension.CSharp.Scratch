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

using ArmoniK.Extensions.CSharp.Client.Common.Enum;

namespace ArmoniK.Extensions.CSharp.Client.Common.Generic;

/// <summary>
///   Represents a generic pagination model to handle data pagination effectively.
///   This class can be used to manage paging information and the data subset filtering.
/// </summary>
/// <typeparam name="TFilter">The type of data being paginated.</typeparam>
/// <typeparam name="TField">The enum type of fields, used as sorting criteria.</typeparam>
public record Pagination<TFilter, TField>
  where TFilter : new()
  where TField : new()
{
  /// <summary>
  ///   The current page number.
  /// </summary>
  /// <value>The page number of the current subset.</value>
  public int Page { get; set; }

  /// <summary>
  ///   The number of items per page.
  /// </summary>
  /// <value>The maximum number of items to be displayed on a single page.</value>
  public int PageSize { get; set; }

  /// <summary>
  ///   The total number of items across all pages.
  /// </summary>
  /// <value>The total count of items.</value>
  public int Total { get; set; }

  /// <summary>
  ///   The sorting direction for pagination results.
  /// </summary>
  /// <value>The direction in which the sorted data should be ordered (e.g., ascending, descending).</value>
  public SortDirection SortDirection { get; set; }

  /// <summary>
  ///   The sorting criteria
  /// </summary>
  public TField SortField { get; set; } = new();

  /// <summary>
  ///   The filter used for the pagination.
  /// </summary>
  /// <value>The filter criteria used to determine which items appear in the pagination.</value>
  public TFilter Filter { get; set; } = new();
}
