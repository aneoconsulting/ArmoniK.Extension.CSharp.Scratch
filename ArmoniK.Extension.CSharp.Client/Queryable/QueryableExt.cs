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
using System.Linq.Expressions;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Provide additional extensions methods for IQueryable instances
/// </summary>
public static class QueryableExt
{
  /// <summary>
  ///   Converts an IQueryable instance to as an IAsyncEnumerable
  /// </summary>
  /// <typeparam name="T">The objets type of the collection</typeparam>
  /// <param name="queryable">The queryable instance</param>
  /// <returns>The IAsyncEnumerable instance</returns>
  public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> queryable)
    => queryable as IAsyncEnumerable<T> ?? queryable.ToAsyncEnumerable();

  /// <summary>
  ///   Sets the maximum page size for the answer.
  /// </summary>
  /// <typeparam name="T">The objets type of the collection</typeparam>
  /// <param name="source">The queryable instance</param>
  /// <param name="pageSize">The page size</param>
  /// <returns>The queryable instance</returns>
  public static IQueryable<T> WithPageSize<T>(this IQueryable<T> source,
                                              int                pageSize)
  {
    if (source == null)
    {
      throw new ArgumentNullException(nameof(source));
    }

    if (pageSize <= 0)
    {
      throw new InvalidOperationException("Page size must be greater than 0.");
    }

    return source.Provider.CreateQuery<T>(Expression.Call(null,
                                                          new Func<IQueryable<T>, int, IQueryable<T>>(WithPageSize).Method,
                                                          source.Expression,
                                                          Expression.Constant(pageSize)));
  }
}
