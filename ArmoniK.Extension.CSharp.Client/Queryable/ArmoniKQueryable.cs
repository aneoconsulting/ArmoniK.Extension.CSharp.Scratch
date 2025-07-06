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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Class that define a query object
/// </summary>
/// <typeparam name="TSource"></typeparam>
internal class ArmoniKQueryable<TSource> : IOrderedQueryable<TSource>
{
  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="provider">The query provider</param>
  /// <exception cref="ArgumentNullException">When provider is null</exception>
  public ArmoniKQueryable(IAsyncQueryProvider<TSource> provider)
  {
    Provider   = provider ?? throw new ArgumentNullException(nameof(provider));
    Expression = Expression.Constant(this);
  }

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="provider">The query provider</param>
  /// <param name="tree">The filtering tree</param>
  /// <exception cref="ArgumentNullException">When provider or tree is null</exception>
  public ArmoniKQueryable(IQueryProvider provider,
                          Expression     tree)
  {
    Provider   = provider ?? throw new ArgumentNullException(nameof(provider));
    Expression = tree     ?? throw new ArgumentNullException(nameof(tree));
  }

  /// <inheritdoc />
  public Type ElementType
    => typeof(TSource);

  /// <inheritdoc />
  public Expression Expression { get; }

  /// <inheritdoc />
  public IQueryProvider Provider { get; }

  /// <inheritdoc />
  public IEnumerator<TSource> GetEnumerator()
    => ((IEnumerable<TSource>)Provider.Execute(Expression)).GetEnumerator();

  IEnumerator IEnumerable.GetEnumerator()
    => GetEnumerator();

  /// <summary>
  ///   Makes the query object asynchronously enumerable
  /// </summary>
  /// <returns>The object asynchronously enumerable</returns>
  public IAsyncEnumerable<TSource> AsAsyncEnumerable()
    => new ArmoniKQueryableAsync(this);

  internal class ArmoniKQueryableAsync(ArmoniKQueryable<TSource> queryable) : IAsyncEnumerable<TSource>
  {
    private readonly ArmoniKQueryable<TSource> queryable_ = queryable;

    public IAsyncEnumerator<TSource> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
      var asyncProvider = (IAsyncQueryProvider<TSource>)queryable_.Provider;
      return asyncProvider.ExecuteAsync(queryable_.Expression,
                                        cancellationToken)
                          .GetAsyncEnumerator(cancellationToken);
    }
  }
}
