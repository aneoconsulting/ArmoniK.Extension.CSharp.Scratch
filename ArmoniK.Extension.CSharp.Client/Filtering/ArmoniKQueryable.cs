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

namespace ArmoniK.Extension.CSharp.Client.Filtering;

/// <summary>
///   Class that define a query object
/// </summary>
/// <typeparam name="TElement"></typeparam>
public class ArmoniKQueryable<TElement> : IQueryable<TElement>
{
  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="provider">The query provider</param>
  /// <exception cref="ArgumentNullException">When provider is null</exception>
  public ArmoniKQueryable(IAsyncQueryProvider<TElement> provider)
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
    => typeof(TElement);

  /// <inheritdoc />
  public Expression Expression { get; }

  /// <inheritdoc />
  public IQueryProvider Provider { get; }

  /// <inheritdoc />
  public IEnumerator<TElement> GetEnumerator()
    => ((IEnumerable<TElement>)Provider.Execute(Expression)).GetEnumerator();

  IEnumerator IEnumerable.GetEnumerator()
    => GetEnumerator();

  /// <summary>
  ///   Makes the query object asynchronously queryable
  /// </summary>
  /// <returns>The object asynchronously queryable</returns>
  public IAsyncEnumerable<TElement> AsAsyncEnumerable()
    => new ArmoniKQueryableAsync(this);

  internal class ArmoniKQueryableAsync(ArmoniKQueryable<TElement> queryable) : IAsyncEnumerable<TElement>
  {
    private readonly ArmoniKQueryable<TElement> queryable_ = queryable;

    public IAsyncEnumerator<TElement> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
      var asyncProvider = (IAsyncQueryProvider<TElement>)queryable_.Provider;
      return asyncProvider.ExecuteAsync(queryable_.Expression,
                                        cancellationToken)
                          .GetAsyncEnumerator(cancellationToken);
    }
  }
}
