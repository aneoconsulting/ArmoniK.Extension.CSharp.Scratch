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

public class ArmoniKQueryable<TElement> : IQueryable<TElement>
{
  public ArmoniKQueryable(IAsyncQueryProvider<TElement> provider)
  {
    Provider   = provider ?? throw new ArgumentNullException(nameof(provider));
    Expression = Expression.Constant(this);
  }

  public ArmoniKQueryable(IQueryProvider provider,
                          Expression     tree)
  {
    Provider   = provider ?? throw new ArgumentNullException(nameof(provider));
    Expression = tree;
  }

  /// <inheritdoc />
  public Type ElementType
    => typeof(TElement);

  public Expression Expression { get; }

  public IQueryProvider Provider { get; }

  public IEnumerator<TElement> GetEnumerator()
    => ((IEnumerable<TElement>)Provider.Execute(Expression)).GetEnumerator();

  IEnumerator IEnumerable.GetEnumerator()
    => GetEnumerator();

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
