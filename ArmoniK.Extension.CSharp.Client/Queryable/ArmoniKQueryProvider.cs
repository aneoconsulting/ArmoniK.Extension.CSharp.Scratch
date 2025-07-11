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

using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;

using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Class query provider that build the protobuf filtering structure
/// </summary>
internal abstract class ArmoniKQueryProvider<TPagination, TPage, TSource, TEnumField, TFilterOr, TFilterAnd, TFilterField> : IAsyncQueryProvider<TSource>
  where TFilterOr : new()
  where TFilterAnd : new()
{
  protected readonly ILogger<IBlobService> logger_;

  /// <summary>
  ///   Create the query provider
  /// </summary>
  /// <param name="logger">The logger</param>
  protected ArmoniKQueryProvider(ILogger<IBlobService> logger)
    => logger_ = logger;

  public QueryExecution<TPagination, TPage, TSource, TEnumField, TFilterOr, TFilterAnd, TFilterField> QueryExecution { get; private set; }

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>The query object</returns>
  public IQueryable CreateQuery(Expression expression)
    => new ArmoniKQueryable<TSource>(this,
                                     expression);

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>The query object</returns>
  public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    => new ArmoniKQueryable<TElement>(this,
                                      expression);

  /// <summary>
  ///   Visit the expression tree and generate the protobuf filtering structures
  ///   and fetches the instances according to the filter.
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>An enumeration of instances compliant with the filter</returns>
  /// <exception cref="InvalidExpressionException">When the filtering expression is invalid</exception>
  public object Execute(Expression expression)
  {
    QueryExecution = CreateQueryExecution();
    QueryExecution.VisitExpression(expression);
    if (QueryExecution.FuncReturnTSource != null)
    {
      return QueryExecution.FuncReturnTSource(QueryExecution.ExecuteAsync())
                           .GetAwaiter()
                           .GetResult();
    }

    if (QueryExecution.FuncReturnNullableTSource != null)
    {
      return QueryExecution.FuncReturnNullableTSource(QueryExecution.ExecuteAsync())
                           .GetAwaiter()
                           .GetResult();
    }

    return QueryExecution.ExecuteAsync()
                         .ToListAsync()
                         .GetAwaiter()
                         .GetResult();
  }

  /// <summary>
  ///   Visit the expression tree and generate the protobuf filtering structures
  ///   and fetches the instances according to the filter.
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>An enumeration of instances compliant with the filter</returns>
  /// <exception cref="InvalidExpressionException">When the filtering expression is invalid</exception>
  public TResult Execute<TResult>(Expression expression)
    => (TResult)Execute(expression);

  /// <summary>
  ///   Visit the expression tree and generate the protobuf filtering structures
  ///   and fetches the instances according to the filter.
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <param name="cancellationToken">The cancellation token</param>
  /// <returns>An asynchronous enumeration of instances compliant with the filter</returns>
  /// <exception cref="InvalidExpressionException">When the filtering expression is invalid</exception>
  public IAsyncEnumerable<TSource> ExecuteAsync(Expression                                 expression,
                                                [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    QueryExecution = CreateQueryExecution();
    QueryExecution.VisitExpression(expression);
    return QueryExecution.ExecuteAsync(cancellationToken);
  }

  protected abstract QueryExecution<TPagination, TPage, TSource, TEnumField, TFilterOr, TFilterAnd, TFilterField> CreateQueryExecution();
}
