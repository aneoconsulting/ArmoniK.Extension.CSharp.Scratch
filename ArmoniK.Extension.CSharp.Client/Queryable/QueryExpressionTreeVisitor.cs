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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using ArmoniK.Utils;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Abstract base class for visiting and analyzing LINQ query expression trees.
///   Converts LINQ expressions into filter and sort criteria for ArmoniK queries.
/// </summary>
/// <typeparam name="TSource">The source type being queried.</typeparam>
/// <typeparam name="TEnumField">The enumeration type for field identifiers.</typeparam>
/// <typeparam name="TFilterOr">The type representing OR filter operations.</typeparam>
/// <typeparam name="TFilterAnd">The type representing AND filter operations.</typeparam>
/// <typeparam name="TFilterField">The type representing individual field filters.</typeparam>
internal abstract class QueryExpressionTreeVisitor<TSource, TEnumField, TFilterOr, TFilterAnd, TFilterField>
  where TFilterOr : new()
  where TFilterAnd : new()
{
  public Func<IAsyncEnumerable<TSource>, TSource?>? FuncReturnNullableTSource { get; private set; }
  public Func<IAsyncEnumerable<TSource>, TSource>?  FuncReturnTSource         { get; private set; }

  public TFilterOr Filters { get; private set; }

  public TEnumField SortCriteria { get; protected set; }

  public bool IsSortAscending { get; protected set; }

  protected abstract bool                                                                        IsWhereExpressionTreeVisitorInstantiated { get; }
  protected abstract WhereExpressionTreeVisitor<TEnumField, TFilterOr, TFilterAnd, TFilterField> WhereExpressionTreeVisitor               { get; }
  protected abstract OrderByExpressionTreeVisitor<TEnumField>                                    OrderByWhereExpressionTreeVisitor        { get; }

  /// <summary>
  ///   Visits and analyzes the provided expression tree.
  /// </summary>
  /// <param name="tree">The expression tree to analyze.</param>
  public void VisitTree(Expression tree)
  {
    VisitTreeInternal(tree);
    if (IsWhereExpressionTreeVisitorInstantiated)
    {
      // A Where() was found
      Filters = WhereExpressionTreeVisitor.GetFilterOrRootNode();
    }
    else
    {
      Filters = new TFilterOr();
    }
  }

  private void VisitTreeInternal(Expression tree)
  {
    if (tree is MethodCallExpression call)
    {
      if (call.Method.DeclaringType == typeof(System.Linq.Queryable))
      {
        var thisType = call.Arguments[0].Type;
        if (thisType == typeof(IQueryable<TSource>) || thisType == typeof(IOrderedQueryable<TSource>))
        {
          // This is a call of an extension method of IQueryable<TElement>,
          // then lets left recursively walk along the expression tree.
          VisitTreeInternal(call.Arguments[0]);
        }
        else if (thisType == typeof(ArmoniKQueryable<TSource>))
        {
          // This is the leftmost element of the LINQ query, it represents the entire collection.
          // Nothing to do here.
        }
        else
        {
          // Should never happen
          throw new InvalidOperationException("Internal error: unexpected expression type");
        }

        if (call.Method.ReturnType == typeof(IQueryable<TSource>))
        {
          HandleIQueryableOfTElementExpression(call);
        }
        else if (call.Method.ReturnType == typeof(TSource))
        {
          HandleTElementExpression(call);
        }
        else if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.OrderBy)))
        {
          // IOrderedQueryable<TSource> OrderBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
          // IOrderedQueryable<TSource> OrderBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IComparer<TKey> comparer)
          // IOrderedQueryable<TSource> OrderByDescending<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
          // IOrderedQueryable<TSource> OrderByDescending<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IComparer<TKey> comparer)
          var expression = (UnaryExpression)call.Arguments[1];
          var lambda     = (LambdaExpression)expression.Operand;

          IsSortAscending = call.Method.Name == nameof(System.Linq.Queryable.OrderBy);
          SortCriteria    = OrderByWhereExpressionTreeVisitor.Visit(lambda);
        }
        else
        {
          // IQueryable<TResult> OfType<TResult>(this IQueryable source)
          // IQueryable<TResult> Cast<TResult>(this IQueryable source)
          // IQueryable<TResult> Select<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
          // IQueryable<TResult> Select<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, TResult>> selector)
          // IQueryable<TResult> SelectMany<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, IEnumerable<TResult>>> selector)
          // IQueryable<TResult> SelectMany<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, IEnumerable<TResult>>> selector)
          // IQueryable<TResult> SelectMany<TSource, TCollection, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, IEnumerable<TCollection>>> collectionSelector, Expression<Func<TSource, TCollection, TResult>> resultSelector)
          // IQueryable<TResult> SelectMany<TSource, TCollection, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, IEnumerable<TCollection>>> collectionSelector, Expression<Func<TSource, TCollection, TResult>> resultSelector)
          // IQueryable<TResult> Join<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector)
          // IQueryable<TResult> Join<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, IEnumerable<TInner>, TResult>> resultSelector)
          // IQueryable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, IEnumerable<TInner>, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector, Expression<Func<TKey, IEnumerable<TElement>, TResult>> resultSelector)
          // IQueryable<TResult> GroupBy<TSource, TKey, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
          // IQueryable<TResult> GroupBy<TSource, TKey, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector, Expression<Func<TKey, IEnumerable<TElement>, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
          // IQueryable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector)
          // IQueryable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IEqualityComparer<TKey> comparer)
          // IQueryable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<(TFirst First, TSecond Second)> Zip<TFirst, TSecond>(this IQueryable<TFirst> source1, IEnumerable<TSecond> source2)
          // IQueryable<TResult> Zip<TFirst, TSecond, TResult>(this IQueryable<TFirst> source1, IEnumerable<TSecond> source2, Expression<Func<TFirst, TSecond, TResult>> resultSelector)
          // bool Contains<TSource>(this IQueryable<TSource> source, TSource item)
          // bool Contains<TSource>(this IQueryable<TSource> source, TSource item, IEqualityComparer<TSource> comparer)
          // bool SequenceEqual<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
          // bool SequenceEqual<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
          // bool Any<TSource>(this IQueryable<TSource> source)
          // bool Any<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
          // bool All<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
          // int Count<TSource>(this IQueryable<TSource> source)
          // int Count<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
          // long LongCount<TSource>(this IQueryable<TSource> source)
          // long LongCount<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
          // TResult Min<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
          // TResult Max<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
          // int Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int>> selector)
          // int? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int?>> selector)
          // long Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long>> selector)
          // long? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long?>> selector)
          // float Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float>> selector)
          // float? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float?>> selector)
          // double Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double>> selector)
          // double? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double?>> selector)
          // decimal Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal>> selector)
          // decimal? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector)
          // double Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int>> selector)
          // double? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int?>> selector)
          // float Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float>> selector)
          // float? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float?>> selector)
          // double Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long>> selector)
          // double? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long?>> selector)
          // double Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double>> selector)
          // double? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double?>> selector)
          // decimal Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal>> selector)
          // decimal? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector)
          // TAccumulate Aggregate<TSource, TAccumulate>(this IQueryable<TSource> source, TAccumulate seed, Expression<Func<TAccumulate, TSource, TAccumulate>> func)
          // TResult Aggregate<TSource, TAccumulate, TResult>(this IQueryable<TSource> source, TAccumulate seed, Expression<Func<TAccumulate, TSource, TAccumulate>> func, Expression<Func<TAccumulate, TResult>> selector)
          throw new NotImplementedException();
        }
      }
    }
  }

  private void HandleTElementExpression(MethodCallExpression call)
  {
    if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.First)))
    {
      if (call.Arguments.Count == 1)
      {
        // TSource First<TSource>(this IQueryable<TSource> source)
        // TSource FirstOrDefault<TSource>(this IQueryable<TSource> source)
        // Nothing to do here.
      }
      else
      {
        // TSource First<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        // TSource FirstOrDefault<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        WhereExpressionTreeVisitor.Visit(lambda);
      }

      if (call.Method.Name == nameof(System.Linq.Queryable.FirstOrDefault))
      {
        FuncReturnNullableTSource = queryable => queryable.FirstOrDefaultAsync()
                                                          .WaitSync();
      }
      else
      {
        FuncReturnTSource = queryable => queryable.FirstAsync()
                                                  .WaitSync();
      }
    }
    else
    {
      // TSource Last<TSource>(this IQueryable<TSource> source)
      // TSource Last<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // TSource LastOrDefault<TSource>(this IQueryable<TSource> source)
      // TSource LastOrDefault<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // TSource Single<TSource>(this IQueryable<TSource> source)
      // TSource Single<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // TSource SingleOrDefault<TSource>(this IQueryable<TSource> source)
      // TSource SingleOrDefault<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // TSource ElementAt<TSource>(this IQueryable<TSource> source, int index)
      // TSource ElementAtOrDefault<TSource>(this IQueryable<TSource> source, int index)
      // TSource Aggregate<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, TSource, TSource>> func)
      // TSource Min<TSource>(this IQueryable<TSource> source)
      // TSource Max<TSource>(this IQueryable<TSource> source)
      throw new NotImplementedException();
    }
  }

  private void HandleIQueryableOfTElementExpression(MethodCallExpression call)
  {
    if (call.Method.Name == nameof(System.Linq.Queryable.Where))
    {
      var expression = (UnaryExpression)call.Arguments[1];
      var lambda     = (LambdaExpression)expression.Operand;
      if (lambda.Parameters.Count == 1)
      {
        // IQueryable<TSource> Where<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        WhereExpressionTreeVisitor.Visit(lambda);
      }
      else
      {
        // IQueryable<TSource> Where<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int, bool>> predicate)
        var typename = typeof(TSource).Name;
        throw new InvalidOperationException("Expression not supported. Please consult documentation." + Environment.NewLine + "Expression is: " + call);
      }
    }
    else
    {
      // IQueryable<TSource> Take<TSource>(this IQueryable<TSource> source, int count)
      // IQueryable<TSource> TakeWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // IQueryable<TSource> TakeWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int, bool>> predicate)
      // IQueryable<TSource> Skip<TSource>(this IQueryable<TSource> source, int count)
      // IQueryable<TSource> SkipWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // IQueryable<TSource> SkipWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int, bool>> predicate)
      // IQueryable<TSource> Distinct<TSource>(this IQueryable<TSource> source)
      // IQueryable<TSource> Distinct<TSource>(this IQueryable<TSource> source, IEqualityComparer<TSource> comparer)
      // IQueryable<TSource> Concat<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Union<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Union<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
      // IQueryable<TSource> Intersect<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Intersect<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
      // IQueryable<TSource> Except<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Except<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
      // IQueryable<TSource> DefaultIfEmpty<TSource>(this IQueryable<TSource> source)
      // IQueryable<TSource> DefaultIfEmpty<TSource>(this IQueryable<TSource> source, TSource defaultValue)
      // IQueryable<TSource> Reverse<TSource>(this IQueryable<TSource> source)
      // IQueryable<TSource> SkipLast<TSource>(this IQueryable<TSource> source, int count)
      // IQueryable<TSource> TakeLast<TSource>(this IQueryable<TSource> source, int count)
      // IQueryable<TSource> Append<TSource>(this IQueryable<TSource> source, TSource element)
      // IQueryable<TSource> Prepend<TSource>(this IQueryable<TSource> source, TSource element)
      throw new NotImplementedException();
    }
  }
}
