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

using System.Linq.Expressions;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

internal abstract class QueryableExpressionTreeVisitor<TEnumField, TFilterOr, TFilterAnd, TFilterField>
  where TFilterOr : new()
  where TFilterAnd : new()
{
  public TFilterOr Filters { get; private set; }

  public TEnumField SortCriteria { get; protected set; }

  public bool IsSortAscending { get; protected set; }

  protected abstract bool                                                                        IsWhereExpressionTreeVisitorInstantiated { get; }
  protected abstract WhereExpressionTreeVisitor<TEnumField, TFilterOr, TFilterAnd, TFilterField> WhereExpressionTreeVisitor               { get; }
  protected abstract OrderByExpressionTreeVisitor<TEnumField>                                    OrderByWhereExpressionTreeVisitor        { get; }

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
      var typeName = call.Method.DeclaringType?.FullName ?? "";
      if (call.Method.Name == nameof(System.Linq.Queryable.Where) && typeName == "System.Linq.Queryable")
      {
        if (call.Arguments[0] is MethodCallExpression call2)
        {
          // We are in the case Where().Where()
          VisitTreeInternal(call2);
        }

        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        WhereExpressionTreeVisitor.Visit(lambda);
      }
      else if ((call.Method.Name == nameof(System.Linq.Queryable.OrderBy) || call.Method.Name == nameof(System.Linq.Queryable.OrderByDescending)) &&
               typeName == "System.Linq.Queryable")
      {
        if (call.Arguments[0] is MethodCallExpression call2)
        {
          // We are in the case Where().OrderBy()
          VisitTreeInternal(call2);
        }

        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;

        IsSortAscending = call.Method.Name == nameof(System.Linq.Queryable.OrderBy);
        SortCriteria    = OrderByWhereExpressionTreeVisitor.Visit(lambda);
      }
    }
  }
}
