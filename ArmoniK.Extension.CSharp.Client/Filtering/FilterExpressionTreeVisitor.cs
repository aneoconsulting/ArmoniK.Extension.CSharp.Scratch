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
using System.Reflection;

using ArmoniK.Api.gRPC.V1.Results;

namespace ArmoniK.Extension.CSharp.Client.Filtering;

internal abstract class FilterExpressionTreeVisitor
{
  protected readonly Stack<Expression> ExpressionStack     = new();
  protected readonly Stack<Type>       ExpressionTypeStack = new();
  protected readonly Stack<object>     FilterStack         = new();

  public FilterExpressionTreeVisitor(Expression tree)
    => Tree = tree;

  public Expression Tree { get; init; }

  public Filters Filters { get; private set; }

  public void Visit()
  {
    Visit(Tree);
    var filter = FilterStack.Pop();
    if (!FilterStack.Any())
    {
      if (filter is Filters orNode)
      {
        Filters = orNode;
      }
      else if (filter is FiltersAnd andNode)
      {
        Filters = new Filters();
        Filters.Or.Add(andNode);
      }
      else if (filter is FilterField filterField)
      {
        Filters = new Filters();
        var and = new FiltersAnd();
        and.And.Add(filterField);
        Filters.Or.Add(and);
      }
      else
      {
        throw new InvalidOperationException("Internal error, blob filter ignored.");
      }
    }
  }

  private void Visit(Expression node)
  {
    if (node is MethodCallExpression call)
    {
      var typeName = call.Method.DeclaringType?.FullName ?? "";
      if (call.Method.Name == "Where" && typeName == "System.Linq.Queryable")
      {
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        Visit(lambda.Body);
      }
    }
    else if (node is ConstantExpression constant)
    {
      OnConstant(constant);
    }
    else if (node is MemberExpression member)
    {
      switch (member.Member.MemberType)
      {
        case MemberTypes.Property:
          OnPropertyMemberAccess(member);
          break;
        case MemberTypes.Field:
          OnFieldAccess(member);
          break;
      }
    }
    else if (node is BinaryExpression binary)
    {
      Visit(binary.Left);
      Visit(binary.Right);
      OnBinaryOperator(binary.NodeType);
    }
  }

  protected abstract void OnConstant(ConstantExpression           constant);
  protected abstract void OnPropertyMemberAccess(MemberExpression member);
  protected abstract void OnFieldAccess(MemberExpression          member);
  protected abstract void OnBinaryOperator(ExpressionType         expressionType);
}
