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
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using ArmoniK.Api.gRPC.V1.Results;

namespace ArmoniK.Extension.CSharp.Client.Filtering;

internal abstract class FilterExpressionTreeVisitor
{
  protected readonly Stack<Type>   ExpressionTypeStack = new();
  protected readonly Stack<object> FilterStack         = new();

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

  private void Visit(Expression node,
                     bool       notOp = false)
  {
    if (node is MethodCallExpression call)
    {
      var memberAccess = call.Object as MemberExpression;
      var typeName     = call.Method.DeclaringType?.FullName ?? "";
      if (call.Method.Name == nameof(Queryable.Where) && typeName == "System.Linq.Queryable")
      {
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        Visit(lambda.Body);
      }
      else if (call.Method.Name == nameof(string.StartsWith) && typeName == "System.String" && memberAccess?.Expression.NodeType == ExpressionType.Parameter)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid blob filter: StartsWith method overload not supported.");
        }

        Visit(call.Object);
        Visit(call.Arguments[0]);
        OnMethodOperator(call.Method);
      }
      else if (call.Method.Name == nameof(string.EndsWith) && typeName == "System.String" && memberAccess?.Expression.NodeType == ExpressionType.Parameter)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid blob filter: EndsWith method overload not supported.");
        }

        Visit(call.Object);
        Visit(call.Arguments[0]);
        OnMethodOperator(call.Method);
      }
      else if (call.Method.Name == nameof(string.Contains) && typeName == "System.String" && memberAccess?.Expression.NodeType == ExpressionType.Parameter)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid blob filter: Contains method overload not supported.");
        }

        Visit(call.Object);
        Visit(call.Arguments[0]);
        OnMethodOperator(call.Method,
                         notOp);
      }
      else
      {
        // execute the method
        EvaluateExpression(call);
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
      if (LeftMostExpressionIsLambdaParameter(binary.Left) || LeftMostExpressionIsLambdaParameter(binary.Right))
      {
        Visit(binary.Left);
        Visit(binary.Right);
        OnBinaryOperator(binary.NodeType);
      }
      else
      {
        EvaluateExpression(binary);
      }
    }
    else if (node is UnaryExpression unary)
    {
      if (unary.NodeType == ExpressionType.Not && LeftMostExpressionIsLambdaParameter(unary.Operand))
      {
        Visit(unary.Operand,
              !notOp);
      }
      else
      {
        EvaluateExpression(unary);
      }
    }
  }

  private void EvaluateExpression(Expression expr)
  {
    Expression<Func<object>> lambda = null;
    try
    {
      var objectExpr = Expression.Convert(expr,
                                          typeof(object));
      lambda = Expression.Lambda<Func<object>>(objectExpr);
      var result = lambda.Compile()();
      ExpressionTypeStack.Push(result.GetType());
      FilterStack.Push(result);
    }
    catch (Exception ex)
    {
      throw new InvalidExpressionException("Invalid blob filter: could not evaluate method call " + lambda,
                                           ex);
    }
  }

  protected abstract void OnConstant(ConstantExpression           constant);
  protected abstract void OnPropertyMemberAccess(MemberExpression member);
  protected abstract bool OnFieldAccess(MemberExpression          member);
  protected abstract void OnBinaryOperator(ExpressionType         expressionType);

  protected abstract void OnMethodOperator(MethodInfo method,
                                           bool       notOp = false);

  protected abstract bool LeftMostExpressionIsLambdaParameter(Expression expression);
}
