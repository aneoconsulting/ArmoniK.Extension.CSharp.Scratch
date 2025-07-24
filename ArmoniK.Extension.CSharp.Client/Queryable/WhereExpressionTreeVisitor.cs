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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Google.Protobuf.Collections;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Visitor class of a lambda describing a filter.
/// </summary>
/// <typeparam name="TFilterOr">The type of logical OR nodes.</typeparam>
/// <typeparam name="TFilterAnd">The type of logical AND nodes.</typeparam>
/// <typeparam name="TFilterField">The type of instance node describing a filter on a single property.</typeparam>
internal abstract class WhereExpressionTreeVisitor<TFilterOr, TFilterAnd, TFilterField>
  where TFilterOr : new()
  where TFilterAnd : new()
{
  protected readonly Stack<(object, Type)> FilterStack = new();

  /// <summary>
  ///   Visit the lambda Expression Tree describing the filter.
  /// </summary>
  /// <param name="lambda">the lambda Expression Tree.</param>
  public void Visit(LambdaExpression lambda)
  {
    Visit(lambda.Body);

    if (FilterStack.Count == 2)
    {
      // merge : left && right
      HandleBoolExpression(ExpressionType.AndAlso);
    }

    FilterStack.Push((CreateFilterFromStack()!, typeof(bool)));
  }

  /// <summary>
  ///   Get the resulting tree from the analysis stack.
  /// </summary>
  /// <returns>The root node of the tree.</returns>
  /// <exception cref="InvalidOperationException">
  ///   When the analysis stack is in an inconsistent state,
  ///   which results from an invalid Expression Tree filter.
  /// </exception>
  public TFilterOr GetFilterOrRootNode()
  {
    var (filters, _) = ((TFilterOr, Type))FilterStack.Pop();
    if (FilterStack.Any())
    {
      throw new InvalidOperationException("Internal error: analysis stack is in an inconsistent state");
    }

    return filters;
  }

  private TFilterOr CreateFilterFromStack()
  {
    var orNode = new TFilterOr();
    var (filter, _) = FilterStack.Pop();
    if (filter is TFilterOr filterOr)
    {
      orNode = filterOr;
    }
    else if (filter is TFilterAnd andNode)
    {
      GetRepeatedFilterAnd(orNode)
        .Add(andNode);
    }
    else if (filter is TFilterField filterField)
    {
      var and = new TFilterAnd();
      GetRepeatedFilterField(and)
        .Add(filterField);
      GetRepeatedFilterAnd(orNode)
        .Add(and);
    }

    return orNode;
  }

  private void Visit(Expression node,
                     bool       notOp = false)
  {
    if (node is MethodCallExpression call)
    {
      var hasParameterQualifier = call.Object!.IsLeftMostQualifierAParameter();
      var typeName              = call.Method.DeclaringType?.FullName ?? "";
      if (call.Method.Name == nameof(string.StartsWith) && typeName == "System.String" && hasParameterQualifier)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid filter: StartsWith method overload not supported.");
        }

        Visit(call.Object!);
        Visit(call.Arguments[0]);
        OnStringMethodOperator(call.Method);
        return;
      }

      if (call.Method.Name == nameof(string.EndsWith) && typeName == "System.String" && hasParameterQualifier)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid filter: EndsWith method overload not supported.");
        }

        Visit(call.Object!);
        Visit(call.Arguments[0]);
        OnStringMethodOperator(call.Method);
        return;
      }

      if (call.Method.Name == nameof(string.Contains))
      {
        if (typeName == "System.String" && hasParameterQualifier)
        {
          if (call.Arguments.Count != 1)
          {
            throw new InvalidExpressionException("Invalid filter: Contains method overload not supported.");
          }

          Visit(call.Object!);
          Visit(call.Arguments[0]);
          OnStringMethodOperator(call.Method,
                                 notOp);
          return;
        }

        if (typeName == "System.Linq.Enumerable")
        {
          if (call.Arguments.Count != 2)
          {
            throw new InvalidExpressionException("Invalid filter: Contains method overload not supported.");
          }

          Visit(call.Arguments[0]);
          Visit(call.Arguments[1]);
          OnByteArrayMethodOperator(call.Method,
                                    notOp);
          return;
        }
      }

      if (call.Method.Name == "get_Item")
      {
        Visit(call.Object!);
        Visit(call.Arguments[0]);
        OnIndexerAccess();
        return;
      }

      // Evaluate the method
      var result = call.EvaluateExpression();
      if (result != null)
      {
        FilterStack.Push((result, result.GetType()));
      }
    }
    else if (node is InvocationExpression invoke)
    {
      // Invocation of a delegate or lambda, let's evaluate it
      var result = invoke.EvaluateExpression();
      if (result != null)
      {
        FilterStack.Push((result, result.GetType()));
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
        var result = binary.EvaluateExpression();
        if (result != null)
        {
          FilterStack.Push((result, result.GetType()));
        }
      }
    }
    else if (node is UnaryExpression unary)
    {
      if (unary.NodeType == ExpressionType.Convert)
      {
        Visit(unary.Operand);
      }
      else if (unary.NodeType == ExpressionType.Not && LeftMostExpressionIsLambdaParameter(unary.Operand))
      {
        Visit(unary.Operand,
              !notOp);
      }
      else
      {
        var result = unary.EvaluateExpression();
        if (result != null)
        {
          FilterStack.Push((result, result.GetType()));
        }
      }
    }
  }

  private void OnPropertyMemberAccess(MemberExpression member)
  {
    if (member.IsLeftMostQualifierAParameter())
    {
      if (!PushProperty(member))
      {
        throw new InvalidOperationException("Unsupported filter expression on member " + member.Member.Name);
      }
    }
    else
    {
      var val = member.GetValueFromExpression();
      if (val != null)
      {
        FilterStack.Push((val, val.GetType()));
      }
      else
      {
        throw new InvalidOperationException("Unsupported filter expression on member " + member.Member.Name);
      }
    }
  }

  private void OnConstant(ConstantExpression constant)
    => FilterStack.Push((constant.Value, constant.Type));

  private bool OnFieldAccess(MemberExpression member)
  {
    var val = member.GetValueFromExpression();
    if (val != null)
    {
      FilterStack.Push((val, val.GetType()));
      return true;
    }

    return false;
  }

  private void OnBinaryOperator(ExpressionType expressionType)
  {
    var rhs     = FilterStack.Pop();
    var lhs     = FilterStack.Pop();
    var rhsType = rhs.Item2;
    var lhsType = lhs.Item2;
    FilterStack.Push(lhs);
    FilterStack.Push(rhs);
    if (rhsType == typeof(bool))
    {
      HandleBoolExpression(expressionType);
    }
    else if (rhsType == typeof(string))
    {
      HandleStringExpression(expressionType);
    }
    else if (rhsType == typeof(BlobStatus) || lhsType == typeof(BlobStatus) || rhsType == typeof(TaskStatus) || lhsType == typeof(TaskStatus))
    {
      HandleStatusExpression(expressionType);
    }
    else if (rhsType == typeof(int))
    {
      HandleIntegerExpression(expressionType);
    }
    else if (rhsType == typeof(DateTime))
    {
      HandleDateTimeExpression(expressionType);
    }
    else if (rhsType == typeof(TimeSpan))
    {
      HandleTimeSpanExpression(expressionType);
    }
    else
    {
      throw new InvalidOperationException("Invalid filter: Operands of expression type {rhsType.Name} are not supported.");
    }
  }

  private static Expression GetLeftMostExpression(BinaryExpression binaryExpression)
  {
    if (binaryExpression.Left is BinaryExpression binary)
    {
      return GetLeftMostExpression(binary);
    }

    return binaryExpression.Left;
  }

  private static MemberExpression GetRightMostMemberExpression(MemberExpression member)
  {
    if (member.Expression is MemberExpression rightExpression)
    {
      return GetRightMostMemberExpression(rightExpression);
    }

    return member;
  }

  private bool LeftMostExpressionIsLambdaParameter(Expression expression)
  {
    if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
    {
      expression = unary.Operand;
    }

    if (expression is BinaryExpression binary)
    {
      expression = GetLeftMostExpression(binary);
    }

    if (expression is MemberExpression member)
    {
      member = GetRightMostMemberExpression(member);
      return LeftMostExpressionIsLambdaParameter(member.Expression);
    }

    if (expression is MethodCallExpression call)
    {
      if (call.Method.DeclaringType?.FullName == "System.Linq.Enumerable")
      {
        // This is an extension method, <this> is then the first argument.
        return LeftMostExpressionIsLambdaParameter(call.Arguments[0]);
      }

      if (call.Object != null)
      {
        return LeftMostExpressionIsLambdaParameter(call.Object);
      }
    }

    return expression.NodeType == ExpressionType.Parameter;
  }

  protected abstract TFilterOr  CreateFilterOr(params  TFilterAnd[]   filters);
  protected abstract TFilterAnd CreateFilterAnd(params TFilterField[] filters);

  protected abstract RepeatedField<TFilterAnd> GetRepeatedFilterAnd(TFilterOr or);

  protected abstract RepeatedField<TFilterField> GetRepeatedFilterField(TFilterAnd or);

  private void HandleBoolExpression(ExpressionType type)
  {
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    switch (type)
    {
      case ExpressionType.OrElse:
        if (lhsFilter is TFilterOr lhsOrFilter)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <or expression> || <or expression>
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              GetRepeatedFilterAnd(lhsOrFilter)
                .Add(rhsAndFilter);
            }

            FilterStack.Push((lhsOrFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <or expression> || <and expression>
            GetRepeatedFilterAnd(lhsOrFilter)
              .Add(rhsAndFilter);
            FilterStack.Push((lhsOrFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <or expression> || <filter field>
            var andFilter = CreateFilterAnd(rhsFilterField);
            GetRepeatedFilterAnd(lhsOrFilter)
              .Add(andFilter);
            FilterStack.Push((lhsOrFilter, typeof(bool)));
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterAnd lhsAndFilter)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <and expression> || <or expression>
            var orFilter = CreateFilterOr(lhsAndFilter);
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              GetRepeatedFilterAnd(orFilter)
                .Add(rhsAndFilter);
            }

            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <and expression> || <and expression>
            var orFilter = CreateFilterOr(lhsAndFilter,
                                          rhsAndFilter);
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <and expression> || <filter field>
            var andRhsFilter = CreateFilterAnd(rhsFilterField);
            var orFilter = CreateFilterOr(lhsAndFilter,
                                          andRhsFilter);
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterField lhsFilterField)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <filter field> || <or expression>
            var andFilter = CreateFilterAnd(lhsFilterField);
            var orFilter  = CreateFilterOr(andFilter);
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              GetRepeatedFilterAnd(orFilter)
                .Add(rhsAndFilter);
            }

            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <filter field> || <and expression>
            var orFilter = CreateFilterOr(CreateFilterAnd(lhsFilterField),
                                          rhsAndFilter);
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <filter field> || <filter field>
            var orFilter = CreateFilterOr(CreateFilterAnd(lhsFilterField),
                                          CreateFilterAnd(rhsFilterField));
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }

        break;
      case ExpressionType.AndAlso:
        if (lhsFilter is TFilterOr lhsOrFilter2)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <or expression> && <or expression>
            var orFilter = new TFilterOr();
            foreach (var lhsAndFilter in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
              {
                var andFilter = CreateFilterAnd();
                GetRepeatedFilterField(andFilter)
                  .Add(GetRepeatedFilterField(lhsAndFilter));
                GetRepeatedFilterField(andFilter)
                  .Add(GetRepeatedFilterField(rhsAndFilter));
                GetRepeatedFilterAnd(orFilter)
                  .Add(andFilter);
              }
            }

            FilterStack.Push((orFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <or expression> && <and expression>
            var orFilter = new TFilterOr();
            foreach (var lhsAnd in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              var andFilter = new TFilterAnd();
              GetRepeatedFilterField(andFilter)
                .Add(GetRepeatedFilterField(lhsAnd));
              foreach (var rhsAnd in GetRepeatedFilterField(rhsAndFilter))
              {
                GetRepeatedFilterField(andFilter)
                  .Add(rhsAnd);
              }

              GetRepeatedFilterAnd(orFilter)
                .Add(andFilter);
            }

            FilterStack.Push((orFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <or expression> && <filter field>
            foreach (var and in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              GetRepeatedFilterField(and)
                .Add(rhsFilterField);
            }

            FilterStack.Push((lhsOrFilter2, typeof(bool)));
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterAnd lhsAndFilter)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <and expression> && <or expression>
            var orFilter = new TFilterOr();
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              var andFilter = new TFilterAnd();
              GetRepeatedFilterField(andFilter)
                .Add(GetRepeatedFilterField(lhsAndFilter));
              GetRepeatedFilterField(andFilter)
                .Add(GetRepeatedFilterField(rhsAndFilter));
              GetRepeatedFilterAnd(orFilter)
                .Add(andFilter);
            }

            FilterStack.Push((orFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <and expression> && <and expression>
            foreach (var rhsFilterField in GetRepeatedFilterField(rhsAndFilter))
            {
              GetRepeatedFilterField(lhsAndFilter)
                .Add(rhsFilterField);
            }

            FilterStack.Push((lhsAndFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <and expression> && <filter field>
            GetRepeatedFilterField(lhsAndFilter)
              .Add(rhsFilterField);
            FilterStack.Push((lhsAndFilter, typeof(bool)));
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterField lhsFilterField)
        {
          if (rhsFilter is TFilterField rhsFilterField)
          {
            // <filter field> && <filter field>
            var andFilter = CreateFilterAnd(lhsFilterField,
                                            rhsFilterField);
            FilterStack.Push((andFilter!, typeof(bool)));
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }

        break;
      default:
        throw new InvalidOperationException($"Internal error: operator '{type}' is not supported on operands of type bool.");
    }
  }

  protected abstract void HandleStringExpression(ExpressionType   type);
  protected abstract void HandleIntegerExpression(ExpressionType  type);
  protected abstract void HandleDateTimeExpression(ExpressionType type);
  protected abstract void HandleTimeSpanExpression(ExpressionType type);
  protected abstract void HandleStatusExpression(ExpressionType   type);

  protected abstract void OnStringMethodOperator(MethodInfo method,
                                                 bool       notOp = false);

  protected abstract void OnByteArrayMethodOperator(MethodInfo method,
                                                    bool       notOp = false);

  protected abstract void OnIndexerAccess();

  protected abstract bool PushProperty(MemberExpression member);
}
