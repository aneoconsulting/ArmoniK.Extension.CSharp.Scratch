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

using Google.Protobuf.Collections;

namespace ArmoniK.Extension.CSharp.Client.Filtering;

internal abstract class FilterExpressionTreeVisitor<TEnumField, TFilterOr, TFilterAnd, TFilterField>
  where TFilterOr : new()
  where TFilterAnd : new()
{
  protected readonly Stack<Type>   ExpressionTypeStack = new();
  protected readonly Stack<object> FilterStack         = new();

  public FilterExpressionTreeVisitor(Expression tree)
    => Tree = tree;

  public Expression Tree { get; init; }

  public TFilterOr Filters { get; private set; }

  public void Visit()
  {
    if (Tree is MethodCallExpression call)
    {
      var memberAccess = call.Object as MemberExpression;
      var typeName     = call.Method.DeclaringType?.FullName ?? "";
      if (call.Method.Name == nameof(Queryable.Where) && typeName == "System.Linq.Queryable")
      {
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        Visit(lambda.Body);
      }
    }

    var filter = FilterStack.Pop();
    if (!FilterStack.Any())
    {
      if (filter is TFilterOr orNode)
      {
        Filters = orNode;
      }
      else if (filter is TFilterAnd andNode)
      {
        Filters = new TFilterOr();
        GetRepeatedFilterAnd(Filters)
          .Add(andNode);
      }
      else if (filter is TFilterField filterField)
      {
        Filters = new TFilterOr();
        var and = new TFilterAnd();
        GetRepeatedFilterField(and)
          .Add(filterField);
        GetRepeatedFilterAnd(Filters)
          .Add(and);
      }
      else
      {
        throw new InvalidOperationException("Internal error: analysis stack in an inconsistent state");
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
      if (call.Method.Name == nameof(string.StartsWith) && typeName == "System.String" && memberAccess?.Expression.NodeType == ExpressionType.Parameter)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("StartsWith method overload not supported.");
        }

        Visit(call.Object);
        Visit(call.Arguments[0]);
        OnMethodOperator(call.Method);
      }
      else if (call.Method.Name == nameof(string.EndsWith) && typeName == "System.String" && memberAccess?.Expression.NodeType == ExpressionType.Parameter)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("EndsWith method overload not supported.");
        }

        Visit(call.Object);
        Visit(call.Arguments[0]);
        OnMethodOperator(call.Method);
      }
      else if (call.Method.Name == nameof(string.Contains) && typeName == "System.String" && memberAccess?.Expression.NodeType == ExpressionType.Parameter)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Contains method overload not supported.");
        }

        Visit(call.Object);
        Visit(call.Arguments[0]);
        OnMethodOperator(call.Method,
                         notOp);
      }
      else
      {
        // Evaluate the method
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
      if (unary.NodeType == ExpressionType.Convert)
      {
        Visit(unary.Operand);
        ExpressionTypeStack.Pop();
        ExpressionTypeStack.Push(unary.Operand.Type);
      }
      else if (unary.NodeType == ExpressionType.Not && LeftMostExpressionIsLambdaParameter(unary.Operand))
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
      throw new InvalidExpressionException("could not evaluate method call " + lambda,
                                           ex);
    }
  }

  private static object GetValueFromExpression(Expression expression)
  {
    switch (expression)
    {
      case MemberExpression memberExpr:
        object instance = null;

        if (memberExpr.Expression is ConstantExpression constant)
        {
          instance = constant.Value;
        }
        else if (memberExpr.Expression != null)
        {
          instance = GetValueFromExpression(memberExpr.Expression);
          if (instance == null)
          {
            return null;
          }
        }

        if (memberExpr.Member is PropertyInfo propInfo)
        {
          return propInfo.GetValue(instance);
        }

        if (memberExpr.Member is FieldInfo fieldInfo)
        {
          return fieldInfo.GetValue(instance);
        }

        break;

      case ConstantExpression constExpr:
        return constExpr.Value;
    }

    return null;
  }


  private void OnPropertyMemberAccess(MemberExpression member)
  {
    Type memberType;

    var val = GetValueFromExpression(member);
    if (val != null)
    {
      ExpressionTypeStack.Push(val.GetType());
      FilterStack.Push(val);
    }
    else
    {
      TEnumField enumField;
      if (TryGetFieldTypeFromName(member.Member.Name,
                                  out memberType) && TryGetEnumFieldFromName(member.Member.Name,
                                                                             out enumField))
      {
        ExpressionTypeStack.Push(memberType);
        FilterStack.Push(enumField);
      }
      else
      {
        throw new InvalidOperationException("Illegal expression on member " + member.Member.Name);
      }
    }
  }

  private void OnConstant(ConstantExpression constant)
  {
    ExpressionTypeStack.Push(constant.Type);
    FilterStack.Push(constant.Value);
  }

  private bool OnFieldAccess(MemberExpression member)
  {
    if (member.Expression is ConstantExpression constant)
    {
      var closure = constant.Value;
      var capturedValue = closure.GetType()
                                 .GetField(member.Member.Name)
                                 .GetValue(closure);
      ExpressionTypeStack.Push(capturedValue.GetType());
      FilterStack.Push(capturedValue);
      return true;
    }

    return false;
  }

  private void OnBinaryOperator(ExpressionType expressionType)
  {
    var rhsType = ExpressionTypeStack.Pop();
    var lhsType = ExpressionTypeStack.Pop();

    if (rhsType == typeof(bool))
    {
      HandleBoolExpression(expressionType);
    }
    else if (rhsType == typeof(string))
    {
      HandleStringExpression(expressionType);
    }
    else if (rhsType == typeof(BlobStatus) || lhsType == typeof(BlobStatus))
    {
      HandleBlobStatusExpression(expressionType);
    }
    else if (rhsType == typeof(int))
    {
      HandleIntegerExpression(expressionType);
    }
    else if (rhsType == typeof(DateTime))
    {
      HandleDateTimeExpression(expressionType);
    }
    else
    {
      throw new InvalidOperationException("Operands of expression type {rhsType.Name} are not supported.");
    }

    ExpressionTypeStack.Push(typeof(bool));
  }

  private bool LeftMostExpressionIsLambdaParameter(Expression expression)
  {
    if (expression == null)
    {
      return false;
    }

    if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
    {
      expression = unary.Operand;
    }

    if (expression is BinaryExpression binary)
    {
      while (binary != null)
      {
        expression = binary.Left;
        binary     = expression as BinaryExpression;
      }
    }

    if (expression is MemberExpression member)
    {
      while (member != null)
      {
        expression = member.Expression;
        member     = expression as MemberExpression;
      }

      return LeftMostExpressionIsLambdaParameter(expression);
    }

    if (expression is MethodCallExpression call)
    {
      return LeftMostExpressionIsLambdaParameter(call.Object);
    }

    return expression.NodeType == ExpressionType.Parameter;
  }

  protected abstract TFilterOr  CreateFilterOr(params  TFilterAnd[]   filters);
  protected abstract TFilterAnd CreateFilterAnd(params TFilterField[] filters);

  protected abstract RepeatedField<TFilterAnd> GetRepeatedFilterAnd(TFilterOr or);

  protected abstract RepeatedField<TFilterField> GetRepeatedFilterField(TFilterAnd or);

  private void HandleBoolExpression(ExpressionType type)
  {
    var rhsFilter = FilterStack.Pop();
    var lhsFilter = FilterStack.Pop();
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

            FilterStack.Push(lhsOrFilter);
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <or expression> || <and expression>
            GetRepeatedFilterAnd(lhsOrFilter)
              .Add(rhsAndFilter);
            FilterStack.Push(lhsOrFilter);
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <or expression> || <filter field>
            var andFilter = CreateFilterAnd(rhsFilterField);
            GetRepeatedFilterAnd(lhsOrFilter)
              .Add(andFilter);
            FilterStack.Push(lhsOrFilter);
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

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <and expression> || <and expression>
            var orFilter = CreateFilterOr(lhsAndFilter,
                                          rhsAndFilter);
            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <and expression> || <filter field>
            var andRhsFilter = CreateFilterAnd(rhsFilterField);
            var orFilter = CreateFilterOr(lhsAndFilter,
                                          andRhsFilter);
            FilterStack.Push(orFilter);
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

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <filter field> || <and expression>
            var orFilter = CreateFilterOr(CreateFilterAnd(lhsFilterField),
                                          rhsAndFilter);
            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <filter field> || <filter field>
            var orFilter = CreateFilterOr(CreateFilterAnd(lhsFilterField),
                                          CreateFilterAnd(rhsFilterField));
            FilterStack.Push(orFilter);
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

            FilterStack.Push(orFilter);
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

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <or expression> && <filter field>
            foreach (var and in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              GetRepeatedFilterField(and)
                .Add(rhsFilterField);
            }

            FilterStack.Push(lhsOrFilter2);
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

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <and expression> && <and expression>
            foreach (var rhsFilterField in GetRepeatedFilterField(rhsAndFilter))
            {
              GetRepeatedFilterField(lhsAndFilter)
                .Add(rhsFilterField);
            }

            FilterStack.Push(lhsAndFilter);
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <and expression> && <filter field>
            GetRepeatedFilterField(lhsAndFilter)
              .Add(rhsFilterField);
            FilterStack.Push(lhsAndFilter);
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
            FilterStack.Push(andFilter);
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

  protected abstract void HandleStringExpression(ExpressionType     type);
  protected abstract void HandleIntegerExpression(ExpressionType    type);
  protected abstract void HandleDateTimeExpression(ExpressionType   type);
  protected abstract void HandleBlobStatusExpression(ExpressionType type);

  protected abstract void OnMethodOperator(MethodInfo method,
                                           bool       notOp = false);

  protected abstract bool TryGetEnumFieldFromName(string         name,
                                                  out TEnumField enumField);

  protected abstract bool TryGetFieldTypeFromName(string   name,
                                                  out Type type);
}
