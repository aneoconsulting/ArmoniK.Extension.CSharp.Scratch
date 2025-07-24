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
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

/// <summary>
///   Class containing helper extension methods to work on Expressions Trees
/// </summary>
internal static class ExpressionTreeExt
{
  /// <summary>
  ///   Checks that the leftmost element of a qualified expression is a parameter
  ///   of the Expression Tree lambda.
  /// </summary>
  /// <param name="expression"></param>
  /// <returns></returns>
  public static bool IsLeftMostQualifierAParameter(this Expression expression)
  {
    switch (expression)
    {
      case MemberExpression member:
        if (member.Expression is MemberExpression leftMember1)
        {
          return IsLeftMostQualifierAParameter(leftMember1);
        }

        return member.Expression is ParameterExpression;
      case MethodCallExpression call:
        if (call.Object is MemberExpression leftMember2)
        {
          return IsLeftMostQualifierAParameter(leftMember2);
        }

        return call.Object is ParameterExpression;
      default:
        return false;
    }
  }

  /// <summary>
  ///   Evaluates an Expression Tree by compiling and invoking it.
  /// </summary>
  /// <param name="expr">The expression</param>
  /// <returns>The result of the evaluation</returns>
  /// <exception cref="InvalidExpressionException">
  ///   When something went wrong during the evaluation,
  ///   either during the compilation or the execution.
  /// </exception>
  public static object? EvaluateExpression(this Expression expr)
  {
    Expression<Func<object>> lambda = null;
    try
    {
      var objectExpr = Expression.Convert(expr,
                                          typeof(object));
      lambda = Expression.Lambda<Func<object>>(objectExpr);
      var result = lambda.Compile()();
      return result;
    }
    catch (Exception ex)
    {
      throw new InvalidExpressionException("Invalid filter: could not evaluate expression " + lambda,
                                           ex);
    }
  }

  /// <summary>
  ///   Evaluates an Expression Tree that does not need compilation.
  /// </summary>
  /// <param name="expression">The expression</param>
  /// <returns>The value described by the expression.</returns>
  public static object? GetValueFromExpression(this Expression expression)
  {
    switch (expression)
    {
      case MemberExpression memberExpr:
        object? instance = null;

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
}
