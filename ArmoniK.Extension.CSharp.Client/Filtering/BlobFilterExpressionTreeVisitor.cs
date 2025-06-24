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
using System.Linq.Expressions;
using System.Reflection;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

using Google.Protobuf.WellKnownTypes;

using Type = System.Type;

namespace ArmoniK.Extension.CSharp.Client.Filtering;

internal class BlobFilterExpressionTreeVisitor : FilterExpressionTreeVisitor
{
  private static readonly Dictionary<string, Type> memberName2Type_ = new()
                                                                      {
                                                                        {
                                                                          nameof(BlobInfo.SessionId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobInfo.BlobId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobInfo.BlobName), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.CompletedAt), typeof(DateTime?)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.CreateAt), typeof(DateTime)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.Status), typeof(BlobStatus)
                                                                        },
                                                                      };

  private static readonly Dictionary<string, ResultRawEnumField> memberName2EnumField_ = new()
                                                                                         {
                                                                                           {
                                                                                             nameof(BlobInfo.SessionId), ResultRawEnumField.SessionId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.BlobId), ResultRawEnumField.ResultId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.BlobName), ResultRawEnumField.Name
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.CompletedAt), ResultRawEnumField.CompletedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.CreateAt), ResultRawEnumField.CreatedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.Status), ResultRawEnumField.Status
                                                                                           },
                                                                                         };

  public BlobFilterExpressionTreeVisitor(Expression tree)
    : base(tree)
  {
  }

  protected override void OnConstant(ConstantExpression constant)
  {
    ExpressionTypeStack.Push(constant.Type);
    FilterStack.Push(constant.Value);
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

  protected override void OnPropertyMemberAccess(MemberExpression member)
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
      if (memberName2Type_.TryGetValue(member.Member.Name,
                                       out memberType))
      {
        ExpressionTypeStack.Push(memberType);
        FilterStack.Push(memberName2EnumField_[member.Member.Name]);
      }
      else
      {
        // Illegal Expression
        throw new InvalidOperationException("Invalid Blob filter expression on member " + member.Member.Name);
      }
    }
  }

  protected override bool OnFieldAccess(MemberExpression member)
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

  protected override void OnBinaryOperator(ExpressionType expressionType)
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
    else if (rhsType == typeof(int))
    {
      HandleIntegerExpression(expressionType);
    }
    else if (rhsType == typeof(DateTime))
    {
      HandleDateTimeExpression(expressionType);
    }
    else if (rhsType == typeof(BlobStatus))
    {
      HandleBlobStatusExpression(expressionType);
    }
    else
    {
      throw new InvalidOperationException($"Invalid Blob filter: operands of expressionType {rhsType.Name} are not supported.");
    }

    ExpressionTypeStack.Push(typeof(bool));
  }

  private void HandleBoolExpression(ExpressionType type)
  {
    var rhsFilter = FilterStack.Pop();
    var lhsFilter = FilterStack.Pop();
    switch (type)
    {
      case ExpressionType.OrElse:
        if (lhsFilter is Filters lhsOrFilter)
        {
          if (rhsFilter is Filters rhsOrFilter)
          {
            // <or expression> || <or expression>
            foreach (var rhsAndFilter in rhsOrFilter.Or)
            {
              lhsOrFilter.Or.Add(rhsAndFilter);
            }

            FilterStack.Push(lhsOrFilter);
          }
          else if (rhsFilter is FiltersAnd rhsAndFilter)
          {
            // <or expression> || <and expression>
            lhsOrFilter.Or.Add(rhsAndFilter);
            FilterStack.Push(lhsOrFilter);
          }
          else if (rhsFilter is FilterField rhsFilterField)
          {
            // <or expression> || <filter field>
            var andFilter = new FiltersAnd
                            {
                              And =
                              {
                                rhsFilterField,
                              },
                            };
            lhsOrFilter.Or.Add(andFilter);
            FilterStack.Push(lhsOrFilter);
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on OR expression).");
          }
        }
        else if (lhsFilter is FiltersAnd lhsAndFilter)
        {
          if (rhsFilter is Filters rhsOrFilter)
          {
            // <and expression> || <or expression>
            var orFilter = new Filters
                           {
                             Or =
                             {
                               lhsAndFilter,
                             },
                           };
            foreach (var rhsAndFilter in rhsOrFilter.Or)
            {
              orFilter.Or.Add(rhsAndFilter);
            }

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FiltersAnd rhsAndFilter)
          {
            // <and expression> || <and expression>
            var orFilter = new Filters
                           {
                             Or =
                             {
                               lhsAndFilter,
                               rhsAndFilter,
                             },
                           };
            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FilterField rhsFilterField)
          {
            // <and expression> || <filter field>
            var andRhsFilter = new FiltersAnd
                               {
                                 And =
                                 {
                                   rhsFilterField,
                                 },
                               };
            var orFilter = new Filters
                           {
                             Or =
                             {
                               lhsAndFilter,
                               andRhsFilter,
                             },
                           };
            FilterStack.Push(orFilter);
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on OR expression).");
          }
        }
        else if (lhsFilter is FilterField lhsFilterField)
        {
          if (rhsFilter is Filters rhsOrFilter)
          {
            // <filter field> || <or expression>
            var andFilter = new FiltersAnd
                            {
                              And =
                              {
                                lhsFilterField,
                              },
                            };
            var orFilter = new Filters
                           {
                             Or =
                             {
                               andFilter,
                             },
                           };
            foreach (var rhsAndFilter in rhsOrFilter.Or)
            {
              orFilter.Or.Add(rhsAndFilter);
            }

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FiltersAnd rhsAndFilter)
          {
            // <filter field> || <and expression>
            var orFilter = new Filters
                           {
                             Or =
                             {
                               new FiltersAnd
                               {
                                 And =
                                 {
                                   lhsFilterField,
                                 },
                               },
                               rhsAndFilter,
                             },
                           };
            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FilterField rhsFilterField)
          {
            // <filter field> || <filter field>
            var orFilter = new Filters
                           {
                             Or =
                             {
                               new FiltersAnd
                               {
                                 And =
                                 {
                                   lhsFilterField,
                                 },
                               },
                               new FiltersAnd
                               {
                                 And =
                                 {
                                   rhsFilterField,
                                 },
                               },
                             },
                           };
            FilterStack.Push(orFilter);
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on OR expression).");
          }
        }

        break;
      case ExpressionType.AndAlso:
        if (lhsFilter is Filters lhsOrFilter2)
        {
          if (rhsFilter is Filters rhsOrFilter)
          {
            // <or expression> && <or expression>
            var orFilter = new Filters();
            foreach (var lhsAndFilter in lhsOrFilter2.Or)
            {
              foreach (var rhsAndFilter in rhsOrFilter.Or)
              {
                var andFilter = new FiltersAnd();
                andFilter.And.Add(lhsAndFilter.And);
                andFilter.And.Add(rhsAndFilter.And);
                orFilter.Or.Add(andFilter);
              }
            }

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FiltersAnd rhsAndFilter)
          {
            // <or expression> && <and expression>
            var orFilter = new Filters();
            foreach (var lhsAnd in lhsOrFilter2.Or)
            {
              var andFilter = new FiltersAnd();
              andFilter.And.Add(lhsAnd.And);
              foreach (var rhsAnd in rhsAndFilter.And)
              {
                andFilter.And.Add(rhsAnd);
              }

              orFilter.Or.Add(andFilter);
            }

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FilterField rhsFilterField)
          {
            // <or expression> && <filter field>
            foreach (var and in lhsOrFilter2.Or)
            {
              and.And.Add(rhsFilterField);
            }

            FilterStack.Push(lhsOrFilter2);
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on AND expression).");
          }
        }
        else if (lhsFilter is FiltersAnd lhsAndFilter)
        {
          if (rhsFilter is Filters rhsOrFilter)
          {
            // <and expression> && <or expression>
            var orFilter = new Filters();
            foreach (var rhsAndFilter in rhsOrFilter.Or)
            {
              var andFilter = new FiltersAnd();
              andFilter.And.Add(lhsAndFilter.And);
              andFilter.And.Add(rhsAndFilter.And);
              orFilter.Or.Add(andFilter);
            }

            FilterStack.Push(orFilter);
          }
          else if (rhsFilter is FiltersAnd rhsAndFilter)
          {
            // <and expression> && <and expression>
            foreach (var rhsFilterField in rhsAndFilter.And)
            {
              lhsAndFilter.And.Add(rhsFilterField);
            }

            FilterStack.Push(lhsAndFilter);
          }
          else if (rhsFilter is FilterField rhsFilterField)
          {
            // <and expression> && <filter field>
            lhsAndFilter.And.Add(rhsFilterField);
            FilterStack.Push(lhsAndFilter);
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on AND expression).");
          }
        }
        else if (lhsFilter is FilterField lhsFilterField)
        {
          if (rhsFilter is FilterField rhsFilterField)
          {
            // <filter field> && <filter field>
            var andFilter = new FiltersAnd
                            {
                              And =
                              {
                                lhsFilterField,
                                rhsFilterField,
                              },
                            };
            FilterStack.Push(andFilter);
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on AND expression).");
          }
        }

        break;
      default:
        throw new InvalidOperationException($"Invalid Blob filter: operator '{type}' is not supported on operands of type bool.");
    }
  }

  private void HandleStringExpression(ExpressionType type)
  {
    var filterField = new FilterField();
    var filter      = new FilterString();
    switch (type)
    {
      case ExpressionType.Equal:
        filter.Operator = FilterStringOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        filter.Operator = FilterStringOperator.NotEqual;
        break;
      default:
        throw new InvalidOperationException($"Invalid Blob filter: operator '{type}' is not supported on operands of type string.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var rhsFilter  = FilterStack.Pop();
    var lhsFilter  = FilterStack.Pop();
    if (lhsFilter is ResultRawEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = lhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (lhsFilter is string str)
    {
      // Left hand side is a constant
      filterField.FilterString = filter;
      filter.Value             = str;
      constCount++;
    }

    if (rhsFilter is ResultRawEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = rhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (rhsFilter is string str)
    {
      // Right hand side is a constant
      filterField.FilterString = filter;
      filter.Value             = str;
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid Blob filter: a filter expression is expected to be of the form <property> <operator> <expression>.");
    }

    FilterStack.Push(filterField);
  }

  private void HandleIntegerExpression(ExpressionType type)
  {
    var filterField = new FilterField();
    var filter      = new FilterNumber();
    switch (type)
    {
      case ExpressionType.Equal:
        filter.Operator = FilterNumberOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        filter.Operator = FilterNumberOperator.NotEqual;
        break;
      case ExpressionType.LessThan:
        filter.Operator = FilterNumberOperator.LessThan;
        break;
      case ExpressionType.LessThanOrEqual:
        filter.Operator = FilterNumberOperator.LessThanOrEqual;
        break;
      case ExpressionType.GreaterThan:
        filter.Operator = FilterNumberOperator.GreaterThan;
        break;
      case ExpressionType.GreaterThanOrEqual:
        filter.Operator = FilterNumberOperator.GreaterThanOrEqual;
        break;
      default:
        throw new InvalidOperationException($"Invalid Blob filter: operator '{type}' is not supported on operands of type int.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var rhsFilter  = FilterStack.Pop();
    var lhsFilter  = FilterStack.Pop();
    if (lhsFilter is ResultRawEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = lhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (lhsFilter is int number)
    {
      // Left hand side is a constant
      filterField.FilterNumber = filter;
      filter.Value             = number;
      constCount++;
    }

    if (rhsFilter is ResultRawEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = rhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (rhsFilter is int number)
    {
      // Right hand side is a constant
      filterField.FilterNumber = filter;
      filter.Value             = number;
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid Blob filter: a filter expression is expected to be of the form <property> <operator> <expression>.");
    }

    FilterStack.Push(filterField);
  }

  private void HandleDateTimeExpression(ExpressionType type)
  {
    var filterField = new FilterField();
    var filter      = new FilterDate();
    switch (type)
    {
      case ExpressionType.Equal:
        filter.Operator = FilterDateOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        filter.Operator = FilterDateOperator.NotEqual;
        break;
      case ExpressionType.LessThan:
        filter.Operator = FilterDateOperator.Before;
        break;
      case ExpressionType.LessThanOrEqual:
        filter.Operator = FilterDateOperator.BeforeOrEqual;
        break;
      case ExpressionType.GreaterThan:
        filter.Operator = FilterDateOperator.After;
        break;
      case ExpressionType.GreaterThanOrEqual:
        filter.Operator = FilterDateOperator.AfterOrEqual;
        break;
      default:
        throw new InvalidOperationException($"Invalid Blob filter: operator '{type}' is not supported on operands of type DateTime.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var rhsFilter  = FilterStack.Pop();
    var lhsFilter  = FilterStack.Pop();
    if (lhsFilter is ResultRawEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = lhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (lhsFilter is DateTime date)
    {
      // Left hand side is a constant
      filterField.FilterDate = filter;
      filter.Value           = date.ToTimestamp();
      constCount++;
    }

    if (rhsFilter is ResultRawEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = rhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (rhsFilter is DateTime date)
    {
      // Right hand side is a constant
      filterField.FilterDate = filter;
      filter.Value           = date.ToTimestamp();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid Blob filter: a filter expression is expected to be of the form <property> <operator> <expression>.");
    }

    FilterStack.Push(filterField);
  }

  private void HandleBlobStatusExpression(ExpressionType type)
  {
    var filterField = new FilterField();
    var filter      = new FilterStatus();
    switch (type)
    {
      case ExpressionType.Equal:
        filter.Operator = FilterStatusOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        filter.Operator = FilterStatusOperator.NotEqual;
        break;
      default:
        throw new InvalidOperationException($"Invalid Blob filter: operator '{type}' is not supported on operands of type BlobStatus.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var rhsFilter  = FilterStack.Pop();
    var lhsFilter  = FilterStack.Pop();
    if (lhsFilter is ResultRawEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = lhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (lhsFilter is BlobStatus status)
    {
      // Left hand side is a constant
      filterField.FilterStatus = filter;
      filter.Value             = status.ToGrpcStatus();
      constCount++;
    }

    if (rhsFilter is ResultRawEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = rhsFilterField,
                                             },
                          };
      fieldCount++;
    }
    else if (rhsFilter is BlobStatus status)
    {
      // Right hand side is a constant
      filterField.FilterStatus = filter;
      filter.Value             = status.ToGrpcStatus();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid Blob filter: a filter expression is expected to be of the form <property> <operator> <expression>.");
    }

    FilterStack.Push(filterField);
  }
}
