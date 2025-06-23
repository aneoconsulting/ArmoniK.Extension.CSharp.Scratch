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
    ExpressionStack.Push(constant);
    FilterStack.Push(constant.Value);
  }

  protected override void OnPropertyMemberAccess(MemberExpression member)
  {
    Type memberType;
    if (memberName2Type_.TryGetValue(member.Member.Name,
                                     out memberType))
    {
      ExpressionTypeStack.Push(memberType);
      ExpressionStack.Push(member);
      FilterStack.Push(memberName2EnumField_[member.Member.Name]);
    }
    else
    {
      // Illegal Expression
      throw new InvalidOperationException("Invalid Blob filter expression on member " + member.Member.Name);
    }
  }

  protected override void OnFieldAccess(MemberExpression member)
  {
    if (member.Expression is ConstantExpression constant)
    {
      var closure = ((ConstantExpression)member.Expression).Value;
      var capturedValue = closure.GetType()
                                 .GetField(member.Member.Name)
                                 .GetValue(closure);
      ExpressionTypeStack.Push(capturedValue.GetType());
      ExpressionStack.Push(member);
      FilterStack.Push(capturedValue);
    }
  }

  protected override void OnBinaryOperator(ExpressionType expressionType)
  {
    var rhsType = ExpressionTypeStack.Pop();
    var lhsType = ExpressionTypeStack.Pop();

    if (rhsType != lhsType)
    {
      // Illegal Expression
      throw new InvalidOperationException($"Invalid Blob filter: expressionType mismatch between operands, left operand: {rhsType.Name}, right operand: {lhsType.Name}");
    }

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
  }

  private void HandleBoolExpression(ExpressionType type)
  {
    var rhsExpression = ExpressionStack.Pop();
    var lhsExpression = ExpressionStack.Pop();

    var rhsFilter = FilterStack.Pop();
    var lhsFilter = FilterStack.Pop();
    switch (type)
    {
      case ExpressionType.OrElse:
        if (lhsFilter is Filters lhsOrFilter)
        {
          if (rhsFilter is FilterField rhsFilterField)
          {
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
          if (rhsFilter is FilterField rhsFilterField)
          {
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
          if (rhsFilter is FilterField rhsFilterField)
          {
            var andLhsFilter = new FiltersAnd
                               {
                                 And =
                                 {
                                   lhsFilterField,
                                 },
                               };
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
                               andLhsFilter,
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

        break;
      case ExpressionType.AndAlso:
        if (lhsFilter is Filters lhsOrFilter2)
        {
          if (rhsFilter is FilterField filterField)
          {
          }
          else
          {
            throw new InvalidOperationException("Invalid Blob filter: internal error (FilterField expected on AND expression).");
          }
        }
        else if (lhsFilter is FiltersAnd lhsAndFilter)
        {
          if (rhsFilter is FilterField rhsFilterField)
          {
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
