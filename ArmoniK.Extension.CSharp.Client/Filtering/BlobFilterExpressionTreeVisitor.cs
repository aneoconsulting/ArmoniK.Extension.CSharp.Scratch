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

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using Type = System.Type;

namespace ArmoniK.Extension.CSharp.Client.Filtering;

internal class BlobFilterExpressionTreeVisitor : FilterExpressionTreeVisitor<ResultRawEnumField, Filters, FiltersAnd, FilterField>
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

  protected override Filters CreateFilterOr(params FiltersAnd[] filters)
  {
    var orFilter = new Filters();
    orFilter.Or.Add(filters);
    return orFilter;
  }

  protected override FiltersAnd CreateFilterAnd(params FilterField[] filters)
  {
    var andFilter = new FiltersAnd();
    andFilter.And.Add(filters);
    return andFilter;
  }

  protected override RepeatedField<FiltersAnd> GetRepeatedFilterAnd(Filters or)
    => or.Or;

  protected override RepeatedField<FilterField> GetRepeatedFilterField(FiltersAnd and)
    => and.And;

  protected override bool TryGetEnumFieldFromName(string                 name,
                                                  out ResultRawEnumField enumField)
    => memberName2EnumField_.TryGetValue(name,
                                         out enumField);

  protected override bool TryGetFieldTypeFromName(string   name,
                                                  out Type type)
    => memberName2Type_.TryGetValue(name,
                                    out type);


  protected override void HandleStringExpression(ExpressionType type)
  {
    var filter = new FilterString();
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

    PushFilter(filter);
  }

  private void PushFilter(FilterString filter)
  {
    var filterField = new FilterField();
    var fieldCount  = 0;
    var constCount  = 0;
    var rhsFilter   = FilterStack.Pop();
    var lhsFilter   = FilterStack.Pop();
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
    else if (lhsFilter is char car)
    {
      // Left hand side is a constant
      filterField.FilterString = filter;
      filter.Value             = car.ToString();
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
    else if (rhsFilter is char car)
    {
      // Right hand side is a constant
      filterField.FilterString = filter;
      filter.Value             = car.ToString();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid Blob filter: a filter expression is expected to be of the form <property> <operator> <expression>.");
    }

    FilterStack.Push(filterField);
  }

  protected override void HandleIntegerExpression(ExpressionType type)
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

  protected override void HandleDateTimeExpression(ExpressionType type)
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

  protected override void HandleBlobStatusExpression(ExpressionType type)
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

  protected override void OnMethodOperator(MethodInfo method,
                                           bool       notOp = false)
  {
    var filter = new FilterString();
    switch (method.Name)
    {
      case nameof(string.StartsWith):
        filter.Operator = FilterStringOperator.StartsWith;
        PushFilter(filter);
        break;
      case nameof(string.EndsWith):
        filter.Operator = FilterStringOperator.EndsWith;
        PushFilter(filter);
        break;
      case nameof(string.Contains):
        if (notOp)
        {
          filter.Operator = FilterStringOperator.NotContains;
        }
        else
        {
          filter.Operator = FilterStringOperator.Contains;
        }

        PushFilter(filter);
        break;
    }
  }
}
