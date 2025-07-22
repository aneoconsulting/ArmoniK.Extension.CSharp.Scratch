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
using System.Linq.Expressions;
using System.Reflection;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using Type = System.Type;

namespace ArmoniK.Extension.CSharp.Client.Queryable.BlobStateQuery;

/// <summary>
///   Specialisation of WhereExpressionTreeVisitor for queries on BlobState instances.
/// </summary>
internal class BlobStateWhereExpressionTreeVisitor : WhereExpressionTreeVisitor<Filters, FiltersAnd, FilterField>
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
                                                                          nameof(BlobInfo.CreatedBy), typeof(string)
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
                                                                        {
                                                                          nameof(BlobState.OwnerId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.OpaqueId), typeof(byte[])
                                                                        },
                                                                        {
                                                                          nameof(BlobState.Size), typeof(int)
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
                                                                                             nameof(BlobInfo.CreatedBy), ResultRawEnumField.CreatedBy
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
                                                                                           {
                                                                                             nameof(BlobState.OwnerId), ResultRawEnumField.OwnerTaskId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.OpaqueId), ResultRawEnumField.OpaqueId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.Size), ResultRawEnumField.Size
                                                                                           },
                                                                                         };

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

  protected override bool PushProperty(MemberExpression member)
  {
    if (memberName2Type_.TryGetValue(member.Member.Name,
                                     out var memberType) && memberName2EnumField_.TryGetValue(member.Member.Name,
                                                                                              out var enumField))
    {
      FilterStack.Push((enumField, memberType));
      return true;
    }

    return false;
  }

  protected override void OnIndexerAccess()
    => throw new InvalidExpressionException("Invalid filter expression.");

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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type string.");
    }

    PushStringFilter(filter);
  }

  private void PushStringFilter(FilterString filter)
  {
    var filterField = new FilterField();
    var fieldCount  = 0;
    var constCount  = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
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
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((filterField, typeof(bool)));
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type integer.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
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
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((filterField, typeof(bool)));
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type DateTime.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
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
      filter.Value = date.ToUniversalTime()
                         .ToTimestamp();
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
      filter.Value = date.ToUniversalTime()
                         .ToTimestamp();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((filterField, typeof(bool)));
  }

  protected override void HandleTimeSpanExpression(ExpressionType type)
    // Should never happen
    => throw new InvalidOperationException("Internal error: TimeSpan expression unexpected in a BlobState filter");

  protected override void HandleStatusExpression(ExpressionType type)
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type BlobStatus.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
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
    else if (lhsFilter is int statusInt)
    {
      // Left hand side is a constant
      filterField.FilterStatus = filter;
      filter.Value             = ((BlobStatus)statusInt).ToGrpcStatus();
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
    else if (rhsFilter is int statusInt)
    {
      // Right hand side is a constant
      filterField.FilterStatus = filter;
      filter.Value             = ((BlobStatus)statusInt).ToGrpcStatus();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((filterField, typeof(bool)));
  }

  protected override void OnStringMethodOperator(MethodInfo method,
                                                 bool       notOp = false)
  {
    var filter = new FilterString();
    switch (method.Name)
    {
      case nameof(string.StartsWith):
        filter.Operator = FilterStringOperator.StartsWith;
        PushStringFilter(filter);
        break;
      case nameof(string.EndsWith):
        filter.Operator = FilterStringOperator.EndsWith;
        PushStringFilter(filter);
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

        PushStringFilter(filter);
        break;
      default:
        throw new InvalidOperationException($"Method string.{method.Name} is not supported to filter blobs.");
    }
  }

  protected override void OnByteArrayMethodOperator(MethodInfo method,
                                                    bool       notOp = false)
  {
    var filter = new FilterArray();
    switch (method.Name)
    {
      case nameof(string.Contains):
        if (notOp)
        {
          filter.Operator = FilterArrayOperator.NotContains;
        }
        else
        {
          filter.Operator = FilterArrayOperator.Contains;
        }

        PushByteArrayFilter(filter);
        break;
      default:
        throw new InvalidOperationException($"Method byte[].{method.Name} is not supported to filter blobs.");
    }
  }

  private void PushByteArrayFilter(FilterArray filter)
  {
    var filterField = new FilterField();
    var fieldCount  = 0;
    var constCount  = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
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
    else if (lhsFilter is byte value)
    {
      // Left hand side is a constant
      filterField.FilterArray = filter;
      filter.Value            = value.ToString();
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
    else if (rhsFilter is byte value)
    {
      // Right hand side is a constant
      filterField.FilterArray = filter;
      filter.Value            = value.ToString();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((filterField, typeof(bool)));
  }
}
