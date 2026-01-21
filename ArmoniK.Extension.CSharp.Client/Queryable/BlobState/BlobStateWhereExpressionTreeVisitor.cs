// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using Type = System.Type;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

internal class BlobStateWhereExpressionTreeVisitor : WhereExpressionTreeVisitor<ResultRawEnumField, Filters, FiltersAnd, FilterField>
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
                                                                          nameof(CSharp.Common.Common.Domain.Blob.BlobState.CompletedAt), typeof(DateTime?)
                                                                        },
                                                                        {
                                                                          nameof(CSharp.Common.Common.Domain.Blob.BlobState.CreateAt), typeof(DateTime)
                                                                        },
                                                                        {
                                                                          nameof(CSharp.Common.Common.Domain.Blob.BlobState.Status), typeof(BlobStatus)
                                                                        },
                                                                        {
                                                                          nameof(CSharp.Common.Common.Domain.Blob.BlobState.OwnerId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(CSharp.Common.Common.Domain.Blob.BlobState.OpaqueId), typeof(byte[])
                                                                        },
                                                                        {
                                                                          nameof(CSharp.Common.Common.Domain.Blob.BlobState.Size), typeof(int)
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
                                                                                             nameof(CSharp.Common.Common.Domain.Blob.BlobState.CompletedAt),
                                                                                             ResultRawEnumField.CompletedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(CSharp.Common.Common.Domain.Blob.BlobState.CreateAt),
                                                                                             ResultRawEnumField.CreatedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(CSharp.Common.Common.Domain.Blob.BlobState.Status),
                                                                                             ResultRawEnumField.Status
                                                                                           },
                                                                                           {
                                                                                             nameof(CSharp.Common.Common.Domain.Blob.BlobState.OwnerId),
                                                                                             ResultRawEnumField.OwnerTaskId
                                                                                           },
                                                                                           {
                                                                                             nameof(CSharp.Common.Common.Domain.Blob.BlobState.OpaqueId),
                                                                                             ResultRawEnumField.OpaqueId
                                                                                           },
                                                                                           {
                                                                                             nameof(CSharp.Common.Common.Domain.Blob.BlobState.Size),
                                                                                             ResultRawEnumField.Size
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type string.");
    }

    PushStringFilter(filter);
  }

  private void PushStringFilter(FilterString filter)
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
      throw new InvalidOperationException("Invalid filter expression.");
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type integer.");
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
      throw new InvalidOperationException("Invalid filter expression.");
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type DateTime.");
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
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type BlobStatus.");
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

    FilterStack.Push(filterField);
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

  protected override void OnCollectionContains(ResultRawEnumField enumField,
                                               object             collection,
                                               bool               notOp = false)
  {
    var isEmpty = true;
    var orNode  = new Filters();
    if (notOp)
    {
      orNode.Or.Add(new FiltersAnd());
    }

    switch (enumField)
    {
      case ResultRawEnumField.OpaqueId:
        foreach (var val in (IEnumerable<byte[]>)collection)
        {
          isEmpty = false;
          var resultField = new ResultField
                            {
                              ResultRawField = new ResultRawField
                                               {
                                                 Field = enumField,
                                               },
                            };

          if (notOp)
          {
            orNode.Or[0]
                  .And.Add(new FilterField
                           {
                             Field = resultField,
                             FilterArray = new FilterArray
                                           {
                                             Operator = FilterArrayOperator.NotContains,
                                             Value    = Convert.ToBase64String(val),
                                           },
                           });
          }
          else
          {
            orNode.Or.Add(new FiltersAnd
                          {
                            And =
                            {
                              new FilterField
                              {
                                Field = resultField,
                                FilterArray = new FilterArray
                                              {
                                                Operator = FilterArrayOperator.Contains,
                                                Value    = Convert.ToBase64String(val),
                                              },
                              },
                            },
                          });
          }
        }

        break;

      case ResultRawEnumField.CreatedBy:
      case ResultRawEnumField.Name:
      case ResultRawEnumField.OwnerTaskId:
      case ResultRawEnumField.ResultId:
      case ResultRawEnumField.SessionId:
        foreach (var val in (IEnumerable<string>)collection)
        {
          isEmpty = false;
          var resultField = new ResultField
                            {
                              ResultRawField = new ResultRawField
                                               {
                                                 Field = enumField,
                                               },
                            };
          if (notOp)
          {
            orNode.Or[0]
                  .And.Add(new FilterField
                           {
                             Field = resultField,
                             FilterString = new FilterString
                                            {
                                              Operator = FilterStringOperator.NotEqual,
                                              Value    = val,
                                            },
                           });
          }
          else
          {
            orNode.Or.Add(new FiltersAnd
                          {
                            And =
                            {
                              new FilterField
                              {
                                Field = resultField,
                                FilterString = new FilterString
                                               {
                                                 Operator = FilterStringOperator.Equal,
                                                 Value    = val,
                                               },
                              },
                            },
                          });
          }
        }

        break;
      default:
        throw new InvalidOperationException($"Cannot apply Contains method on a collection containing '{enumField}' values.");
    }

    if (isEmpty)
    {
      FilterStack.Push(notOp);
    }
    else
    {
      FilterStack.Push(orNode);
    }

    ExpressionTypeStack.Push(typeof(bool));
  }

  protected override void OnByteArrayMethodOperator(MethodInfo method,
                                                    bool       notOp = false)
  {
    var filter = new FilterArray();
    switch (method.Name)
    {
      case "Contains":
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

    FilterStack.Push(filterField);
  }
}
