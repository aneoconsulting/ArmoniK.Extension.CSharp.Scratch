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
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using FilterStatus = ArmoniK.Api.gRPC.V1.Tasks.FilterStatus;
using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;
using Type = System.Type;

namespace ArmoniK.Extension.CSharp.Client.Queryable.TaskStateQuery;

internal class TaskStateWhereExpressionTreeVisitor : WhereExpressionTreeVisitor<Filters, FiltersAnd, FilterField>
{
  private static readonly Dictionary<string, TaskSummaryEnumField> memberName2EnumField_ = new()
                                                                                           {
                                                                                             {
                                                                                               nameof(TaskInfos.TaskId), TaskSummaryEnumField.TaskId
                                                                                             },
                                                                                             {
                                                                                               nameof(TaskInfos.PayloadId), TaskSummaryEnumField.PayloadId
                                                                                             },
                                                                                             {
                                                                                               nameof(TaskInfos.SessionId), TaskSummaryEnumField.SessionId
                                                                                             },
                                                                                             {
                                                                                               nameof(TaskState.CreateAt), TaskSummaryEnumField.CreatedAt
                                                                                             },
                                                                                             {
                                                                                               nameof(TaskState.EndedAt), TaskSummaryEnumField.EndedAt
                                                                                             },
                                                                                             {
                                                                                               nameof(TaskState.StartedAt), TaskSummaryEnumField.StartedAt
                                                                                             },
                                                                                             {
                                                                                               nameof(TaskState.Status), TaskSummaryEnumField.Status
                                                                                             },
                                                                                           };

  private static readonly Dictionary<string, TaskOptionEnumField> memberName2OptionEnumField_ = new()
                                                                                                {
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.MaxDuration),
                                                                                                    TaskOptionEnumField.MaxDuration
                                                                                                  },
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.MaxRetries), TaskOptionEnumField.MaxRetries
                                                                                                  },
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.PartitionId),
                                                                                                    TaskOptionEnumField.PartitionId
                                                                                                  },
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.Priority), TaskOptionEnumField.Priority
                                                                                                  },
                                                                                                };

  private static readonly Dictionary<string, Type> memberName2Type_ = new()
                                                                      {
                                                                        {
                                                                          nameof(TaskInfos.TaskId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(TaskInfos.PayloadId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(TaskInfos.SessionId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(TaskState.CreateAt), typeof(DateTime)
                                                                        },
                                                                        {
                                                                          nameof(TaskState.EndedAt), typeof(DateTime)
                                                                        },
                                                                        {
                                                                          nameof(TaskState.StartedAt), typeof(DateTime)
                                                                        },
                                                                        {
                                                                          nameof(TaskState.Status), typeof(TaskStatus)
                                                                        },
                                                                        {
                                                                          nameof(TaskConfiguration.MaxDuration), typeof(TimeSpan)
                                                                        },
                                                                        {
                                                                          nameof(TaskConfiguration.MaxRetries), typeof(int)
                                                                        },
                                                                        {
                                                                          nameof(TaskConfiguration.PartitionId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(TaskConfiguration.Priority), typeof(int)
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
    Type memberType;
    if (member.Expression.Type == typeof(TaskConfiguration))
    {
      if (memberName2Type_.TryGetValue(member.Member.Name,
                                       out memberType) && memberName2OptionEnumField_.TryGetValue(member.Member.Name,
                                                                                                  out var enumOptionField))
      {
        // filter on a property of TaskOption property
        FilterStack.Push((enumOptionField, memberType));
        return true;
      }

      if (member.Member.Name == nameof(TaskConfiguration.Options))
      {
        var optionGenericField = new TaskOptionGenericField(); // value will be set later
        FilterStack.Push((optionGenericField, typeof(string)));
        return true;
      }
    }

    if (memberName2Type_.TryGetValue(member.Member.Name,
                                     out memberType) && memberName2EnumField_.TryGetValue(member.Member.Name,
                                                                                          out var enumField))
    {
      FilterStack.Push((enumField, memberType));
      return true;
    }

    return false;
  }

  protected override void OnIndexerAccess()
  {
    var (rhs, _) = FilterStack.Pop();
    var (lhs, _) = FilterStack.Pop();

    if (lhs is TaskOptionGenericField optionGenericField && rhs is string key)
    {
      optionGenericField.Field = key;
      FilterStack.Push((optionGenericField, typeof(string)));
    }
    else
    {
      throw new InvalidExpressionException("Invalid filter expression");
    }
  }

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
    if (lhsFilter is TaskSummaryEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
                                               {
                                                 Field = lhsFilterField,
                                               },
                          };
      fieldCount++;
    }
    else if (lhsFilter is TaskOptionEnumField lhsFilterOptionField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = lhsFilterOptionField,
                                              },
                          };
      fieldCount++;
    }
    else if (lhsFilter is TaskOptionGenericField lhsFilterOptionGenericField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionGenericField = lhsFilterOptionGenericField,
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

    if (rhsFilter is TaskSummaryEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
                                               {
                                                 Field = rhsFilterField,
                                               },
                          };
      fieldCount++;
    }
    else if (rhsFilter is TaskOptionEnumField rhsFilterOptionField)
    {
      // Right hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = rhsFilterOptionField,
                                              },
                          };
      fieldCount++;
    }
    else if (rhsFilter is TaskOptionGenericField rhsFilterOptionGenericField)
    {
      // Tight hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionGenericField = rhsFilterOptionGenericField,
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
    if (lhsFilter is TaskOptionEnumField lhsFilterOptionField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = lhsFilterOptionField,
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

    if (rhsFilter is TaskOptionEnumField rhsFilterOptionField)
    {
      // Right hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = rhsFilterOptionField,
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
    if (lhsFilter is TaskSummaryEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
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

    if (rhsFilter is TaskSummaryEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
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
  {
    var filterField = new FilterField();
    var filter      = new FilterDuration();
    switch (type)
    {
      case ExpressionType.Equal:
        filter.Operator = FilterDurationOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        filter.Operator = FilterDurationOperator.NotEqual;
        break;
      case ExpressionType.GreaterThan:
        filter.Operator = FilterDurationOperator.LongerThan;
        break;
      case ExpressionType.GreaterThanOrEqual:
        filter.Operator = FilterDurationOperator.LongerThanOrEqual;
        break;
      case ExpressionType.LessThan:
        filter.Operator = FilterDurationOperator.ShorterThan;
        break;
      case ExpressionType.LessThanOrEqual:
        filter.Operator = FilterDurationOperator.ShorterThanOrEqual;
        break;
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    if (lhsFilter is TaskOptionEnumField lhsFilterOptionField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = lhsFilterOptionField,
                                              },
                          };
      fieldCount++;
    }
    else if (lhsFilter is TimeSpan timeSpan)
    {
      // Left hand side is a constant
      filterField.FilterDuration = filter;
      filter.Value               = Duration.FromTimeSpan(timeSpan);
      constCount++;
    }

    if (rhsFilter is TaskOptionEnumField rhsFilterOptionField)
    {
      // Right hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskOptionField = new TaskOptionField
                                              {
                                                Field = rhsFilterOptionField,
                                              },
                          };
      fieldCount++;
    }
    else if (rhsFilter is TimeSpan timeSpan)
    {
      // Right hand side is a constant
      filterField.FilterDuration = filter;
      filter.Value               = Duration.FromTimeSpan(timeSpan);
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((filterField, typeof(bool)));
  }

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
    if (lhsFilter is TaskSummaryEnumField lhsFilterField)
    {
      // Left hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
                                               {
                                                 Field = lhsFilterField,
                                               },
                          };
      fieldCount++;
    }
    else if (lhsFilter is TaskStatus status)
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
      filter.Value             = ((TaskStatus)statusInt).ToGrpcStatus();
      constCount++;
    }

    if (rhsFilter is TaskSummaryEnumField rhsFilterField)
    {
      // Right hand side is the property
      filterField.Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
                                               {
                                                 Field = rhsFilterField,
                                               },
                          };
      fieldCount++;
    }
    else if (rhsFilter is TaskStatus status)
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
      filter.Value             = ((TaskStatus)statusInt).ToGrpcStatus();
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
        throw new InvalidOperationException($"Method string.{method.Name} is not supported to filter tasks.");
    }
  }

  protected override void OnByteArrayMethodOperator(MethodInfo method,
                                                    bool       notOp = false)
    // Should never happen
    => throw new InvalidOperationException("Internal error: byte array expression unexpected in a TaskState filter");
}
