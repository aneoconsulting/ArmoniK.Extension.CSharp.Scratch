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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

namespace ArmoniK.Extension.CSharp.Client.Queryable.TaskStateQuery;

/// <summary>
///   Specialisation of OrderByExpressionTreeVisitor for queries on TaskState instances.
/// </summary>
internal class TaskStateOrderByExpressionTreeVisitor : OrderByExpressionTreeVisitor<TaskField>
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

  /// <inheritdoc />
  public override TaskField Visit(LambdaExpression lambda)
  {
    switch (lambda.Body)
    {
      case MemberExpression member:
        if (member.IsLeftMostQualifierAParameter())
        {
          if (memberName2EnumField_.TryGetValue(member.Member.Name,
                                                out var field))
          {
            return new TaskField
                   {
                     TaskSummaryField = new TaskSummaryField
                                        {
                                          Field = field,
                                        },
                   };
          }

          if (member.Expression.Type == typeof(TaskConfiguration) && memberName2OptionEnumField_.TryGetValue(member.Member.Name,
                                                                                                             out var optionField))
          {
            return new TaskField
                   {
                     TaskOptionField = new TaskOptionField
                                       {
                                         Field = optionField,
                                       },
                   };
          }
        }

        break;
      case MethodCallExpression call:
        if (call.Method.Name == "get_Item" && call.IsLeftMostQualifierAParameter())
        {
          var member = call.Object as MemberExpression;
          if (member?.Member.Name == nameof(TaskConfiguration.Options))
          {
            var result = call.Arguments[0]
                             .EvaluateExpression()
                             ?.ToString();
            if (result != null)
            {
              return new TaskField
                     {
                       TaskOptionGenericField = new TaskOptionGenericField
                                                {
                                                  Field = result,
                                                },
                     };
            }
          }
        }

        break;
    }

    throw new InvalidExpressionException("Invalid task ordering expression: a sortable TaskState property was expected." + Environment.NewLine + "Expression was: " +
                                         lambda.Body);
  }
}
