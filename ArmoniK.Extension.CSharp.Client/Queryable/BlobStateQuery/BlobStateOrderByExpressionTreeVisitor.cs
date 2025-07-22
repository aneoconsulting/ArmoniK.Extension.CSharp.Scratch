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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

namespace ArmoniK.Extension.CSharp.Client.Queryable.BlobStateQuery;

internal class BlobStateOrderByExpressionTreeVisitor : OrderByExpressionTreeVisitor<ResultField>
{
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

  public override ResultField Visit(LambdaExpression lambda)
  {
    if (lambda.Body is MemberExpression member)
    {
      if (memberName2EnumField_.TryGetValue(member.Member.Name,
                                            out var field))
      {
        return new ResultField
               {
                 ResultRawField = new ResultRawField
                                  {
                                    Field = field,
                                  },
               };
      }
    }

    throw new InvalidExpressionException("Invalid blob ordering expression: a sortable BlobState property was expected." + Environment.NewLine + "Expression was: " +
                                         lambda.Body);
  }
}
