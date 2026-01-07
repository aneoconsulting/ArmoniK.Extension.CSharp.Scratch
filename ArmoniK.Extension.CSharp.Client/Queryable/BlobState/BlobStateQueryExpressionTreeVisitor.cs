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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.Extension.CSharp.Client.Queryable;

internal class BlobStateQueryExpressionTreeVisitor : QueryExpressionTreeVisitor<BlobState, ResultRawEnumField, Filters, FiltersAnd, FilterField>
{
  private OrderByExpressionTreeVisitor<ResultRawEnumField>                                  orderByVisitor_;
  private WhereExpressionTreeVisitor<ResultRawEnumField, Filters, FiltersAnd, FilterField>? whereVisitor_;

  public BlobStateQueryExpressionTreeVisitor()
  {
    // By default the requests are ordered by BlobId in ascending order
    SortCriteria    = ResultRawEnumField.ResultId;
    IsSortAscending = true;
  }

  protected override bool IsWhereExpressionTreeVisitorInstantiated
    => whereVisitor_ != null;

  protected override WhereExpressionTreeVisitor<ResultRawEnumField, Filters, FiltersAnd, FilterField> WhereExpressionTreeVisitor
  {
    get
    {
      whereVisitor_ = whereVisitor_ ?? new BlobStateWhereExpressionTreeVisitor();
      return whereVisitor_;
    }
  }

  protected override OrderByExpressionTreeVisitor<ResultRawEnumField> OrderByWhereExpressionTreeVisitor
  {
    get
    {
      orderByVisitor_ = orderByVisitor_ ?? new BlobStateOrderByExpressionTreeVisitor();
      return orderByVisitor_;
    }
  }
}
