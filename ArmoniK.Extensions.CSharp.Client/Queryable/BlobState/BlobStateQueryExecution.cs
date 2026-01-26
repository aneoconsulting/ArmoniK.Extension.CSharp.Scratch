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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Enum;
using ArmoniK.Extensions.CSharp.Client.Common.Generic;
using ArmoniK.Extensions.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.BlobState;

internal class BlobStateQueryExecution : QueryExecution<BlobPage, CSharp.Common.Common.Domain.Blob.BlobState, ResultField, ResultRawEnumField, Filters, FiltersAnd,
  FilterField>
{
  private readonly IBlobService          blobService_;
  private readonly ILogger<IBlobService> logger_;

  public BlobStateQueryExecution(IBlobService          service,
                                 ILogger<IBlobService> logger)
  {
    blobService_ = service;
    logger_      = logger;
  }

  protected override void LogError(Exception ex,
                                   string    message)
    => logger_.LogError(ex,
                        message);

  protected override async Task<BlobPage> RequestInstancesAsync(Pagination<Filters, ResultField> pagination,
                                                                CancellationToken                cancellationToken)
    => await blobService_.ListBlobsAsync((BlobPagination)pagination,
                                         cancellationToken)
                         .ConfigureAwait(false);

  protected override QueryExpressionTreeVisitor<CSharp.Common.Common.Domain.Blob.BlobState, ResultRawEnumField, Filters, FiltersAnd, FilterField>
    CreateQueryExpressionTreeVisitor()
    => new BlobStateQueryExpressionTreeVisitor();

  protected override Pagination<Filters, ResultField> CreatePaginationInstance(
    QueryExpressionTreeVisitor<CSharp.Common.Common.Domain.Blob.BlobState, ResultRawEnumField, Filters, FiltersAnd, FilterField> visitor)
    => new BlobPagination
       {
         Filter = visitor.Filters!,
         Page   = 0,
         PageSize = visitor.PageSize.HasValue
                      ? visitor.PageSize.Value
                      : 1000,
         SortDirection = visitor.IsSortAscending
                           ? SortDirection.Asc
                           : SortDirection.Desc,
         SortField = new ResultField
                     {
                       ResultRawField = new ResultRawField
                                        {
                                          Field = visitor.SortCriteria,
                                        },
                     },
       };

  protected override int GetTotalPageElements(BlobPage page)
    => page.TotalBlobCount;

  protected override CSharp.Common.Common.Domain.Blob.BlobState[] GetPageElements(BlobPage page)
    => page.Blobs;
}
