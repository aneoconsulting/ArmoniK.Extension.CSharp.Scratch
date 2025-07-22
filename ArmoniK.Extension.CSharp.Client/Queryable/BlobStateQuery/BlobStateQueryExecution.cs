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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Queryable.BlobStateQuery;

internal class BlobStateQueryExecution : QueryExecution<BlobPagination, BlobPage, BlobState, ResultField, Filters, FiltersAnd, FilterField>
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

  protected override async Task<BlobPage> RequestInstances(BlobPagination    pagination,
                                                           CancellationToken cancellationToken)
  {
    pagination.Page++;
    return await blobService_.ListBlobsAsync(pagination,
                                             cancellationToken)
                             .ConfigureAwait(false);
  }

  protected override QueryExpressionTreeVisitor<BlobState, ResultField, Filters, FiltersAnd, FilterField> CreateQueryExpressionTreeVisitor()
    => new BlobStateQueryExpressionTreeVisitor();

  protected override BlobPagination CreatePaginationInstance(Filters     filter,
                                                             ResultField sortCriteria,
                                                             bool        isAscending)
    => new()
       {
         Filter   = filter,
         Page     = -1,
         PageSize = 50,
         SortDirection = isAscending
                           ? SortDirection.Asc
                           : SortDirection.Desc,
         SortField = sortCriteria,
       };

  protected override int GetTotalPageElements(BlobPage page)
    => page.TotalBlobCount;

  protected override IEnumerable<BlobState> GetPageElements(BlobPage page)
    => page.Blobs;
}
