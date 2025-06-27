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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Filtering;

public class BlobQueryProvider : IAsyncQueryProvider<BlobState>
{
  private readonly IBlobService          blobService_;
  private readonly ILogger<IBlobService> logger_;

  public BlobQueryProvider(IBlobService          blobService,
                           ILogger<IBlobService> logger)
  {
    blobService_ = blobService;
    logger_      = logger;
  }

  public BlobPagination BlobPagination { get; private set; }

  public IQueryable CreateQuery(Expression expression)
    => throw new InvalidOperationException("Use CreateQuery<TElement>(Expression expression) instead");

  public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    => new ArmoniKQueryable<TElement>(this,
                                      expression);

  public object Execute(Expression expression)
    => ExecuteAsync(expression)
       .ToListAsync()
       .Result;

  public TResult Execute<TResult>(Expression expression)
    => (TResult)Execute(expression);

  public async IAsyncEnumerable<BlobState> ExecuteAsync(Expression                                 expression,
                                                        [EnumeratorCancellation] CancellationToken token = default)
  {
    var visitor = new BlobFilterExpressionTreeVisitor(expression);

    try
    {
      visitor.Visit();
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Invalid blob filter: " + expression);
      throw new InvalidExpressionException("Invalid blob filter: " + expression,
                                           ex);
    }

    BlobPagination = new BlobPagination
                     {
                       Filter        = visitor.Filters ?? new Filters(),
                       Page          = -1,
                       PageSize      = 50,
                       SortDirection = SortDirection.Asc,
                       SortField = new ResultField
                                   {
                                     ResultRawField = new ResultRawField
                                                      {
                                                        Field = ResultRawEnumField.ResultId,
                                                      },
                                   },
                     };
    var      total = 0;
    BlobPage page;
    do
    {
      BlobPagination.Page++;
      page = await blobService_.ListBlobsAsync(BlobPagination,
                                               token)
                               .ConfigureAwait(false);
      total += page.Blobs.Length;
      foreach (var blobState in page.Blobs)
      {
        yield return blobState;
      }
    } while (total < page.TotalBlobCount);
  }
}
