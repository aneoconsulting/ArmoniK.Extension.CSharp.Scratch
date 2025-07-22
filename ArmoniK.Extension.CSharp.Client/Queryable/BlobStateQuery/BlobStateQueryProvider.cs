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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extension.CSharp.Client.Queryable.BlobStateQuery;

/// <summary>
///   Specialisation of ArmoniKQueryProvider for queries on BlobState instances.
/// </summary>
internal class BlobStateQueryProvider : ArmoniKQueryProvider<IBlobService, BlobPagination, BlobPage, BlobState, ResultField, Filters, FiltersAnd, FilterField>
{
  private readonly IBlobService blobService_;

  public BlobStateQueryProvider(IBlobService          service,
                                ILogger<IBlobService> logger)
    : base(logger)
    => blobService_ = service;

  protected override QueryExecution<BlobPagination, BlobPage, BlobState, ResultField, Filters, FiltersAnd, FilterField> CreateQueryExecution()
    => new BlobStateQueryExecution(blobService_,
                                   logger_);
}
