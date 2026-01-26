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
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.BlobState;

internal class BlobStateQueryProvider : ArmoniKQueryProvider<BlobPage, CSharp.Common.Common.Domain.Blob.BlobState, ResultField, ResultRawEnumField, Filters, FiltersAnd,
  FilterField>
{
  private readonly IBlobService blobService_;

  public BlobStateQueryProvider(IBlobService          service,
                                ILogger<IBlobService> logger)
    : base(logger)
    => blobService_ = service;

  protected override QueryExecution<BlobPage, CSharp.Common.Common.Domain.Blob.BlobState, ResultField, ResultRawEnumField, Filters, FiltersAnd, FilterField>
    CreateQueryExecution()
    => new BlobStateQueryExecution(blobService_,
                                   Logger);
}
