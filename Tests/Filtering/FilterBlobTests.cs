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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Filtering;

using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Filtering;

public class FilterBlobTests : BaseBlobFilterTests
{
  private readonly ListResultsResponse response = new()
                                                  {
                                                    Results =
                                                    {
                                                      new ResultRaw
                                                      {
                                                        ResultId    = "blob1Id",
                                                        Name        = "blob1",
                                                        SessionId   = "sessionId",
                                                        Status      = ResultStatus.Completed,
                                                        CreatedAt   = DateTime.UtcNow.ToTimestamp(),
                                                        CompletedAt = DateTime.UtcNow.ToTimestamp(),
                                                      },
                                                      new ResultRaw
                                                      {
                                                        ResultId    = "blob2Id",
                                                        Name        = "blob2",
                                                        SessionId   = "sessionId",
                                                        Status      = ResultStatus.Completed,
                                                        CreatedAt   = DateTime.UtcNow.ToTimestamp(),
                                                        CompletedAt = DateTime.UtcNow.ToTimestamp(),
                                                      },
                                                    },
                                                    Total = 2,
                                                  };

  private BlobPagination BuildBlobPagination(Filters filter)
    => new()
       {
         Filter        = filter,
         Page          = 0,
         PageSize      = 50,
         SortDirection = SortDirection.Asc,
         SortField     = ResultRawEnumField.ResultId,
       };

  [Test]
  public void BlobIdFilterWithConstant()
  {
    var client = new MockedArmoniKClient();

    var filter = new Filters
                 {
                   Or =
                   {
                     new FiltersAnd
                     {
                       And =
                       {
                         BuildFilterString("BlobId",
                                           "==",
                                           "blob1"),
                       },
                     },
                   },
                 };

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == "blob1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithLocalVar()
  {
    var client = new MockedArmoniKClient();

    var filter = new Filters
                 {
                   Or =
                   {
                     new FiltersAnd
                     {
                       And =
                       {
                         BuildFilterString("BlobId",
                                           "==",
                                           "blob1"),
                       },
                     },
                   },
                 };

    // Build the query that get all blobs from session "session1"
    var blob  = "blob1";
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == blob);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();
    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithConstantExpression()
  {
    var client = new MockedArmoniKClient();

    var filter = new Filters
                 {
                   Or =
                   {
                     new FiltersAnd
                     {
                       And =
                       {
                         BuildFilterString("BlobId",
                                           "==",
                                           "blob1"),
                       },
                     },
                   },
                 };

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == "blob" + "1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }
}
