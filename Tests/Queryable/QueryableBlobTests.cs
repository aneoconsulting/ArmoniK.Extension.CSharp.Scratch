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
using ArmoniK.Extension.CSharp.Client.Queryable;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Queryable;

public class QueryableBlobTests : BaseBlobFilterTests
{
  [Test]
  public void OrderByBlobIdAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.SessionId == "session1")
                      .OrderBy(blobState => blobState.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
  }

  [Test]
  public void OrderByBlobIdDescending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.SessionId == "session1")
                      .OrderByDescending(blobState => blobState.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId",
                                               false)));
  }

  [Test]
  public void OrderByBlobIdAscendingInvertedCall()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    var query = client.BlobService.BlobCollection.OrderBy(blobState => blobState.BlobId)
                      .Where(blobState => blobState.SessionId == "session1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
  }

  [Test]
  public void OrderByBlobIdAscendingDoubleCall()
  {
    var client = new MockedArmoniKClient();

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

    // The last call is right
    var query = client.BlobService.BlobCollection.OrderBy(blobState => blobState.Size)
                      .OrderBy(blobState => blobState.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(new Filters(),
                                               "BlobId")));
  }
}
