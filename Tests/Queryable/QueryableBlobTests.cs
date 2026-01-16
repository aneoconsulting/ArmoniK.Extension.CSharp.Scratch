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
using ArmoniK.Extension.CSharp.Client.Queryable;
using ArmoniK.Extension.CSharp.Client.Queryable.BlobState;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.SessionId == "session1")
                      .OrderBy(blobState => blobState.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.SessionId == "session1")
                      .OrderByDescending(blobState => blobState.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var query = client.BlobService.AsQueryable()
                      .OrderBy(blobState => blobState.BlobId)
                      .Where(blobState => blobState.SessionId == "session1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
  }

  [Test]
  public void OrderByBlobIdAscendingDoubleCall()
  {
    var client = new MockedArmoniKClient();

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    // The last call to OrderBy is right (Here BlobId)
    var query = client.BlobService.AsQueryable()
                      .OrderBy(blobState => blobState.Size)
                      .OrderBy(blobState => blobState.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(new Filters(),
                                               "BlobId")));
  }

  [Test]
  public void FirstOrDefaultOnSessionId()
  {
    var client = new MockedArmoniKClient();

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.SessionId == "session1");

    // Execute the query
    var result = query.FirstOrDefault();

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
    Assert.That(result!.BlobId,
                Is.EqualTo("blob1Id"));

    // Execute query with an OrderBy
    var resultOrderBy = query.OrderBy(blobState => blobState.BlobId)
                             .FirstOrDefault();

    blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
    Assert.That(resultOrderBy!.BlobId,
                Is.EqualTo("blob1Id"));
  }

  [Test]
  public void FirstOrDefaultWithLambdaOnSessionId()
  {
    var client = new MockedArmoniKClient();

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    BlobStatus.Completed)));

    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.SessionId == "session1");

    // Execute the query (combine the Where condition && FirstOrDefault condition)
    var result = query.FirstOrDefault(blobState => blobState.Status == BlobStatus.Completed);

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
    Assert.That(result!.BlobId,
                Is.EqualTo("blob1Id"));
  }

  [Test]
  public void FirstOnSessionId()
  {
    var client = new MockedArmoniKClient();

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.SessionId == "session1");

    // Execute the query
    var result = query.First();

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
    Assert.That(result.BlobId,
                Is.EqualTo("blob1Id"));

    // Execute query with an OrderBy
    var resultOrderBy = query.OrderBy(blobState => blobState.BlobId)
                             .First();

    blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
    Assert.That(resultOrderBy.BlobId,
                Is.EqualTo("blob1Id"));
  }

  [Test]
  public void FirstWithLambdaOnSessionId()
  {
    var client = new MockedArmoniKClient();

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(Response);

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    BlobStatus.Completed)));

    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.SessionId == "session1");

    // Execute the query (combine the Where condition && FirstOrDefault condition)
    var result = query.First(blobState => blobState.Status == BlobStatus.Completed);

    var blobQueryProvider = (BlobStateQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter,
                                               "BlobId")));
    Assert.That(result.BlobId,
                Is.EqualTo("blob1Id"));
  }
}
