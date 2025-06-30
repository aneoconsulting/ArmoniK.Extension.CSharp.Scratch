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

using System.Data;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Filtering;

using NUnit.Framework;

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Filtering;

public class FilterBlobTests : BaseBlobFilterTests
{
  [Test]
  public void BlobIdFilterWithConstant()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListResultsRequest, ListResultsResponse>(response);

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

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

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

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == "blob" + "1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithBlobStateProperty()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var blob = new BlobState
               {
                 BlobId = "blob1",
               };
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == blob.BlobId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobStatusFilterWithStatus()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterStatus("Status",
                                                    "==",
                                                    BlobStatus.Completed)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Status == BlobStatus.Completed);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobStatusFilterWithStatusClosure()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterStatus("Status",
                                                    "!=",
                                                    BlobStatus.Completed)));

    var status = BlobStatus.Completed;
    var query  = client.BlobService.BlobCollection.Where(blobState => blobState.Status != status);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CreateAtFilterEquals()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.Now;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      "==",
                                                      date)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.CreateAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CreateAtFilterGreater()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.Now;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      ">",
                                                      date)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.CreateAt > date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CreateAtFilterGreaterOrEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.Now;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      ">=",
                                                      date)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.CreateAt >= date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CreateAtFilterLessThan()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.Now;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      "<",
                                                      date)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.CreateAt < date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CreateAtFilterLessThanOrEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.Now;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      "<=",
                                                      date)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.CreateAt <= date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CompletedAtEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.Now;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CompletedAt",
                                                      "==",
                                                      date)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.CompletedAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void OwnerIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("OwnerId",
                                                    "==",
                                                    "task1")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.OwnerId == "task1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void SizeFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Size",
                                                 "==",
                                                 100)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Size == 100);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void SizeFilterLessThan()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Size",
                                                 "<",
                                                 100)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Size < 100);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void SizeFilterLessThanOrEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Size",
                                                 "<=",
                                                 100)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Size <= 100);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void SizeFilterGreaterThan()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Size",
                                                 ">",
                                                 100)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Size > 100);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void SizeFilterGreaterThanOrEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Size",
                                                 ">=",
                                                 100)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Size >= 100);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void SizeFilterNotEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Size",
                                                 "!=",
                                                 100)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.Size != 100);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithStartWith()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "StartsWith",
                                                    "blob")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId.StartsWith("blob"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithStartWithChar()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "StartsWith",
                                                    "b")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId.StartsWith('b'));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  private static string Foo()
    => "blob";

  [Test]
  public void BlobIdFilterWithMethodCall1()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == Foo());

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithMethodCall2()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var obj = new RecursiveClass
              {
                Inner = new RecursiveClass
                        {
                          Inner = new RecursiveClass
                                  {
                                    Info = "blob1",
                                  },
                        },
              };
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == obj.Inner.Inner.GetInfo());

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithMemberAccess()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var obj = new RecursiveClass
              {
                Inner = new RecursiveClass
                        {
                          Inner = new RecursiveClass
                                  {
                                    Info = "blob1",
                                  },
                        },
              };
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == obj.Inner.Inner.Info);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithTuple()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var data  = ("session1", "blob1");
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.SessionId == data.Item1 && blobState.BlobId == data.Item2);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithExpression()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == Foo() + "1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithEndsWith()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "EndsWith",
                                                    "blob")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId.EndsWith("blob"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "Contains",
                                                    "blob")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId.Contains("blob"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobIdFilterWithNotContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "NotContains",
                                                    "blob")));

    var query = client.BlobService.BlobCollection.Where(blobState => !blobState.BlobId.Contains("blob"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void OpaqueIdFilterWithContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterArray("OpaqueId",
                                                   "Contains",
                                                   1)));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.OpaqueId.Contains<byte>(1));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void OpaqueIdFilterWithNotContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterArray("OpaqueId",
                                                   "NotContains",
                                                   1)));

    var query = client.BlobService.BlobCollection.Where(blobState => !blobState.OpaqueId.Contains<byte>(1));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CombinedFilter1()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var query = client.BlobService.BlobCollection.Where(blobState => blobState.SessionId == "session1")
                      .Where(blobState => blobState.BlobId                               == "blob1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void CombinedFilter2()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")));

    var query1 = client.BlobService.BlobCollection.Where(blobState => blobState.SessionId == "session1");
    var query2 = query1.Where(blobState => blobState.BlobId                               == "blob1");

    // Execute the query
    var result = query2.AsAsyncEnumerable()
                       .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query2).Provider;
    Assert.That(blobQueryProvider.BlobPagination,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  [Test]
  public void BlobFilterFailure()
  {
    var client = new MockedArmoniKClient();

    // Invalid filter
    var query = client.BlobService.BlobCollection.Where(blobState => blobState.BlobId == blobState.BlobName);

    // Execute the query
    Assert.Throws<InvalidExpressionException>(() => query.ToList());
  }

  private class RecursiveClass
  {
    public RecursiveClass? Inner { get; init; }

    public string Info { get; init; } = "";

    public string GetInfo()
      => Info;
  }
}
