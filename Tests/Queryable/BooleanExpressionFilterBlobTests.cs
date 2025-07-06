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

using System.Linq.Expressions;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extension.CSharp.Client.Queryable;

using NUnit.Framework;

using Tests.Configuration;

namespace Tests.Queryable;

/// <summary>
///   logical OR and logical AND tests
/// </summary>
public class BooleanExpressionFilterBlobTests : BaseBlobFilterTests
{
  // <or expression> || <or expression>
  [Test]
  public void OrExpressionOfOrAndOr()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob3")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob4")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobId == "blob1" || blobState.BlobId == "blob2") || (blobState.BlobId == "blob3" || blobState.BlobId == "blob4")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to blobState.BlobId
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));

    // Expressions "blobState.BlobId == "blobX""
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var blob2 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob2"));
    var blob3 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob3"));
    var blob4 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob4"));

    // (blob1 || blob2)
    var or1 = Expression.OrElse(blob1,
                                blob2);

    // (blob3 || blob4)
    var or2 = Expression.OrElse(blob3,
                                blob4);

    // ((blob1 || blob2) || (blob3 || blob4))
    var finalOr = Expression.OrElse(or1,
                                    or2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalOr,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <or expression> || <and expression>
  [Test]
  public void OrExpressionOfOrAndAnd()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob3"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob4")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobId == "blob1" || blobState.BlobId == "blob2") || (blobState.BlobId == "blob3" && blobState.BlobId == "blob4")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to blobState.BlobId
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));

    // Expressions "blobState.BlobId == "blobX""
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var blob2 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob2"));
    var blob3 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob3"));
    var blob4 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob4"));

    // (blob1 || blob2)
    var or1 = Expression.OrElse(blob1,
                                blob2);

    // (blob3 && blob4)
    var and2 = Expression.AndAlso(blob3,
                                  blob4);

    // ((blob1 || blob2) || (blob3 && blob4))
    var finalOr = Expression.OrElse(or1,
                                    and2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalOr,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <or expression> || <filter field>
  [Test]
  public void OrExpressionOfOrAndFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob1")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob2")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.BlobId == "blob1" || blobState.BlobName == "myBlob1" || blobState.BlobName == "myBlob2");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <and expression> || <or expression>
  [Test]
  public void OrExpressionOfAndAndOr()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob3")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob4")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobId == "blob1" && blobState.BlobId == "blob2") || (blobState.BlobId == "blob3" || blobState.BlobId == "blob4")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to blobState.BlobId
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));

    // Expressions "blobState.BlobId == "blobX""
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var blob2 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob2"));
    var blob3 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob3"));
    var blob4 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob4"));

    // (blob1 && blob2)
    var and1 = Expression.AndAlso(blob1,
                                  blob2);

    // (blob3 || blob4)
    var or2 = Expression.OrElse(blob3,
                                blob4);

    // ((blob1 && blob2) || (blob3 || blob4))
    var finalOr = Expression.OrElse(and1,
                                    or2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalOr,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <and expression> || <and expression>
  [Test]
  public void OrExpressionOfAndAndAnd()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob3"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob4")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobId == "blob1" && blobState.BlobId == "blob2") || (blobState.BlobId == "blob3" && blobState.BlobId == "blob4")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to blobState.BlobId
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));

    // Expressions "blobState.BlobId == "blobX""
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var blob2 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob2"));
    var blob3 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob3"));
    var blob4 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob4"));

    // (blob1 && blob2)
    var and1 = Expression.AndAlso(blob1,
                                  blob2);

    // (blob3 && blob4)
    var and2 = Expression.AndAlso(blob3,
                                  blob4);

    // ((blob1 && blob2) || (blob3 && blob4))
    var finalOr = Expression.OrElse(and1,
                                    and2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalOr,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <and expression> || <filter field>
  [Test]
  public void OrExpressionOfAndAndFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => (blobState.BlobId == "blob1" && blobState.BlobName == "myBlob") || blobState.BlobId == "blob2");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <filter field> || <or expression>
  [Test]
  public void OrExpressionOfFilterAndOr()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => blobState.BlobId == "blob1" || (blobState.BlobId == "blob2" || blobState.BlobName == "myBlob")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to properties
    var blobId = Expression.Property(param,
                                     nameof(BlobState.BlobId));
    var blobName = Expression.Property(param,
                                       nameof(BlobState.BlobName));

    // Comparisons
    var blob1 = Expression.Equal(blobId,
                                 Expression.Constant("blob1"));
    var blob2 = Expression.Equal(blobId,
                                 Expression.Constant("blob2"));
    var myBlob = Expression.Equal(blobName,
                                  Expression.Constant("myBlob"));

    // (blobId == "blob2" || blobName == "myBlob")
    var innerOr = Expression.OrElse(blob2,
                                    myBlob);

    // (blobId == "blob1" || (...))
    var finalOr = Expression.OrElse(blob1,
                                    innerOr);

    // Lambda: blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalOr,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <filter field> || <and expression>
  [Test]
  public void OrExpressionOfFilterAndAnd()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.BlobId == "blob1" || (blobState.BlobId == "blob2" && blobState.BlobName == "myBlob"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <filter field> || <filter field>
  [Test]
  public void OrExpressionOfFilterAndFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.BlobId == "blob1" || blobState.BlobName == "myBlob");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <or expression> && <or expression>
  [Test]
  public void AndExpressionOfOrAndOr()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob2"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob2"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => (blobState.BlobName == "myBlob1" || blobState.BlobName == "myBlob2") &&
                                          (blobState.BlobId   == "blob1"   || blobState.BlobId   == "blob2"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <or expression> && <and expression>
  [Test]
  public void AndExpressionOfOrAndAnd()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob1"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")),
                         BuildAnd(BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob2"),
                                  BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobName == "myBlob1" || blobState.BlobName == "myBlob2") && (blobState.BlobId == "blob1" && blobState.SessionId == "session1")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to properties
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));
    var blobNameProperty = Expression.Property(param,
                                               nameof(BlobState.BlobName));
    var sessionIdProperty = Expression.Property(param,
                                                nameof(BlobState.SessionId));

    // Expressions "blobState.BlobId == "blobX""
    var myBlob1 = Expression.Equal(blobNameProperty,
                                   Expression.Constant("myBlob1"));
    var myBlob2 = Expression.Equal(blobNameProperty,
                                   Expression.Constant("myBlob2"));
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var session1 = Expression.Equal(sessionIdProperty,
                                    Expression.Constant("session1"));

    // (myBlob1 || myBlob2)
    var or1 = Expression.OrElse(myBlob1,
                                myBlob2);

    // (blob1 && session1)
    var and2 = Expression.AndAlso(blob1,
                                  session1);

    // ((myBlob1 || myBlob2) && (blob1 && session1))
    var finalAnd = Expression.AndAlso(or1,
                                      and2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalAnd,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <or expression> && <filter field>
  [Test]
  public void AndExpressionOfOrAndFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob2"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => (blobState.BlobId == "blob1" || blobState.BlobId == "blob2") && blobState.BlobName == "myBlob");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <and expression> && <or expression>
  [Test]
  public void AndExpressionOfAndAndOr()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob1")),
                         BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob2")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobId == "blob1" && blobState.SessionId == "session1") && (blobState.BlobName == "myBlob1" || blobState.BlobName == "myBlob2")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to properties
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));
    var blobNameProperty = Expression.Property(param,
                                               nameof(BlobState.BlobName));
    var sessionIdProperty = Expression.Property(param,
                                                nameof(BlobState.SessionId));

    // Expressions "blobState.BlobId == "blobX""
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var session1 = Expression.Equal(sessionIdProperty,
                                    Expression.Constant("session1"));
    var myBlob1 = Expression.Equal(blobNameProperty,
                                   Expression.Constant("myBlob1"));
    var myBlob2 = Expression.Equal(blobNameProperty,
                                   Expression.Constant("myBlob2"));

    // (blob1 && session1)
    var and1 = Expression.AndAlso(blob1,
                                  session1);

    // (myBlob1 || myBlob2)
    var or2 = Expression.OrElse(myBlob1,
                                myBlob2);

    // ((blob1 && blob2) && (blob3 || blob4))
    var finalAnd = Expression.AndAlso(and1,
                                      or2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalAnd,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <and expression> && <and expression>
  [Test]
  public void AndExpressionOfAndAndAnd()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob2")));

    // Let's build manually the following expression tree because Resharper simplify the expression by removing parentheses
    // blobState => (blobState.BlobId == "blob1" && blobState.SessionId == "session1") && (blobState.BlobName == "myBlob1" && blobState.BlobName == "myBlob2")
    // Parameter: blobState =>
    var param = Expression.Parameter(typeof(BlobState),
                                     "blobState");

    // Access to properties
    var blobIdProperty = Expression.Property(param,
                                             nameof(BlobState.BlobId));
    var blobNameProperty = Expression.Property(param,
                                               nameof(BlobState.BlobName));
    var sessionIdProperty = Expression.Property(param,
                                                nameof(BlobState.SessionId));

    // Expressions "blobState.BlobId == "blobX""
    var blob1 = Expression.Equal(blobIdProperty,
                                 Expression.Constant("blob1"));
    var session1 = Expression.Equal(sessionIdProperty,
                                    Expression.Constant("session1"));
    var myBlob1 = Expression.Equal(blobNameProperty,
                                   Expression.Constant("myBlob1"));
    var myBlob2 = Expression.Equal(blobNameProperty,
                                   Expression.Constant("myBlob2"));

    // (blob1 && session1)
    var and1 = Expression.AndAlso(blob1,
                                  session1);

    // (myBlob1 && myBlob2)
    var and2 = Expression.AndAlso(myBlob1,
                                  myBlob2);

    // ((blob1 && blob2) && (blob3 || blob4))
    var finalAnd = Expression.AndAlso(and1,
                                      and2);

    // Lambda complète : blobState => ...
    var lambda = Expression.Lambda<Func<BlobState, bool>>(finalAnd,
                                                          param);

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(lambda);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <and expression> && <filter field>
  [Test]
  public void AndExpressionOfAndAndFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("SessionId",
                                                    "==",
                                                    "session1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.BlobId == "blob1" && blobState.SessionId == "session1" && blobState.BlobName == "myBlob");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }

  // <filter field> && <filter field>
  [Test]
  public void AndExpressionOfFilterAndFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("BlobId",
                                                    "==",
                                                    "blob1"),
                                  BuildFilterString("BlobName",
                                                    "==",
                                                    "myBlob")));

    // Build the query that get all blobs from session "session1"
    var query = client.BlobService.AsQueryable()
                      .Where(blobState => blobState.BlobId == "blob1" && blobState.BlobName == "myBlob");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var blobQueryProvider = (BlobQueryProvider)((ArmoniKQueryable<BlobState>)query).Provider;
    Assert.That(blobQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildBlobPagination(filter)));
  }
}
