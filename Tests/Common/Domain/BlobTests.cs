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
using ArmoniK.Extensions.CSharp.Client.Common.Enum;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;

using NUnit.Framework;

namespace Tests.Common.Domain;

[TestFixture]
public class BlobTests
{
  [Test]
  public void CreateBlobInfoTest()
  {
    var blobInfo = new BlobInfo
                   {
                     SessionId = "sessionId",
                     BlobName  = "blobName",
                     BlobId    = "blobId",
                   };
    Assert.That(blobInfo.SessionId,
                Is.EqualTo("sessionId"));
    Assert.That(blobInfo.BlobName,
                Is.EqualTo("blobName"));
    Assert.That(blobInfo.BlobId,
                Is.EqualTo("blobId"));
  }

  [Test]
  public void CreateBlobInfoAndCompareWithAnother()
  {
    var blobInfo = new BlobInfo
                   {
                     SessionId = "sessionId",
                     BlobName  = "blobName",
                     BlobId    = "blobId",
                   };
    var anotherBlobInfo = new BlobInfo
                          {
                            SessionId = "sessionId",
                            BlobName  = "blobName",
                            BlobId    = "blobId",
                          };
    Assert.That(blobInfo,
                Is.EqualTo(anotherBlobInfo));
  }

  [Test]
  public void BlobInfoWithSamePropertiesAreEqual()
  {
    var properties = new
                     {
                       SessionId = "session",
                       BlobName  = "blob",
                       BlobId    = "id",
                     };
    var blobInfo1 = new BlobInfo
                    {
                      SessionId = properties.SessionId,
                      BlobName  = properties.BlobName,
                      BlobId    = properties.BlobId,
                    };
    var blobInfo2 = new BlobInfo
                    {
                      SessionId = properties.SessionId,
                      BlobName  = properties.BlobName,
                      BlobId    = properties.BlobId,
                    };

    Assert.That(blobInfo1,
                Is.EqualTo(blobInfo2));
  }

  [Test]
  public void CreateBlobPage()
  {
    var blobDetails = new BlobState
                      {
                        SessionId = "sessionId",
                        BlobName  = "blobName",
                        BlobId    = "blobId",
                        Status    = BlobStatus.Created,
                        CompletedAt = new DateTime(2025,
                                                   05,
                                                   15),
                        CreateAt = new DateTime(2025,
                                                05,
                                                15),
                      };
    var blobPage = new BlobPage
                   {
                     TotalBlobCount = 5,
                     PageOrder      = 0,
                     Blobs          = [blobDetails],
                   };
    Assert.That(blobPage.TotalBlobCount,
                Is.EqualTo(5));
    Assert.That(blobDetails,
                Is.EqualTo(blobPage.Blobs[0]));
  }

  [Test]
  public void CreateBlobStateTest()
  {
    var blobDetails = new BlobState
                      {
                        SessionId = "sessionId",
                        BlobName  = "blobName",
                        BlobId    = "blobId",
                        Status    = BlobStatus.Created,
                        CompletedAt = new DateTime(2025,
                                                   05,
                                                   15),
                        CreateAt = new DateTime(2025,
                                                05,
                                                15),
                      };
    var expectedDate = new DateTime(2025,
                                    05,
                                    15);
    var expectedFormatted = expectedDate.ToString("yy/MM/dd");

    Assert.That(blobDetails.SessionId,
                Is.EqualTo("sessionId"));
    Assert.That(blobDetails.BlobName,
                Is.EqualTo("blobName"));
    Assert.That(blobDetails.BlobId,
                Is.EqualTo("blobId"));
    Assert.That(blobDetails.Status,
                Is.EqualTo(BlobStatus.Created));
    Assert.That(blobDetails.CreateAt.ToString("yy/MM/dd"),
                Is.EqualTo(expectedFormatted));
    Assert.That(blobDetails.CompletedAt?.ToString("yy/MM/dd"),
                Is.EqualTo(expectedFormatted));
  }

  [TestCase(BlobStatus.Unspecified)]
  [TestCase(BlobStatus.Created)]
  [TestCase(BlobStatus.Completed)]
  [TestCase(BlobStatus.Aborted)]
  [TestCase(BlobStatus.Deleted)]
  [TestCase(BlobStatus.Notfound)]
  [TestCase((BlobStatus)99)]
  public void TestBlobStatus(BlobStatus status)
  {
    var blobDetails = new BlobState
                      {
                        Status = status,
                      };
    Assert.That(blobDetails.Status,
                Is.EqualTo(status));

    switch (status)
    {
      case BlobStatus.Unspecified:
      case BlobStatus.Created:
      case BlobStatus.Completed:
      case BlobStatus.Aborted:
      case BlobStatus.Deleted:
      case BlobStatus.Notfound:
        break;
      default:
        Assert.That(() => SomeMethodThatUsesBlobStatus(status),
                    Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        break;
    }
  }

  private void SomeMethodThatUsesBlobStatus(BlobStatus status)
  {
    if (!Enum.IsDefined(typeof(BlobStatus),
                        status))
    {
      throw new ArgumentOutOfRangeException(nameof(status),
                                            status,
                                            "Undefined BlobStatus value");
    }
  }

  [TestCase(BlobStatus.Unspecified)]
  [TestCase(BlobStatus.Created)]
  [TestCase(BlobStatus.Completed)]
  [TestCase(BlobStatus.Aborted)]
  [TestCase(BlobStatus.Deleted)]
  [TestCase(BlobStatus.Notfound)]
  [TestCase((BlobStatus)99)]
  public void TestBlobStatusExtensions(BlobStatus status)
  {
    switch (status)
    {
      case BlobStatus.Unspecified:
      case BlobStatus.Created:
      case BlobStatus.Completed:
      case BlobStatus.Aborted:
      case BlobStatus.Deleted:
      case BlobStatus.Notfound:
        var grpcStatus = status.ToGrpcStatus();
        Assert.That(grpcStatus.ToString(),
                    Is.EqualTo(status.ToString()));

        var internalStatus = grpcStatus.ToInternalStatus();
        Assert.That(internalStatus,
                    Is.EqualTo(status));
        break;
      default:
        Assert.That(() => status.ToGrpcStatus(),
                    Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        break;
    }
  }

  [Test]
  public void BlobPaginationTest()
  {
    var blobPagination = new BlobPagination
                         {
                           Page          = 1,
                           PageSize      = 10,
                           SortDirection = SortDirection.Asc,
                           Filter        = new Filters(),
                         };

    Assert.That(blobPagination.Page,
                Is.EqualTo(1));
    Assert.That(blobPagination.PageSize,
                Is.EqualTo(10));
    Assert.That(blobPagination.SortDirection,
                Is.EqualTo(SortDirection.Asc));
    Assert.That(blobPagination.Filter,
                Is.Not.Null);
  }

  public void BlobPaginationCompareTest()
  {
    var blobPagination1 = new BlobPagination
                          {
                            Page          = 1,
                            PageSize      = 10,
                            SortDirection = SortDirection.Asc,
                            Filter        = new Filters(),
                          };

    var blobPagination2 = new BlobPagination
                          {
                            Page          = 1,
                            PageSize      = 10,
                            SortDirection = SortDirection.Asc,
                            Filter        = new Filters(),
                          };

    Assert.That(blobPagination1,
                Is.EqualTo(blobPagination2));
  }

  [Test]
  public void BlobPaginationWithFilterTest()
  {
    var filter = new Filters
                 {
                   Or =
                   {
                     new FiltersAnd
                     {
                       And =
                       {
                         new FilterField
                         {
                           Field = new ResultField
                                   {
                                     ResultRawField = new ResultRawField
                                                      {
                                                        Field = ResultRawEnumField.SessionId,
                                                      },
                                   },
                         },
                       },
                     },
                   },
                 };

    var blobPagination = new BlobPagination
                         {
                           Page          = 2,
                           PageSize      = 20,
                           SortDirection = SortDirection.Desc,
                           Filter        = filter,
                         };

    Assert.That(blobPagination.Page,
                Is.EqualTo(2));
    Assert.That(blobPagination.PageSize,
                Is.EqualTo(20));
    Assert.That(blobPagination.SortDirection,
                Is.EqualTo(SortDirection.Desc));
    Assert.That(blobPagination.Filter,
                Is.Not.Null);
    Assert.That(blobPagination.Filter.Or.Count,
                Is.EqualTo(1));
    Assert.That(blobPagination.Filter.Or[0].And.Count,
                Is.EqualTo(1));
    Assert.That(blobPagination.Filter.Or[0]
                              .And[0].Field.ResultRawField.Field,
                Is.EqualTo(ResultRawEnumField.SessionId));
  }

  [Test]
  public void BlobPaginationDefaultValuesTest()
  {
    var blobPagination = new BlobPagination();

    Assert.That(blobPagination.Page,
                Is.EqualTo(0));
    Assert.That(blobPagination.PageSize,
                Is.EqualTo(0));
    Assert.That(blobPagination.SortDirection,
                Is.EqualTo(SortDirection.Unspecified));
    Assert.That(blobPagination.Filter,
                Is.EqualTo(new Filters()));
  }

  [Test]
  public void BlobPaginationWithNullFilterCanBeConvertedToWithEmptyFilter()
  {
    var blobPagination = new BlobPagination
                         {
                           Page          = 1,
                           PageSize      = 10,
                           SortDirection = SortDirection.Asc,
                           Filter        = new Filters(),
                         };

    Assert.That(blobPagination.Filter,
                Is.Not.Null);
    Assert.That(blobPagination.Filter.Or.Count,
                Is.EqualTo(0));
  }

  [Test]
  public void BlobPaginationWithNegativeValuesStillStoresValues()
  {
    var blobPagination = new BlobPagination
                         {
                           Page          = -1,
                           PageSize      = -10,
                           SortDirection = SortDirection.Asc,
                           Filter        = new Filters(),
                         };

    Assert.That(blobPagination.Page,
                Is.EqualTo(-1));
    Assert.That(blobPagination.PageSize,
                Is.EqualTo(-10));
  }
}
