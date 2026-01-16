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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Partition;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Generic;

using NUnit.Framework;

namespace Tests.Common.Domain;

[TestFixture]
public class SortDirectionTests
{
  [TestCase(SortDirection.Unspecified)]
  [TestCase(SortDirection.Asc)]
  [TestCase(SortDirection.Desc)]
  [TestCase((SortDirection)99)]
  public void TestSortDirection(SortDirection direction)
  {
    switch (direction)
    {
      case SortDirection.Unspecified:
      case SortDirection.Asc:
      case SortDirection.Desc:
        var grpcDirection = direction.ToGrpc();
        Assert.That(grpcDirection.ToString(),
                    Is.EqualTo(direction.ToString()));

        var internalDirection = grpcDirection.ToIternal();
        Assert.That(internalDirection,
                    Is.EqualTo(direction));
        break;
      default:
        Assert.That(() => direction.ToGrpc(),
                    Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        break;
    }
  }
}

[TestFixture]
public class PaginationTests
{
  [Test]
  public void CreatePaginationTest()
  {
    var pagination = new Pagination<Filters, PartitionRawEnumField>
                     {
                       Page          = 1,
                       PageSize      = 10,
                       Total         = 100,
                       SortDirection = SortDirection.Asc,
                       Filter        = new Filters(),
                     };

    Assert.That(pagination.Page,
                Is.EqualTo(1));
    Assert.That(pagination.PageSize,
                Is.EqualTo(10));
    Assert.That(pagination.Total,
                Is.EqualTo(100));
    Assert.That(pagination.SortDirection,
                Is.EqualTo(SortDirection.Asc));
    Assert.That(pagination.Filter,
                Is.TypeOf<Filters>());
  }

  [Test]
  public void PartitionPaginationTest()
  {
    var partitionPagination = new PartitionPagination
                              {
                                Page          = 1,
                                PageSize      = 10,
                                Total         = 100,
                                SortDirection = SortDirection.Asc,
                                Filter        = new Filters(),
                              };

    Assert.That(partitionPagination.Page,
                Is.EqualTo(1));
    Assert.That(partitionPagination.PageSize,
                Is.EqualTo(10));
    Assert.That(partitionPagination.Total,
                Is.EqualTo(100));
    Assert.That(partitionPagination.SortDirection,
                Is.EqualTo(SortDirection.Asc));
    Assert.That(partitionPagination.Filter,
                Is.Not.Null);
  }
}
