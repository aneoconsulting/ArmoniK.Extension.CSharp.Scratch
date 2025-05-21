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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Partition;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Extension.CSharp.Client.Common.Generic;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common;

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
        ClassicAssert.AreEqual(direction.ToString(),
                               grpcDirection.ToString());

        var internalDirection = grpcDirection.ToIternal();
        ClassicAssert.AreEqual(direction,
                               internalDirection);
        break;
      default:
        ClassicAssert.Throws<ArgumentOutOfRangeException>(() =>
                                                          {
                                                            direction.ToGrpc();
                                                          });
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
    var pagination = new Pagination<string>
                     {
                       Page          = 1,
                       PageSize      = 10,
                       Total         = 100,
                       SortDirection = SortDirection.Asc,
                       Filter        = "testFilter",
                     };

    ClassicAssert.AreEqual(1,
                           pagination.Page);
    ClassicAssert.AreEqual(10,
                           pagination.PageSize);
    ClassicAssert.AreEqual(100,
                           pagination.Total);
    ClassicAssert.AreEqual(SortDirection.Asc,
                           pagination.SortDirection);
    ClassicAssert.AreEqual("testFilter",
                           pagination.Filter);
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

    ClassicAssert.AreEqual(1,
                           partitionPagination.Page);
    ClassicAssert.AreEqual(10,
                           partitionPagination.PageSize);
    ClassicAssert.AreEqual(100,
                           partitionPagination.Total);
    ClassicAssert.AreEqual(SortDirection.Asc,
                           partitionPagination.SortDirection);
    ClassicAssert.IsNotNull(partitionPagination.Filter);
  }
}
