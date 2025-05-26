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

using Grpc.Core;

using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

using Tests.Helpers;

namespace Tests.Services;

public class PartitionServiceTests
{
  [Test]
  public async Task GetPartitionAsyncShouldReturnPartition()
  {
    var partitionId = "partitionId";
    var mockInvoker = new Mock<CallInvoker>();
    var grpcPartition = new PartitionRaw
                        {
                          Id = "partitionId",

                          PodConfiguration =
                          {
                            {
                              "key1", "value1"
                            },
                            {
                              "key2", "value2"
                            },
                          },
                          PodMax               = 10,
                          PodReserved          = 5,
                          PreemptionPercentage = 15,
                          Priority             = 2,
                          ParentPartitionIds =
                          {
                            "parentId",
                          },
                        };
    grpcPartition.ParentPartitionIds.Add("parentId2");
    var responseAsync = new GetPartitionResponse
                        {
                          Partition = grpcPartition,
                        };
    var callInvoker       = mockInvoker.SetupAsyncUnaryCallInvokerMock<GetPartitionRequest, GetPartitionResponse>(responseAsync);
    var partitionsService = callInvoker.GetPartitionsServiceMock();

    var result = await partitionsService.GetPartitionAsync(partitionId,
                                                           CancellationToken.None);
    ClassicAssert.AreEqual(partitionId,
                           result.Id);
  }


  [Test]
  public void GetPartitionShouldThrowExceptionWhenPartitionNotFound()
  {
    var partitionId     = "nonExistentPartitionId";
    var mockCallInvoker = new Mock<CallInvoker>();
    var rpcException = new RpcException(new Status(StatusCode.NotFound,
                                                   "Partition not found"));
    mockCallInvoker.Setup(m => m.AsyncUnaryCall(It.IsAny<Method<GetPartitionRequest, GetPartitionResponse>>(),
                                                It.IsAny<string>(),
                                                It.IsAny<CallOptions>(),
                                                It.IsAny<GetPartitionRequest>()))
                   .Throws(rpcException);

    var partitionsService = mockCallInvoker.GetPartitionsServiceMock();

    var ex = Assert.ThrowsAsync<RpcException>(async () =>
                                              {
                                                await partitionsService.GetPartitionAsync(partitionId,
                                                                                          CancellationToken.None);
                                              });

    ClassicAssert.AreEqual(StatusCode.NotFound,
                           ex!.StatusCode);
    StringAssert.Contains("Partition not found",
                          ex.Message);
  }

  [Test]
  public async Task ListPartitionsAsyncShouldReturnPartitions()
  {
    var mockInvoker = new Mock<CallInvoker>();

    var grpcPartition1 = new PartitionRaw
                         {
                           Id = "partitionId1",
                           PodConfiguration =
                           {
                             {
                               "key1", "value1"
                             },
                             {
                               "key2", "value2"
                             },
                           },
                           PodMax               = 10,
                           PodReserved          = 5,
                           PreemptionPercentage = 15,
                           Priority             = 2,
                           ParentPartitionIds =
                           {
                             "parentId",
                           },
                         };
    grpcPartition1.ParentPartitionIds.Add("parentId2");

    var grpcPartition2 = new PartitionRaw
                         {
                           Id = "partitionId2",
                           PodConfiguration =
                           {
                             {
                               "keyA", "valueX"
                             },
                             {
                               "keyB", "valueY"
                             },
                           },
                           PodMax               = 20,
                           PodReserved          = 10,
                           PreemptionPercentage = 25,
                           Priority             = 3,
                           ParentPartitionIds =
                           {
                             "parentId3",
                           },
                         };
    grpcPartition2.ParentPartitionIds.Add("parentId4");

    var expectedPartitions = new List<PartitionRaw>
                             {
                               grpcPartition1,
                               grpcPartition2,
                             };

    var response = new ListPartitionsResponse
                   {
                     Partitions =
                     {
                       grpcPartition1,
                       grpcPartition2,
                     },
                     Total = expectedPartitions.Count,
                   };

    mockInvoker.SetupAsyncUnaryCallInvokerMock<ListPartitionsRequest, ListPartitionsResponse>(response);

    var partitionsService = mockInvoker.GetPartitionsServiceMock();

    var result = partitionsService.ListPartitionsAsync(new PartitionPagination(),
                                                       CancellationToken.None);

    var receivedPartitions = new List<Partition>();
    var totalCount         = 0;

    await foreach (var partitionTuple in result)
    {
      totalCount = partitionTuple.Item1;
      receivedPartitions.Add(partitionTuple.Item2);
    }

    ClassicAssert.AreEqual(expectedPartitions.Count,
                           totalCount);
    ClassicAssert.AreEqual(expectedPartitions.Count,
                           receivedPartitions.Count);

    for (var i = 0; i < expectedPartitions.Count; i++)
    {
      var expected = expectedPartitions[i];
      var actual   = receivedPartitions[i];

      ClassicAssert.AreEqual(expected.Id,
                             actual.Id,
                             $"Partition ID mismatch at index {i}");
      ClassicAssert.AreEqual(expected.PodMax,
                             actual.PodMax,
                             $"PodMax mismatch at index {i}");
      ClassicAssert.AreEqual(expected.PodReserved,
                             actual.PodReserved,
                             $"PodReserved mismatch at index {i}");
      ClassicAssert.AreEqual(expected.PreemptionPercentage,
                             actual.PreemptionPercentage,
                             $"PreemptionPercentage mismatch at index {i}");
      ClassicAssert.AreEqual(expected.Priority,
                             actual.Priority,
                             $"Priority mismatch at index {i}");

      CollectionAssert.AreEquivalent(expected.ParentPartitionIds,
                                     actual.ParentPartitionIds,
                                     $"ParentPartitionIds mismatch at index {i}");
    }
  }
}
