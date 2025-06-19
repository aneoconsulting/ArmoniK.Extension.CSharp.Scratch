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

using Tests.Configuration;
using Tests.Helpers;

namespace Tests.Services;

public class PartitionServiceTests
{
  [Test]
  public async Task GetPartitionAsyncShouldReturnPartition()
  {
    var client      = new MockedArmoniKClient();
    var partitionId = "partitionId";
    var grpcPartition = new PartitionRaw
                        {
                          Id = partitionId,

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
    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<GetPartitionRequest, GetPartitionResponse>(responseAsync);

    var result = await client.PartitionsService.GetPartitionAsync(partitionId,
                                                                  CancellationToken.None)
                             .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(result.Id,
                                  Is.EqualTo(partitionId));
                      Assert.That(result.PodMax,
                                  Is.EqualTo(10));
                      Assert.That(result.PodReserved,
                                  Is.EqualTo(5));
                      Assert.That(result.PreemptionPercentage,
                                  Is.EqualTo(15));
                      Assert.That(result.Priority,
                                  Is.EqualTo(2));
                      Assert.That(result.ParentPartitionIds,
                                  Contains.Item("parentId"));
                      Assert.That(result.ParentPartitionIds,
                                  Contains.Item("parentId2"));
                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<GetPartitionRequest, GetPartitionResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.Is<GetPartitionRequest>(r => r.Id == partitionId)),
                                                    Times.Once,
                                                    "AsyncUnaryCall should be called exactly once");
                    });
  }

  [Test]
  public void GetPartitionShouldThrowExceptionWhenPartitionNotFound()
  {
    var client      = new MockedArmoniKClient();
    var partitionId = "nonExistentPartitionId";
    var rpcException = new RpcException(new Status(StatusCode.NotFound,
                                                   "Partition not found"));
    client.CallInvokerMock.Setup(m => m.AsyncUnaryCall(It.IsAny<Method<GetPartitionRequest, GetPartitionResponse>>(),
                                                       It.IsAny<string>(),
                                                       It.IsAny<CallOptions>(),
                                                       It.IsAny<GetPartitionRequest>()))
          .Throws(rpcException);

    Assert.That(async () => await client.PartitionsService.GetPartitionAsync(partitionId,
                                                                             CancellationToken.None),
                Throws.InstanceOf<RpcException>()
                      .With.Property("StatusCode")
                      .EqualTo(StatusCode.NotFound)
                      .And.Message.Contains("Partition not found"));
  }

  [Test]
  public async Task ListPartitionsAsyncShouldReturnPartitions()
  {
    var client = new MockedArmoniKClient();

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

    client.CallInvokerMock.SetupAsyncUnaryCallInvokerMock<ListPartitionsRequest, ListPartitionsResponse>(response);

    var result = client.PartitionsService.ListPartitionsAsync(new PartitionPagination(),
                                                              CancellationToken.None);

    var receivedPartitions = new List<Partition>();
    var totalCount         = 0;

    await foreach (var partitionTuple in result.ConfigureAwait(false))
    {
      totalCount = partitionTuple.Item1;
      receivedPartitions.Add(partitionTuple.Item2);
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(totalCount,
                                  Is.EqualTo(2));
                      Assert.That(receivedPartitions,
                                  Has.Count.EqualTo(2));

                      Assert.That(receivedPartitions[0].Id,
                                  Is.EqualTo("partitionId1"));
                      Assert.That(receivedPartitions[0].PodMax,
                                  Is.EqualTo(10));
                      Assert.That(receivedPartitions[0].PodReserved,
                                  Is.EqualTo(5));
                      Assert.That(receivedPartitions[0].Priority,
                                  Is.EqualTo(2));

                      Assert.That(receivedPartitions[1].Id,
                                  Is.EqualTo("partitionId2"));
                      Assert.That(receivedPartitions[1].PodMax,
                                  Is.EqualTo(20));
                      Assert.That(receivedPartitions[1].PodReserved,
                                  Is.EqualTo(10));
                      Assert.That(receivedPartitions[1].Priority,
                                  Is.EqualTo(3));

                      client.CallInvokerMock.Verify(x => x.AsyncUnaryCall(It.IsAny<Method<ListPartitionsRequest, ListPartitionsResponse>>(),
                                                                          It.IsAny<string>(),
                                                                          It.IsAny<CallOptions>(),
                                                                          It.IsAny<ListPartitionsRequest>()),
                                                    Times.Once,
                                                    "AsyncUnaryCall should be called exactly once");
                    });
  }
}
