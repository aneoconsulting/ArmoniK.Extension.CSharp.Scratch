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

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Partition;
using ArmoniK.Extension.CSharp.Client.Common.Services;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

public class PartitionsService : IPartitionsService
{
  private readonly ObjectPool<ChannelBase>    channel_;
  private readonly ILogger<PartitionsService> logger_;

  public PartitionsService(ObjectPool<ChannelBase> channel,
                           ILoggerFactory          loggerFactory)
  {
    logger_  = loggerFactory.CreateLogger<PartitionsService>();
    channel_ = channel;
  }

  public async Task<Partition> GetPartitionAsync(string            partitionId,
                                                 CancellationToken cancellationToken)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var partitionsClient = new Partitions.PartitionsClient(channel);
    var partition = await partitionsClient.GetPartitionAsync(new GetPartitionRequest
                                                             {
                                                               Id = partitionId,
                                                             },
                                                             cancellationToken: cancellationToken);
    return new Partition
           {
             Id                   = partition.Partition.Id,
             ParentPartitionIds   = partition.Partition.ParentPartitionIds,
             PodConfiguration     = partition.Partition.PodConfiguration,
             PodMax               = partition.Partition.PodMax,
             PodReserved          = partition.Partition.PodReserved,
             PreemptionPercentage = partition.Partition.PreemptionPercentage,
             Priority             = partition.Partition.Priority,
           };
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<(int, Partition)> ListPartitionsAsync(PartitionPagination                        partitionPagination,
                                                                      [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var partitionsClient = new Partitions.PartitionsClient(channel);
    var partitions = await partitionsClient.ListPartitionsAsync(new ListPartitionsRequest
                                                                {
                                                                  Filters  = partitionPagination.Filter,
                                                                  Page     = partitionPagination.Page,
                                                                  PageSize = partitionPagination.PageSize,
                                                                  Sort = new ListPartitionsRequest.Types.Sort
                                                                         {
                                                                           Direction = (SortDirection)partitionPagination.SortDirection,
                                                                         },
                                                                });

    foreach (var partition in partitions.Partitions)
    {
      yield return (partitions.Total, new Partition
                                      {
                                        Id                   = partition.Id,
                                        ParentPartitionIds   = partition.ParentPartitionIds,
                                        PodConfiguration     = partition.PodConfiguration,
                                        PodMax               = partition.PodMax,
                                        PodReserved          = partition.PodReserved,
                                        PreemptionPercentage = partition.PreemptionPercentage,
                                        Priority             = partition.Priority,
                                      });
    }

    ;
  }
}
