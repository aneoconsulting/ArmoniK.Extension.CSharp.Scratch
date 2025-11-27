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

using ArmoniK.Api.gRPC.V1.Partitions;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Partition;

/// <summary>
///   Represents a partition within a ArmoniK, detailing its configuration, resource allocation, and
///   hierarchy.
/// </summary>
public record Partition
{
  /// <summary>
  ///   Identifier of the partition.
  /// </summary>
  public string Id { get; init; } = string.Empty;

  /// <summary>
  ///   Collection of identifiers for parent partitions.
  /// </summary>
  public IEnumerable<string> ParentPartitionIds { get; init; } = [];

  /// <summary>
  ///   Configuration settings for pods within the partition, represented as key-value pairs.
  /// </summary>
  public IEnumerable<KeyValuePair<string, string>> PodConfiguration { get; init; } = [];

  /// <summary>
  ///   Maximum number of pods that can be allocated to this partition.
  /// </summary>
  public long PodMax { get; init; }

  /// <summary>
  ///   Number of pods that are reserved for this partition.
  /// </summary>
  public long PodReserved { get; init; }

  /// <summary>
  ///   Percentage of the partition's capacity that is subject to preemption.
  /// </summary>
  public long PreemptionPercentage { get; init; }

  /// <summary>
  ///   Priority of the partition, which may influence scheduling decisions.
  /// </summary>
  public long Priority { get; init; }
}

/// <summary>
///   Class of extensions methods to convert Protobuf instances and Partition instances
/// </summary>
public static class PartitionExt
{
  /// <summary>
  ///   Convert a PartitionRaw instance into a Partition instance
  /// </summary>
  /// <param name="partitionRaw">The PartitionRaw instance</param>
  /// <returns>The Partition instance</returns>
  public static Partition ToPartition(this PartitionRaw partitionRaw)
    => new()
       {
         Id                   = partitionRaw.Id,
         ParentPartitionIds   = partitionRaw.ParentPartitionIds,
         PodConfiguration     = partitionRaw.PodConfiguration,
         PodMax               = partitionRaw.PodMax,
         PodReserved          = partitionRaw.PodReserved,
         PreemptionPercentage = partitionRaw.PreemptionPercentage,
         Priority             = partitionRaw.Priority,
       };
}
