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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Partition;

using NUnit.Framework;

namespace ArmoniK.Tests.Common.Domain;

[TestFixture]
public class PartitionTests
{
  [Test]
  public void CreatePartitionTest()
  {
    var parentPartitionIds = new List<string>
                             {
                               "parent1",
                               "parent2",
                             };
    var podConfiguration = new List<KeyValuePair<string, string>>
                           {
                             new("key1",
                                 "value1"),
                             new("key2",
                                 "value2"),
                           };

    var partition = new Partition
                    {
                      Id                   = "partition1",
                      ParentPartitionIds   = parentPartitionIds,
                      PodConfiguration     = podConfiguration,
                      PodMax               = 100,
                      PodReserved          = 10,
                      PreemptionPercentage = 20,
                      Priority             = 1,
                    };

    Assert.That(partition.Id,
                Is.EqualTo("partition1"));
    Assert.That(partition.ParentPartitionIds,
                Is.EqualTo(parentPartitionIds));
    Assert.That(partition.PodConfiguration,
                Is.EqualTo(podConfiguration));
    Assert.That(partition.PodMax,
                Is.EqualTo(100));
    Assert.That(partition.PodReserved,
                Is.EqualTo(10));
    Assert.That(partition.PreemptionPercentage,
                Is.EqualTo(20));
    Assert.That(partition.Priority,
                Is.EqualTo(1));
  }

  [Test]
  public void PartitionEqualityTest()
  {
    var parentPartitionIds = new List<string>
                             {
                               "parent1",
                               "parent2",
                             };
    var podConfiguration = new List<KeyValuePair<string, string>>
                           {
                             new("key1",
                                 "value1"),
                             new("key2",
                                 "value2"),
                           };

    var partition1 = new Partition
                     {
                       Id                   = "partition1",
                       ParentPartitionIds   = parentPartitionIds,
                       PodConfiguration     = podConfiguration,
                       PodMax               = 100,
                       PodReserved          = 10,
                       PreemptionPercentage = 20,
                       Priority             = 1,
                     };

    var partition2 = new Partition
                     {
                       Id                   = "partition1",
                       ParentPartitionIds   = parentPartitionIds,
                       PodConfiguration     = podConfiguration,
                       PodMax               = 100,
                       PodReserved          = 10,
                       PreemptionPercentage = 20,
                       Priority             = 1,
                     };

    Assert.That(partition1,
                Is.EqualTo(partition2));
  }

  [Test]
  public void PartitionInequalityTest()
  {
    var parentPartitionIds1 = new List<string>
                              {
                                "parent1",
                                "parent2",
                              };
    var podConfiguration1 = new List<KeyValuePair<string, string>>
                            {
                              new("key1",
                                  "value1"),
                              new("key2",
                                  "value2"),
                            };

    var partition1 = new Partition
                     {
                       Id                   = "partition1",
                       ParentPartitionIds   = parentPartitionIds1,
                       PodConfiguration     = podConfiguration1,
                       PodMax               = 100,
                       PodReserved          = 10,
                       PreemptionPercentage = 20,
                       Priority             = 1,
                     };

    var parentPartitionIds2 = new List<string>
                              {
                                "parent3",
                                "parent4",
                              };
    var podConfiguration2 = new List<KeyValuePair<string, string>>
                            {
                              new("key3",
                                  "value3"),
                              new("key4",
                                  "value4"),
                            };

    var partition2 = new Partition
                     {
                       Id                   = "partition2",
                       ParentPartitionIds   = parentPartitionIds2,
                       PodConfiguration     = podConfiguration2,
                       PodMax               = 200,
                       PodReserved          = 20,
                       PreemptionPercentage = 30,
                       Priority             = 2,
                     };

    Assert.That(partition1,
                Is.Not.EqualTo(partition2));
  }
}
